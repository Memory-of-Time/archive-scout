# Full-output oracle recorded against the untouched supplied v1.0.8 source.
# Native and pure-Python matchers produced the same expected digest.
from __future__ import annotations
import hashlib
import json
import random
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from archive_scout.scanning import automaton
from archive_scout.scanning.automaton import LiteralAutomaton
from archive_scout.scanning.jobs import ScanJob
from archive_scout.scanning.keywords import compile_keywords, compile_prefilter
from archive_scout.scanning.normalization import normalize_search
from archive_scout.scanning.rescanner import _analyze_saved_document
from archive_scout.scanning.scoring import analyze_content, _compact_markup_text
from archive_scout.utils import normalize_search as reference_normalize

def differential_fixture():
    rng = random.Random(180506)
    tokens = ['needle', 'mirror', 'archive', 'ABC', 'abc', 'Saki', 'bathroom', 'school girls', 'rare footage', 'other', 'İ', 'ı', 'ſ', 'K', 'ß', 'ＡＢＣ', '日本語', '<b>', '</b>', '<!-- mirror -->', '\\x6e\\x65edle', '%6d%69rror', '&amp;', '&#32;', '\t', '\n', '\xa0', '\x1f', '. ', '! ', '</p>', '<br><br>', 'http://example.com/needle.mp4']
    rules_pool = ['needle', 'mirror', 'archive', 'ABC', '日本語', 'rare footage', 'school girls', 'exact: rare footage', 'required: archive', 'exclude: unavailable', 'exclude: mirror', 'regex: n.e+dl.', 'regex: (?=a)', 'regex: \\b', 'regex: ^', 'regex: $', 'ABC | case', 'abc | whole', 'needle | label=duplicate', 'mirror | label=duplicate', 'needle | weight=0.3', '%20', 'i', 's', 'k']
    results = []
    for i in range(800):
        raw = ' '.join(rng.choices(tokens, k=rng.randint(0, 120)))
        visible = ' '.join(rng.choices(tokens[:17], k=rng.randint(0, 110)))
        terms = rng.sample(rules_pool, rng.randint(1, 13))
        patterns = compile_keywords(terms)
        pf = compile_prefilter(patterns) if i % 4 else None
        options = {'include_hit_fields': i % 7 != 0, 'include_snippets': i % 11 != 0, 'include_interesting_links': i % 9 != 0}
        outcome = analyze_content('http://example.com/' + rng.choice(tokens[:10]), rng.choice(tokens[:17]), visible, raw, ['http://example.com/' + rng.choice(tokens[:10]) + '.jpg'], patterns, pf, **options)
        results.append(outcome)
    return hashlib.sha256(json.dumps(results, sort_keys=True, ensure_ascii=False).encode()).hexdigest()

class ScannerOptimizationTests(unittest.TestCase):
    ORACLE = '069f3aef9963146e2881ac689b811a949dae8e42008d6637d64a2e9fb0ab6d36'

    def test_full_results_match_unmodified_scanner(self):
        self.assertEqual(differential_fixture(), self.ORACLE)

    def test_python_fallback_matches_unmodified_scanner(self):
        with mock.patch.object(automaton, 'ahocorasick_rs', None):
            self.assertEqual(differential_fixture(), self.ORACLE)

    def test_normalization_preserves_escapes_unicode_and_whitespace(self):
        rng = random.Random(107108)
        atoms = ['a', 'ABC', 'ß', 'İ', 'ı', 'ſ', 'K', 'ＡＢＣ', '日本語', '%2520', '%26amp%3B', '&amp;', '&#x20;', '\\u0061', '\\x41', '_', '\t', '\r\n', '\x1c', '\x85', '\xa0', '\u2003', '\u2028', '\u202f', '\u3000', '\x00', '\ufeff']
        for _ in range(2000):
            value = ''.join(rng.choices(atoms, k=rng.randrange(80)))
            self.assertEqual(normalize_search(value), reference_normalize(value))

    def test_chunk_boundaries_overlap_and_unicode_counts(self):
        patterns = ['aa', 'aaa', 'a', '日本語', '語', 'baba', 'a b', 'bb', 'aba', 'z']
        text = 'aaaaa 日本語 baba a b bb z' * 60 + 'aaa'
        expected = {p: len(list(re.finditer(re.escape(p), text))) for p in patterns if p in text}
        for backend in (automaton.ahocorasick_rs, None):
            with mock.patch.object(automaton, 'ahocorasick_rs', backend):
                for selected in (patterns[:3], patterns):
                    matcher = LiteralAutomaton(selected)
                    for width in (1, 3, 17, 65536):
                        self.assertEqual(matcher.count_non_overlapping(text, width), {p: expected[p] for p in selected if p in expected})

    def test_compact_text_matches_original_rule(self):
        rng = random.Random(105)
        for _ in range(200):
            value = ''.join(rng.choices(['<', '>', 'a', 'b', '\n', '\t', '&amp;', '日本語'], k=100))
            expected = re.sub('\\s+', ' ', re.sub('(?s)<[^>]*>', ' ', value)).strip()
            self.assertEqual(_compact_markup_text(value), expected)
        suffix = '<' * 50000
        self.assertEqual(_compact_markup_text('<p>needle</p>' + suffix), 'needle ' + suffix)

    def test_late_source_and_tag_split_evidence_survive(self):
        raw = '<html><body>nothing visible</body><!--' + 'background ' * 65000 + 'late_needle--><div>split<b></b>phrase</div></html>'
        patterns = compile_keywords(['late needle', 'split phrase'])
        result = analyze_content('http://example.com/', '', 'nothing visible', raw, [], patterns, compile_prefilter(patterns))
        self.assertIn('source', result['hit_fields']['late needle'])
        self.assertIn('compact', result['hit_fields']['split phrase'])

    def test_rescan_reads_file_once_and_uses_recorded_encoding(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'capture.txt'
            payload = '<html><body>Привет зеркало</body></html>'.encode('cp1251')
            path.write_bytes(payload)
            row = {'id': 1, 'capture_id': 1, 'path': str(path), 'original_url': 'http://example.com/', 'mimetype': 'text/html', 'detected_encoding': 'windows-1251', 'content_hash': hashlib.sha256(payload).hexdigest(), 'title': '', 'body_text': '', 'body_zlib': None, 'links_json': '[]', 'normalized_hash': ''}
            original_read = Path.read_bytes
            reads = []

            def tracked_read(value):
                reads.append(value)
                return original_read(value)
            with mock.patch.object(Path, 'read_bytes', tracked_read):
                result = _analyze_saved_document(row, [ScanJob.create(1, 'Recall', ['зеркало'])])
            self.assertEqual(result['kind'], 'success')
            self.assertEqual(reads, [path])
            self.assertIn('body', result['analyses'][0][1]['hit_fields']['зеркало'])
if __name__ == '__main__':
    unittest.main()
