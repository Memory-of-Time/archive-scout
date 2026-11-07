from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig, ReportConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import (
    get_or_create_keyword_set, get_or_create_target, save_match, start_scan_run,
    upsert_captures, upsert_document,
)
from archive_scout.operations import prepare_scan_jobs, run_project
from archive_scout.scanning.jobs import ScanJob
from archive_scout.scanning.pending import _scan_one
from archive_scout.scanning.scoring import SCORING_RECIPE, analyze_content


class MatchEvidenceTests(unittest.TestCase):
    def test_prefilter_keeps_dotless_i_matches_with_both_automata(self):
        for native in (True, False):
            with self.subTest(native=native):
                from archive_scout.scanning import automaton
                backend = automaton.ahocorasick_rs if native else None
                with patch.object(automaton, 'ahocorasick_rs', backend):
                    job = ScanJob.create(1, 'Test', ['kayip'])
                fields = ('', '', 'kayıp', 'kayıp', [], job.patterns)
                direct = analyze_content(*fields)
                self.assertGreater(direct['score'], 0)
                self.assertEqual(analyze_content(*fields, job.prefilter), direct)

    def test_regex_keeps_original_whitespace_and_normalized_matches(self):
        job = ScanJob.create(1, 'Test', [r'regex: item_id\s{2}ABC', 'regex: archive word'])
        result = analyze_content('', '', 'item_id  ABC archive_word', '', [], job.patterns, job.prefilter)
        self.assertEqual(set(result['hits']), {r'item_id\s{2}ABC', 'archive word'})
        self.assertTrue(any('item_id ABC' in snippet for snippet in result['snippets']))

    def test_mixed_regex_views_keep_old_hit_count_and_both_kinds_of_evidence(self):
        rule = 'raw_token|decoded token'
        job = ScanJob.create(1, 'Test', ['regex: ' + rule])
        body = (' x' * 250).join(('RAW_TOKEN', 'first decoded%20token', 'second decoded_token'))
        result = analyze_content('', '', body, '', [], job.patterns, job.prefilter)
        self.assertEqual(result['hits'][rule], 2)
        for passage in ('RAW_TOKEN', 'first decoded token', 'second decoded token'):
            self.assertTrue(any(passage in snippet for snippet in result['snippets']))

    def test_snippets_cover_all_disjoint_passages_and_nonbody_fields(self):
        job = ScanJob.create(1, 'Test', ['needle'])
        labels = [f'MARKER-{index:02}' for index in range(12)]
        body = (' x' * 300).join(f'{label} Needle' for label in labels)
        result = analyze_content('https://example.org/needle-url', 'Needle TITLE-ONLY', body,
                                 '<script>Needle SOURCE-ONLY</script>', ['https://example.org/needle-link'],
                                 job.patterns, job.prefilter)
        snippets = result['snippets']
        body_snippets = [snippet for snippet in snippets if snippet.startswith('[Body]')]
        self.assertEqual(len(body_snippets), len(labels))
        self.assertTrue(all(label in snippet for label, snippet in zip(labels, body_snippets)))
        for field in ('Title', 'Source', 'Links', 'URL'):
            self.assertTrue(any(snippet.startswith(f'[{field}]') for snippet in snippets))

    def test_overlapping_windows_cover_the_end_of_a_long_match(self):
        job = ScanJob.create(1, 'Test', ['needle', 'regex: BEGIN.*END'])
        body = 'needle BEGIN ' + 'x' * 900 + ' END '
        result = analyze_content('', '', body, body, [], job.patterns, job.prefilter)
        self.assertEqual(len(result['snippets']), 1)
        self.assertIn('END', result['snippets'][0])

    def test_pending_plain_text_preserves_body_and_low_weight_hit(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'capture.txt'
            data = b'Literal <unparsed> marker_id  ABC'
            path.write_bytes(data)
            row = {'path': str(path), 'id': 1, 'original_url': 'http://example.org/plain.txt',
                   'mimetype': 'text/plain', 'expected_hash': hashlib.sha256(data).hexdigest(),
                   'expected_size': len(data)}
            job = ScanJob.create(1, 'Test', [r'regex: marker_id\s{2}ABC | weight=0.01'])
            result = _scan_one(row, [job])
            self.assertEqual(result['visible'], data.decode())
            self.assertEqual(result['analyses'][1]['score'], 1)


class ScanReportCompletenessTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.config = ProjectConfig(self.root, ['example.org/*'], ['needle | weight=0.01'],
                                    from_date='2008', to_date='2008', cdx_collapses=[]).normalized()
        self.db = open_database(self.root)
        self.addCleanup(self.db.close)

    def seed(self, index, body):
        url = f'http://example.org/{index}.txt'
        target = get_or_create_target(self.db, self.config.targets[0])
        data = body.encode()
        upsert_captures(self.db, [('20080101000000', url, 'text/plain', '200', '', str(len(data)))],
                        target, cdx_query_signature(self.config))
        capture = self.db.execute('SELECT id FROM captures WHERE original_url=?', (url,)).fetchone()[0]
        path = self.root / f'{index}.txt'
        path.write_bytes(data)
        digest = hashlib.sha256(data).hexdigest()
        document = upsert_document(self.db, capture, path, '', body, [], digest, digest, len(data))
        self.db.commit()
        return url, document

    def test_local_rescan_recovers_weak_hit_and_all_passages_in_reports(self):
        text = (' filler' * 75).join(f'CONTEXT-{i:02} Needle' for i in range(9))
        first, document = self.seed(1, text)
        second, _ = self.seed(2, 'Needle')
        missing, _ = self.seed(3, 'unrelated text')
        keyword_set = get_or_create_keyword_set(self.db, self.config.keyword_set_name, self.config.keywords)
        old = start_scan_run(self.db, keyword_set, 'Old scan', 1, 'rescan', {'scoring_recipe': 3})
        save_match(self.db, old, document, {'score': 0})
        self.db.execute("UPDATE scan_runs SET status='interrupted' WHERE id=?", (old,))
        self.db.commit()
        with patch('archive_scout.operations.index_archive', side_effect=AssertionError('Rescan must stay local')), \
                patch('archive_scout.operations.download_archive', side_effect=AssertionError('Rescan must stay local')):
            paths = run_project(self.config, 'rescan', threading.Event())
        self.assertEqual(paths.status, 'complete')
        urls = set(paths['matched_urls'].read_text(encoding='utf-8').splitlines())
        self.assertEqual(urls, {first, second})
        ranked = paths['matches_ranked'].read_text(encoding='utf-8')
        self.assertNotIn(missing, ranked)
        for index in range(9):
            self.assertIn(f'CONTEXT-{index:02}', ranked)
        self.assertEqual(self.db.execute('SELECT score FROM document_matches WHERE scan_run_id=?', (old,)).fetchone()[0], 0)
        new = self.db.execute('SELECT metadata_json FROM scan_runs ORDER BY id DESC LIMIT 1').fetchone()[0]
        self.assertEqual(json.loads(new)['scoring_recipe'], SCORING_RECIPE)

    def test_reenabled_snippets_backfill_in_a_new_generation(self):
        _, document = self.seed(1, 'Needle')
        lean = replace(self.config, report=ReportConfig(outputs=['matched_urls'], retain_scan_details=False))
        first = prepare_scan_jobs(self.db, lean, 'all')[0]
        self.assertFalse(first.store_snippets)
        save_match(self.db, first.scan_run_id, document, {'score': 1})
        self.db.execute("UPDATE scan_runs SET status='interrupted' WHERE id=?", (first.scan_run_id,))
        self.db.commit()
        second = prepare_scan_jobs(self.db, self.config, 'resume')[0]
        self.assertNotEqual(first.scan_run_id, second.scan_run_id)
        self.assertTrue(second.store_snippets)
        save_match(self.db, second.scan_run_id, document, {'score': 1, 'snippets': ['Needle']})
        self.db.execute("UPDATE scan_runs SET status='complete' WHERE id=?", (second.scan_run_id,))
        self.db.commit()
        self.assertEqual(prepare_scan_jobs(self.db, self.config, 'resume')[0].scan_run_id, second.scan_run_id)
