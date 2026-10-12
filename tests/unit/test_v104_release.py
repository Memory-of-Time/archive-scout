from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.config import ProjectConfig
from archive_scout.constants import TEXT_EXTENSIONS
import archive_scout.content as content_module
import archive_scout.scanning.automaton as automaton_module
from archive_scout.content import is_text_candidate, parse_page
from archive_scout.database.repositories import get_or_create_target, upsert_captures
from archive_scout.operations import run_project
from archive_scout.parsing.embeds import extract_embed_candidates_fast
from archive_scout.scanning.automaton import LiteralAutomaton
from archive_scout.scanning.jobs import ScanJob
from archive_scout.scanning.scoring import analyze_content, prepare_analysis_fields


class V104ReleaseTests(unittest.TestCase):
    def test_requested_legacy_text_extensions_are_scannable(self):
        requested = {".htm", ".shtm", ".dhtm", ".xhtm", ".phtm", ".cgi", ".php", ".dat", ".txt"}
        self.assertTrue(requested.issubset(TEXT_EXTENSIONS))
        for extension in requested:
            with self.subTest(extension=extension):
                self.assertTrue(is_text_candidate(f"http://example.com/page{extension}", "application/octet-stream"))

    @unittest.skipUnless(automaton_module.ahocorasick_rs is not None, "native ahocorasick-rs wheel is not installed")
    def test_native_aho_backend_is_exercised_when_installed(self):
        matcher = LiteralAutomaton(["alpha", "beta"])
        self.assertIsNotNone(matcher._native)
        found: set[str] = set()
        matcher.find_into("xx alpha beta yy", found)
        self.assertEqual(found, {"alpha", "beta"})

    @unittest.skipUnless(content_module.LexborHTMLParser is not None, "native selectolax wheel is not installed")
    def test_native_lexbor_backend_is_exercised_when_installed(self):
        with patch("archive_scout.content.PageParser", side_effect=AssertionError("Python fallback should not run")):
            title, visible, links = parse_page(
                "<html><head><title>Native</title></head><body>Hello <a href='/x'>world</a></body></html>",
                "http://example.com/root/page.htm",
            )
        self.assertEqual(title, "Native")
        self.assertIn("Hello", visible)
        self.assertIn("http://example.com/x", links)

    def test_parse_page_preserves_title_text_and_links(self):
        raw = "<html><head><title>Old Site</title><style>x{}</style></head><body>Hello <a href='/clip.dat'>world</a><script>secret</script></body></html>"
        title, visible, links = parse_page(raw, "http://example.com/root/page.htm")
        self.assertEqual(title, "Old Site")
        self.assertIn("Hello", visible)
        self.assertIn("world", visible)
        self.assertNotIn("secret", visible)
        self.assertIn("http://example.com/clip.dat", links)

    def test_fast_legacy_embed_recovery_avoids_full_parser_dependency(self):
        raw = '<param name="flashvars" value="file=http%3A%2F%2Fcdn.example.com%2Fold.wmv&autoplay=1">'
        urls = {item.url for item in extract_embed_candidates_fast(raw, "http://example.com/page.htm")}
        self.assertIn("http://cdn.example.com/old.wmv", urls)

    def test_prefilter_one_pass_keeps_scoring_semantics(self):
        job = ScanJob.create(1, "set", ["alpha", "required: beta", "exclude: poison"])
        raw = "<html><title>Alpha Beta</title><body>alpha beta text</body></html>"
        title, visible, links = parse_page(raw, "http://example.com/a.htm")
        fields, normalized = prepare_analysis_fields("http://example.com/a.htm", title, visible, raw, links)
        result = analyze_content(
            "http://example.com/a.htm", title, visible, raw, links,
            job.patterns, job.prefilter, fields, normalized,
        )
        self.assertGreater(result["score"], 0)
        self.assertFalse(result["required_missing"])
        self.assertFalse(result["excluded"])

    def test_index_only_operation_generates_reports_without_scan_run(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                output_dir=root,
                targets=["example.com/*"],
                keywords=[],
                from_date="2005",
                to_date="2005",
            )

            def fake_index(_config, database, _stop, _callback=None):
                target_id = get_or_create_target(database, "example.com/*")
                upsert_captures(database, [{
                    "original": "http://example.com/page.shtm",
                    "timestamp": "20050102030405",
                    "mimetype": "text/html",
                    "statuscode": "200",
                    "digest": "ABC",
                    "length": "123",
                }], target_id, "sig")
                database.commit()

            with patch("archive_scout.operations.index_archive", side_effect=fake_index):
                paths = run_project(config, "index", threading.Event())

            self.assertTrue(paths["all_indexed_urls"].is_file())
            self.assertTrue(paths["summary"].is_file())
            self.assertIn("http://example.com/page.shtm", paths["all_indexed_urls"].read_text(encoding="utf-8"))
            self.assertIn("Indexed captures: 1", paths["summary"].read_text(encoding="utf-8"))

            regenerated = run_project(config, "report", threading.Event())
            self.assertTrue(regenerated["all_indexed_urls"].is_file())


if __name__ == "__main__":
    unittest.main()
