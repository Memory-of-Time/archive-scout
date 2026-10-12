from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import parse_cdx_text_rows
from archive_scout.cdx.indexer import PendingWindow, _resolve_strategy, index_archive
from archive_scout.cdx.parameters import build_cdx_params, preferred_index_strategy
from archive_scout.config import MediaConfig, NetworkConfig, ProjectConfig, load_project_config
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import cdx_row_to_dict
from archive_scout.media.indexer import (
    _resolve_media_strategy,
    build_media_num_pages_params,
    build_media_paged_params,
    index_media,
)


class FinalPerformanceTests(unittest.TestCase):
    def _config(self, root: Path, *, strategy: str = "auto") -> ProjectConfig:
        return ProjectConfig(
            output_dir=root,
            targets=["example.com/*"],
            keywords=["needle"],
            from_date="2001",
            to_date="2001",
            cdx_delay=0,
            network=NetworkConfig(index_strategy=strategy, cdx_workers=10),
        ).normalized()

    def test_explicit_paged_indexing_uses_timemap_page_count_then_parallel_pages(self):
        with tempfile.TemporaryDirectory() as temp:
            config = self._config(Path(temp), strategy="paged")
            database = open_database(config.output_dir)
            calls: list[tuple[tuple[str, ...], dict[str, str], bool]] = []

            def fake_get(_self, urls, params, max_bytes=64 * 1024 * 1024, prefer_text=False):
                del max_bytes
                values = dict(params)
                calls.append((tuple(urls), values, prefer_text))
                if values.get("showNumPages") == "true":
                    return 1
                return []

            with patch("archive_scout.cdx.client.HttpClient.get_cdx_any", new=fake_get):
                index_archive(config, database, threading.Event())
            database.close()

            self.assertEqual(len(calls), 2)
            self.assertIn("/web/timemap/json", calls[0][0][0])
            self.assertEqual(calls[0][1]["showNumPages"], "true")
            self.assertNotIn("pageSize", calls[0][1])
            self.assertFalse(calls[0][2])
            self.assertEqual(calls[1][1]["page"], "0")
            self.assertNotIn("pageSize", calls[1][1])
            self.assertFalse(calls[1][2])

    def test_explicit_paged_strategy_remains_available(self):
        with tempfile.TemporaryDirectory() as temp:
            config = self._config(Path(temp), strategy="paged")
            self.assertEqual(preferred_index_strategy(config, "example.com/*"), "paged")
        with tempfile.TemporaryDirectory() as temp:
            config = self._config(Path(temp), strategy="auto")
            self.assertEqual(preferred_index_strategy(config, "example.com/*"), "resume")

    def test_unfinished_numbered_queue_stays_parallel_in_auto_mode(self):
        with tempfile.TemporaryDirectory() as temp:
            config = self._config(Path(temp))
            window = PendingWindow(
                "20010101000000",
                "20011231235959",
                strategy="paged",
                page=712,
                page_count=8000,
                page_blocks=50,
                retry_pages=[18, 711],
                page_failures={18: 2, 711: 1},
                resume_key="old-token",
            )
            _resolve_strategy(window, config, "example.com/*")
            self.assertEqual(window.strategy, "paged")
            self.assertEqual(window.page, 712)
            self.assertEqual(window.page_count, 8000)
            self.assertEqual(window.retry_pages, [18, 711])
            self.assertEqual(window.page_failures, {18: 2, 711: 1})
            self.assertEqual(window.resume_key, "old-token")
            self.assertEqual(window.start, "20010101000000")
            self.assertEqual(window.end, "20011231235959")

    def test_media_automatic_page_grouping_never_becomes_page_size_one(self):
        with tempfile.TemporaryDirectory() as temp:
            config = self._config(Path(temp), strategy="paged")
            extensions = ["jpg", "png", "mp4"]
            count = dict(build_media_num_pages_params(
                config, "example.com/*", "20010101000000", "20011231235959", extensions, config.network.page_blocks
            ))
            page = dict(build_media_paged_params(
                config, "example.com/*", "20010101000000", "20011231235959", extensions, 3, config.network.page_blocks
            ))
            self.assertNotIn("pageSize", count)
            self.assertNotIn("pageSize", page)
            self.assertEqual(page["page"], "3")

    def test_unfinished_media_numbered_queue_stays_parallel(self):
        with tempfile.TemporaryDirectory() as temp:
            config = self._config(Path(temp))
            window = PendingWindow(
                "20010101000000",
                "20011231235959",
                strategy="paged",
                page=999,
                page_count=12000,
                page_blocks=0,
                retry_pages=[50],
                page_failures={50: 2},
            )
            _resolve_media_strategy(window, config, "example.com/*")
            self.assertEqual(window.strategy, "paged")
            self.assertEqual(window.page_count, 12000)
            self.assertEqual(window.retry_pages, [50])
            self.assertEqual(window.page_failures, {50: 2})

    def test_media_auto_indexing_uses_timemap_parallel_page_pipeline(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            base = self._config(root, strategy="paged")
            config = ProjectConfig(
                **{
                    **{name: getattr(base, name) for name in base.__dataclass_fields__},
                    "media": MediaConfig(
                        enabled=True, include_images=True, include_videos=False,
                        include_extensions=["jpg", "png"], discover_embedded=False,
                    ),
                }
            ).normalized()
            database = open_database(root)
            calls: list[dict[str, str]] = []

            def fake_get(_self, _urls, params, max_bytes=64 * 1024 * 1024, prefer_text=False):
                del max_bytes, prefer_text
                values = dict(params)
                calls.append(values)
                if values.get("showNumPages") == "true":
                    return 1
                return []

            with patch("archive_scout.cdx.client.HttpClient.get_cdx_any", new=fake_get):
                index_media(config, database, threading.Event())
            database.close()
            self.assertEqual(len(calls), 2)
            self.assertEqual(calls[0]["showNumPages"], "true")
            self.assertNotIn("pageSize", calls[0])
            self.assertEqual(calls[1]["page"], "0")
            self.assertNotIn("pageSize", calls[1])

    def test_urlkey_text_rows_preserve_urls_with_spaces_and_resume_key(self):
        params = [("fl", "urlkey,timestamp,mimetype,statuscode,digest,length,original")]
        body = (
            b"com,example)/path 20010102030405 text/html 200 ABC 123 http://example.com/a path\n"
            b"\n"
            b"resume-token\n"
        )
        parsed = parse_cdx_text_rows(body, "https://web.archive.org/cdx", params)
        self.assertEqual(parsed.resume_key, "resume-token")
        self.assertEqual(parsed.rows, [
            ("20010102030405", "http://example.com/a path", "text/html", "200", "ABC", "123")
        ])

    def test_compact_cdx_tuple_and_mapping_have_identical_repository_shape(self):
        expected = {
            "timestamp": "20010102030405",
            "original": "http://example.com/a",
            "mimetype": "text/html",
            "statuscode": "200",
            "digest": "ABC",
            "length": "123",
        }
        compact = ("20010102030405", "http://example.com/a", "text/html", "200", "ABC", "123")
        self.assertEqual(cdx_row_to_dict(compact), expected)
        self.assertEqual(cdx_row_to_dict(expected), expected)

    def test_v102_untouched_resume_batch_upgrades_but_custom_value_is_preserved(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            common = {
                "version": "1.0.2",
                "output_dir": temp,
                "targets": ["example.com/*"],
                "keywords": ["needle"],
                "from_date": "2001",
                "to_date": "2001",
                "cdx_delay": 0.75,
                "network": {"page_blocks": 0, "cdx_workers": 10, "index_strategy": "auto"},
            }
            untouched = root / "untouched.json"
            untouched.write_text(json.dumps({**common, "page_size": 50000}), encoding="utf-8")
            self.assertEqual(load_project_config(untouched).page_size, 100000)
            custom = root / "custom.json"
            custom.write_text(json.dumps({**common, "page_size": 75000}), encoding="utf-8")
            self.assertEqual(load_project_config(custom).page_size, 75000)

    def test_resume_request_includes_sort_key_for_reliable_continuation(self):
        with tempfile.TemporaryDirectory() as temp:
            config = self._config(Path(temp))
            params = dict(build_cdx_params(
                config, "example.com/*", "20010101000000", "20011231235959", resume="token"
            ))
            self.assertTrue(params["fl"].startswith("urlkey,"))
            self.assertEqual(params["resumeKey"], "token")
            self.assertEqual(params["limit"], "100000")


if __name__ == "__main__":
    unittest.main()
