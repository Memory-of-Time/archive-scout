from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.indexer import index_archive
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database


class V3PagedIndexingTests(unittest.TestCase):
    def _config(self, root: Path) -> ProjectConfig:
        return ProjectConfig(text_collapse_scope="year", 
            output_dir=root,
            targets=["example.com/*"],
            keywords=["example"],
            from_date="20010101000000",
            to_date="20010101235959",
            cdx_delay=0,
            network=NetworkConfig(index_strategy="paged", page_blocks=2),
        ).normalized()

    def test_broad_query_uses_page_count_then_page_request(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = self._config(root)
            database = open_database(root)
            calls: list[list[tuple[str, str]]] = []
            page_payload = [
                ["timestamp", "original", "mimetype", "statuscode", "digest", "length"],
                ["20010101120000", "http://example.com/page", "text/html", "200", "ABC", "100"],
            ]

            def fake_get_cdx_any(_self, _urls, params, max_bytes=64 * 1024 * 1024, prefer_text=False):
                calls.append(list(params))
                return 1 if len(calls) == 1 else page_payload

            with patch("archive_scout.cdx.client.HttpClient.get_cdx_any", new=fake_get_cdx_any):
                index_archive(config, database, threading.Event())

            self.assertEqual(len(calls), 2)
            count_params = dict(calls[0])
            page_params = dict(calls[1])
            self.assertEqual(count_params["showNumPages"], "true")
            self.assertEqual(count_params["pageSize"], "2")
            self.assertEqual(page_params["page"], "0")
            self.assertEqual(page_params["pageSize"], "2")
            self.assertEqual(database.execute("SELECT COUNT(*) FROM captures").fetchone()[0], 1)
            self.assertEqual(database.execute("SELECT complete FROM index_state").fetchone()[0], 1)
            database.close()

    def test_http_400_page_count_falls_back_to_resume_keys(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = self._config(root)
            database = open_database(root)
            resume_payload = [
                ["timestamp", "original", "mimetype", "statuscode", "digest", "length"],
                ["20010101120000", "http://example.com/page", "text/html", "200", "ABC", "100"],
            ]
            responses = iter([RuntimeError("HTTP 400: pagination unavailable"), resume_payload])

            def fake_get_cdx_any(_self, _urls, _params, max_bytes=64 * 1024 * 1024, prefer_text=False):
                response = next(responses)
                if isinstance(response, BaseException):
                    raise response
                return response

            with patch("archive_scout.cdx.client.HttpClient.get_cdx_any", new=fake_get_cdx_any):
                index_archive(config, database, threading.Event())

            self.assertEqual(database.execute("SELECT COUNT(*) FROM captures").fetchone()[0], 1)
            state = database.execute("SELECT complete,resume_key FROM index_state").fetchone()
            self.assertEqual(state["complete"], 1)
            self.assertIsNone(state["resume_key"])
            database.close()


if __name__ == "__main__":
    unittest.main()
