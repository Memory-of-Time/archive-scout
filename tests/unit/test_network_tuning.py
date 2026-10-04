from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from archive_scout.cdx.indexer import PendingWindow, _request_resume
from archive_scout.cdx.parameters import build_cdx_params, build_paged_cdx_params
from archive_scout.config import ProjectConfig, load_project_config


class NetworkTuningTests(unittest.TestCase):
    def test_new_projects_use_safe_edge_network_defaults(self):
        config = ProjectConfig(output_dir=Path("."), targets=["example.com/*"], keywords=[]).normalized()
        self.assertEqual(config.page_size, 100000)
        self.assertEqual(config.cdx_delay, 2.5)
        self.assertEqual(config.network.page_blocks, 0)
        self.assertEqual(config.network.cdx_workers, 10)
        self.assertEqual(config.workers, 10)
        self.assertEqual(config.scan_workers, 0)
        self.assertEqual(config.max_file_mb, 25.0)
        self.assertEqual(config.download_delay, 0.125)
        self.assertEqual(config.backup_keep, 5)
        self.assertEqual(config.backup_max_mb, 1024.0)
        self.assertFalse(config.auto_backup)
        self.assertEqual(config.network.retry_max_seconds, 300.0)

    def test_paged_and_resume_requests_use_larger_batches(self):
        config = ProjectConfig(
            output_dir=Path("."),
            targets=["example.com/*"],
            keywords=[],
            from_date="2001",
            to_date="2001",
        ).normalized()
        paged = dict(build_paged_cdx_params(config, "example.com/*", "20010101000000", "20011231235959", 0))
        resume = dict(build_cdx_params(config, "example.com/*", "20010101000000", "20011231235959"))
        self.assertEqual(paged["pageSize"], "9")
        self.assertEqual(resume["limit"], "100000")


    def test_resume_response_budget_scales_with_larger_page(self):
        class RecordingClient:
            def __init__(self):
                self.max_bytes = 0

            def get_cdx_any(self, _endpoints, _params, max_bytes, prefer_text):
                self.max_bytes = max_bytes
                self.assert_prefer_text = prefer_text
                return []

        config = ProjectConfig(
            output_dir=Path("."),
            targets=["example.com/*"],
            keywords=[],
            from_date="2001",
            to_date="2001",
        ).normalized()
        client = RecordingClient()
        rows, finished = _request_resume(
            client, config, "example.com/*", PendingWindow("20010101000000", "20011231235959")
        )
        self.assertEqual(rows, [])
        self.assertTrue(finished)
        self.assertTrue(client.assert_prefer_text)
        self.assertGreaterEqual(client.max_bytes, 100000 * 1536)

    def test_v103_untouched_download_defaults_upgrade_to_fast_replay_profile(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "project.json"
            path.write_text(json.dumps({
                "version": "1.0.5",
                "output_dir": temp,
                "targets": ["example.com/*"],
                "keywords": [],
                "from_date": "2001",
                "to_date": "2001",
                "workers": 4,
                "download_delay": 0.5,
            }), encoding="utf-8")
            config = load_project_config(path)
            self.assertEqual(config.workers, 10)
            self.assertEqual(config.download_delay, 0.125)

    def test_custom_download_profile_is_preserved(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "project.json"
            path.write_text(json.dumps({
                "version": "1.0.5",
                "output_dir": temp,
                "targets": ["example.com/*"],
                "keywords": [],
                "from_date": "2001",
                "to_date": "2001",
                "workers": 7,
                "download_delay": 0.2,
            }), encoding="utf-8")
            config = load_project_config(path)
            self.assertEqual(config.workers, 7)
            self.assertEqual(config.download_delay, 0.2)

    def test_earlier_untouched_defaults_upgrade(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "project.json"
            path.write_text(json.dumps({
                "version": "prerelease",
                "output_dir": temp,
                "targets": ["example.com/*"],
                "keywords": [],
                "from_date": "2001",
                "to_date": "2001",
                "page_size": 25000,
                "cdx_delay": 0.75,
                "network": {"page_blocks": 6, "cdx_workers": 6},
            }), encoding="utf-8")
            config = load_project_config(path)
            self.assertEqual(config.page_size, 100000)
            self.assertEqual(config.network.page_blocks, 9)
            self.assertEqual(config.network.cdx_workers, 10)
            self.assertEqual(config.cdx_delay, 2.5)

    def test_earlier_custom_network_settings_are_preserved(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "project.json"
            path.write_text(json.dumps({
                "version": "prerelease",
                "output_dir": temp,
                "targets": ["example.com/*"],
                "keywords": [],
                "from_date": "2001",
                "to_date": "2001",
                "page_size": 25000,
                "cdx_delay": 0.75,
                "network": {"page_blocks": 5, "cdx_workers": 4},
            }), encoding="utf-8")
            config = load_project_config(path)
            self.assertEqual(config.page_size, 25000)
            self.assertEqual(config.network.page_blocks, 5)
            self.assertEqual(config.network.cdx_workers, 4)
            self.assertEqual(config.cdx_delay, 2.5)


if __name__ == "__main__":
    unittest.main()
