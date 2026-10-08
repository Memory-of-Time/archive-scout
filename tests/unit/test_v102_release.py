from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig
from archive_scout.constants import SCHEMA_VERSION, VERSION
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_captures
from archive_scout.downloads import downloader
from archive_scout.downloads.downloader import prepare_acquisition_rows
from archive_scout.ui.main_window import ArchiveScoutApp


class V102ReleaseTests(unittest.TestCase):
    def test_release_identity_and_schema(self):
        self.assertEqual(VERSION, "1.1.1")
        self.assertEqual(SCHEMA_VERSION, 13)

    def test_target_override_key_uses_same_normalized_identity_as_project_config(self):
        self.assertEqual(
            ArchiveScoutApp._target_override_key("https://example.com"),
            "example.com/*",
        )

    def test_target_specific_query_signature_survives_into_replay_selection(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                root,
                ["one.example/*", "two.example/*"],
                [],
                from_date="2000",
                to_date="2010",
                target_settings={
                    "two.example/*": {
                        "from_date": "2005",
                        "to_date": "2005",
                        "cdx_match_type": "prefix",
                    }
                },
            ).normalized()
            db = open_database(root)
            try:
                one = config.for_target("one.example/*")
                two = config.for_target("two.example/*")
                with db:
                    one_id = get_or_create_target(db, "one.example/*", one.settings_for_target("one.example/*"))
                    two_id = get_or_create_target(db, "two.example/*", two.settings_for_target("two.example/*"))
                    upsert_captures(db, [{
                        "urlkey": "example,one)/a",
                        "timestamp": "20040101000000",
                        "original": "http://one.example/a",
                        "mimetype": "text/html",
                        "statuscode": "200",
                        "digest": "A",
                        "length": "20",
                    }], one_id, cdx_query_signature(one))
                    upsert_captures(db, [{
                        "urlkey": "example,two)/b",
                        "timestamp": "20050101000000",
                        "original": "http://two.example/b",
                        "mimetype": "text/html",
                        "statuscode": "200",
                        "digest": "B",
                        "length": "20",
                    }], two_id, cdx_query_signature(two))
                total, rows, _stats = prepare_acquisition_rows(db, config, patterns=None)
                urls = [str(row["original_url"]) for row in rows]
                self.assertEqual(total, 2)
                self.assertEqual(urls, ["http://one.example/a", "http://two.example/b"])
            finally:
                db.close()

    def test_per_target_replay_runtime_settings_create_target_phases(self):
        config = ProjectConfig(
            Path("."),
            ["one.example/*", "two.example/*"],
            [],
            workers=10,
            download_delay=0.125,
            target_settings={
                "two.example/*": {"workers": 3, "download_delay": 1.5},
            },
        ).normalized()
        phases = downloader._text_runtime_target_configs(config)
        self.assertEqual([item.targets for item in phases], [["one.example/*"], ["two.example/*"]])
        self.assertEqual(phases[0].workers, 10)
        self.assertEqual(phases[1].workers, 3)
        self.assertEqual(phases[1].download_delay, 1.5)

    def test_download_only_uses_each_target_runtime_config_once(self):
        config = ProjectConfig(
            Path("."),
            ["one.example/*", "two.example/*"],
            [],
            target_settings={"two.example/*": {"workers": 2, "download_delay": 1.0}},
        ).normalized()
        calls: list[tuple[list[str], int, float]] = []

        def fake_acquire(runtime_config, *_args, **_kwargs):
            calls.append((list(runtime_config.targets), runtime_config.workers, runtime_config.download_delay))
            return {"queued": 1, "downloaded": 1, "skipped": 0, "errors": 0, "elapsed": 0.01}

        with mock.patch.object(downloader, "_acquire_archive", side_effect=fake_acquire):
            stats = downloader.download_archive_only(
                config, mock.MagicMock(), threading.Event(), None
            )
        self.assertEqual(calls[0][0], ["one.example/*"])
        self.assertEqual(calls[1], (["two.example/*"], 2, 1.0))
        self.assertEqual(stats["queued"], 2)
        self.assertEqual(stats["downloaded"], 2)

    def test_multiline_editors_have_cross_platform_outline_contract(self):
        theme_source = Path("archive_scout/ui/theme.py").read_text(encoding="utf-8")
        ui_source = Path("archive_scout/ui/main_window.py").read_text(encoding="utf-8")
        self.assertIn("highlightthickness=1", theme_source)
        self.assertIn('relief="solid"', theme_source)
        self.assertIn('text="Include extensions — one per line"', ui_source)
        self.assertIn('text="Exclude extensions — one per line"', ui_source)


if __name__ == "__main__":
    unittest.main()
