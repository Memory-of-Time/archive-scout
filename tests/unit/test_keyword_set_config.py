from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from archive_scout.config import KeywordSetConfig, MediaConfig, ProjectConfig, load_project_config, save_project_config


class KeywordSetConfigTests(unittest.TestCase):
    def test_legacy_multiple_checked_sets_scan_only_first_without_data_loss(self):
        with tempfile.TemporaryDirectory() as temp:
            original = ProjectConfig(
                output_dir=Path(temp), targets=[], keywords=[],
                keyword_sets=[
                    KeywordSetConfig("Old", ["old"], True),
                    KeywordSetConfig("Hidden", ["hidden"], True),
                    KeywordSetConfig("Later", ["later"], False),
                ],
            )
            self.assertEqual([s.name for s in original.selected_keyword_sets()], ["Old"])
            normalized = original.normalized()
            self.assertEqual([s.selected for s in normalized.keyword_sets], [True, False, False])
            saved = save_project_config(original)
            loaded = load_project_config(saved)
            self.assertEqual([s.name for s in loaded.normalized_keyword_sets()],
                             ["Old", "Hidden", "Later"])
            self.assertEqual([s.rules for s in loaded.normalized_keyword_sets()],
                             [["old"], ["hidden"], ["later"]])
            self.assertEqual([s.name for s in loaded.selected_keyword_sets()], ["Old"])

    def test_nonfirst_selected_set_is_legacy_scanner_source(self):
        config = ProjectConfig(Path('.'), [], [], keyword_sets=[
            KeywordSetConfig("Unselected", ["do not scan"], False),
            KeywordSetConfig("Enabled", ["search me"], True),
        ]).normalized()
        self.assertEqual([s.name for s in config.selected_keyword_sets()], ["Enabled"])
        self.assertEqual(config.keywords, ["search me"])
        self.assertEqual(config.keyword_set_name, "Enabled")
        unchecked = ProjectConfig(Path('.'), [], [], keyword_sets=[
            KeywordSetConfig("Unselected", ["do not scan"], False),
        ]).normalized()
        self.assertEqual(unchecked.selected_keyword_sets(), [])

    def test_multiple_keyword_sets_and_media_round_trip(self):
        with tempfile.TemporaryDirectory() as temp:
            config = ProjectConfig(
                output_dir=Path(temp),
                targets=["example.com/*"],
                keywords=[],
                keyword_sets=[
                    KeywordSetConfig("General", ["WTC", "jumper"], True),
                    KeywordSetConfig("Media", ["exact: skylight.mov"], False),
                ],
                retry_media_capture_ids=[7, 9],
                rate_limit_base_pause=45,
                rate_limit_max_pause=240,
                rate_limit_max_wait=900,
                rate_limit_attempts=10,
                media=MediaConfig(
                    enabled=True,
                    targets=["media.example.com/*"],
                    include_extensions=["jpg", ".mp4", ".gif"],
                    exclude_extensions=["gif"],
                    snapshot_strategy="latest",
                ),
            )
            path = save_project_config(config)
            loaded = load_project_config(path)
            self.assertEqual(len(loaded.normalized_keyword_sets()), 2)
            self.assertEqual([item.name for item in loaded.selected_keyword_sets()], ["General"])
            self.assertEqual(loaded.media.targets, ["media.example.com/*"])
            self.assertEqual(loaded.media.snapshot_strategy, "latest")
            self.assertIn(".mp4", loaded.media.include_extensions)
            self.assertIn(".gif", loaded.media.exclude_extensions)
            self.assertEqual(loaded.retry_media_capture_ids, [7, 9])
            self.assertEqual(loaded.rate_limit_base_pause, 45)
            self.assertEqual(loaded.rate_limit_max_pause, 240)
            self.assertEqual(loaded.rate_limit_max_wait, 900)
            self.assertEqual(loaded.rate_limit_attempts, 10)


if __name__ == "__main__":
    unittest.main()
