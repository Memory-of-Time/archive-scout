from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from archive_scout.config import MediaConfig
from archive_scout.constants import SCHEMA_VERSION, VERSION
from archive_scout.events import ProgressEvent
from archive_scout.ui.dashboard import format_media_policy_summary, format_progress_message, read_dashboard_counts
from archive_scout.ui.main_window import enforce_active_keyword_set_selection


class V108ReleaseTests(unittest.TestCase):
    def test_public_release_identity_and_schema(self):
        self.assertEqual(VERSION, "1.0.8")
        self.assertEqual(SCHEMA_VERSION, 13)


    def test_gui_next_scan_keyword_selection_is_exclusive(self):
        sets = [
            {"name": "Imported old A", "rules": ["alpha"], "selected": True},
            {"name": "Imported old B", "rules": ["beta"], "selected": True},
            {"name": "Current", "rules": ["gamma"], "selected": False},
        ]
        enforce_active_keyword_set_selection(sets, 2, True)
        self.assertEqual([item["selected"] for item in sets], [False, False, True])
        self.assertEqual([item["rules"] for item in sets], [["alpha"], ["beta"], ["gamma"]])
        enforce_active_keyword_set_selection(sets, 2, False)
        self.assertEqual([item["selected"] for item in sets], [False, False, False])

    def test_dashboard_media_counts_are_separate_from_text_counts(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "archive_scout.sqlite3"
            db = sqlite3.connect(path)
            db.executescript(
                """
                CREATE TABLE captures(
                    id INTEGER PRIMARY KEY, state TEXT, skip_reason TEXT
                );
                CREATE TABLE documents(id INTEGER PRIMARY KEY);
                CREATE TABLE document_matches(id INTEGER PRIMARY KEY);
                CREATE TABLE errors(id INTEGER PRIMARY KEY, resolved INTEGER NOT NULL, ignored INTEGER NOT NULL);
                CREATE TABLE recovery_events(id INTEGER PRIMARY KEY);
                CREATE TABLE media_captures(
                    id INTEGER PRIMARY KEY, state TEXT NOT NULL, media_kind TEXT NOT NULL
                );
                CREATE TABLE media_discovery_queue(
                    id INTEGER PRIMARY KEY, source_type TEXT NOT NULL, state TEXT NOT NULL
                );

                INSERT INTO captures(state,skip_reason) VALUES('pending',NULL);
                INSERT INTO media_captures(state,media_kind) VALUES('pending','image');
                INSERT INTO media_captures(state,media_kind) VALUES('downloading','video');
                INSERT INTO media_captures(state,media_kind) VALUES('downloaded','image');
                INSERT INTO media_captures(state,media_kind) VALUES('skipped_strategy','image');
                INSERT INTO media_captures(state,media_kind) VALUES('skipped','video');
                INSERT INTO media_captures(state,media_kind) VALUES('error','video');
                INSERT INTO media_discovery_queue(source_type,state) VALUES('text_validation_deferred','pending');
                INSERT INTO media_discovery_queue(source_type,state) VALUES('text_validation_deferred','complete');
                """
            )
            db.commit(); db.close()
            counts = read_dashboard_counts(path)
            self.assertEqual(counts['captures'], 1)
            self.assertEqual(counts['media_candidates'], 6)
            self.assertEqual(counts['media_selected'], 4)
            self.assertEqual(counts['media_pending'], 1)
            self.assertEqual(counts['media_downloading'], 1)
            self.assertEqual(counts['media_downloaded'], 1)
            self.assertEqual(counts['media_excluded'], 2)
            self.assertEqual(counts['media_errors'], 1)
            self.assertEqual(counts['media_selected_images'], 2)
            self.assertEqual(counts['media_selected_videos'], 2)
            self.assertEqual(counts['media_deferred_from_text'], 1)

    def test_dashboard_media_policy_lists_actual_extensions(self):
        media = MediaConfig(
            enabled=True,
            include_images=True,
            include_videos=False,
            include_extensions=['.jpg', '.jpeg'],
            exclude_extensions=['.gif'],
            snapshot_strategy='earliest',
        )
        summary = format_media_policy_summary(media)
        self.assertIn('Enabled: images', summary)
        self.assertIn('Include extensions (2): .jpg, .jpeg', summary)
        self.assertIn('Exclude extensions: .gif', summary)
        self.assertIn('Snapshot: earliest', summary)

    def test_progress_status_is_phase_explicit_and_rewrites_legacy_year_summary(self):
        current = ProgressEvent(
            'index', 'example.com/* • 20000101000000–20091231235959 • resume-key continuation; seen 10,000',
            0, 1, {'phase': 'resume-key continuation'},
        )
        rendered = format_progress_message(current)
        self.assertTrue(rendered.startswith('Text indexing — '))
        self.assertIn('resume-key continuation', rendered)
        legacy = format_progress_message(ProgressEvent('index', 'Indexing 1 year(s), 0 completed.', 0, 1))
        self.assertNotIn('year(s)', legacy)
        self.assertIn('saved index plan', legacy)


if __name__ == '__main__':
    unittest.main()
