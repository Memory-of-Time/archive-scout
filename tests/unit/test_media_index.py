from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.config import MediaConfig, NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_media_target
from archive_scout.media.indexer import index_media, media_index_state_signature, media_query_signature
from archive_scout.utils import utc_now


class MediaIndexTests(unittest.TestCase):
    def test_earlier_complete_media_state_is_refreshed_without_duplicate_capture_signature(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                output_dir=root,
                targets=["example.com/*"],
                keywords=["x"],
                from_date="20010101000000",
                to_date="20010101235959",
                cdx_delay=0,
                network=NetworkConfig(index_strategy="resume"),
                media=MediaConfig(
                    enabled=True,
                    include_images=True,
                    include_videos=False,
                    include_extensions=["jpg"],
                    discover_embedded=False,
                ),
            ).normalized()
            database = open_database(root)
            target_id = get_or_create_media_target(database, "example.com/*")
            capture_signature = media_query_signature(config)
            state_signature = media_index_state_signature(config)
            self.assertNotEqual(capture_signature, state_signature)
            with database:
                database.execute(
                    """INSERT INTO media_index_state(
                        target_id,extension,year,query_signature,resume_key,complete,seen,error_id,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?)""",
                    (target_id, "__all__", 2001, capture_signature, None, 1, 0, None, utc_now()),
                )
            payload = [
                ["timestamp", "original", "mimetype", "statuscode", "digest", "length"],
                ["20010101010101", "http://example.com/photo.jpg", "image/jpeg", "200", "ABC", "12"],
            ]
            with patch("archive_scout.cdx.client.HttpClient.get_cdx_any", return_value=payload) as mocked:
                index_media(config, database, threading.Event())
            self.assertEqual(mocked.call_count, 1)
            capture = database.execute("SELECT query_signature,state FROM media_captures").fetchone()
            self.assertEqual(capture["query_signature"], capture_signature)
            self.assertEqual(capture["state"], "pending")
            refreshed = database.execute(
                "SELECT complete FROM media_index_state WHERE query_signature=?", (state_signature,)
            ).fetchone()
            self.assertEqual(refreshed["complete"], 1)
            database.close()

    def test_all_extensions_use_one_cdx_query_per_window(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                output_dir=root,
                targets=["example.com/*"],
                keywords=["x"],
                from_date="200101",
                to_date="200101",
                cdx_delay=0,
                cdx_match_type="prefix",
                network=NetworkConfig(index_strategy="resume"),
                media=MediaConfig(
                    enabled=True,
                    include_images=True,
                    include_videos=True,
                    include_extensions=["jpg", "png", "mp4", "wmv"],
                    discover_embedded=False,
                ),
            ).normalized()
            database = open_database(root)
            with patch("archive_scout.cdx.client.HttpClient.get_cdx_any", return_value=[]) as mocked:
                index_media(config, database, threading.Event())
            self.assertEqual(mocked.call_count, 1)
            params = mocked.call_args.args[1]
            original_filters = [value for key, value in params if key == "filter" and value.startswith("original:")]
            self.assertEqual(len(original_filters), 1)
            for extension in ("jpg", "png", "mp4", "wmv"):
                self.assertIn(extension, original_filters[0])
            self.assertNotIn("~original:", original_filters[0])
            self.assertIn("[?&#;]", original_filters[0])
            self.assertEqual(dict(params)["url"], "example.com/")
            state = database.execute("SELECT extension,complete FROM media_index_state").fetchone()
            self.assertEqual(state["extension"], "__all__")
            self.assertEqual(state["complete"], 1)
            database.close()


if __name__ == "__main__":
    unittest.main()
