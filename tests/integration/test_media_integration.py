from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.config import NetworkConfig, MediaConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.media.downloader import download_media
from archive_scout.media.extensions import allowed_media_url, extension_from_url, selected_extensions
from archive_scout.media.indexer import index_media


class MediaIntegrationTests(unittest.TestCase):
    def test_extension_include_exclude(self):
        media = MediaConfig(include_extensions=["jpg", "gif", "mp4"], exclude_extensions=["gif"])
        self.assertEqual(selected_extensions(media), [".jpg", ".mp4"])

    def test_historical_tracking_suffixes_and_extensionless_mime_are_media(self):
        media = MediaConfig(include_extensions=["jpg", "wmv", "swf"], include_images=True, include_videos=True)
        self.assertEqual(extension_from_url("http://example.com/photo.jpg&ref=thumb"), ".jpg")
        self.assertEqual(extension_from_url("http://example.com/get?file=clip.wmv&x=1"), ".wmv")
        self.assertEqual(extension_from_url("http://example.com/video.flv%3Fref=player"), ".flv")
        self.assertTrue(allowed_media_url("http://example.com/photo.jpg&ref=thumb", media)[0])
        allowed, kind, extension = allowed_media_url(
            "http://example.com/player?id=7", media, "application/x-shockwave-flash"
        )
        self.assertTrue(allowed)
        self.assertEqual(kind, "video")
        self.assertEqual(extension, "")

    def test_mocked_media_index_and_download(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                output_dir=root,
                targets=["example.com/*"],
                keywords=["example"],
                from_date="2006",
                to_date="2006",
                workers=2,
                cdx_delay=0,
                download_delay=0,
                network=NetworkConfig(index_strategy="resume"),
                media=MediaConfig(
                    targets=["example.com/images/*"],
                    include_images=True,
                    include_videos=False,
                    include_extensions=["jpg"],
                    exclude_extensions=[],
                    discover_embedded=False,
                    max_file_mb=10,
                ),
            ).normalized()
            cdx_payload = [
                ["timestamp", "original", "mimetype", "statuscode", "digest", "length"],
                ["20060102030405", "http://example.com/images/photo.jpg", "image/jpeg", "200", "ABC", "4"],
            ]
            with patch("archive_scout.cdx.client.HttpClient.get_cdx_any", return_value=cdx_payload):
                database = open_database(root)
                index_media(config, database, threading.Event())
                self.assertEqual(database.execute("SELECT COUNT(*) FROM media_captures").fetchone()[0], 1)
                def fake_download(_client, _url, destination, _max_bytes, _accept="*/*"):
                    data = b"JPEGDATA"
                    Path(destination).write_bytes(data)
                    return {
                        "path": Path(destination),
                        "bytes": len(data),
                        "content_hash": "bd90ecf41be5fbd72e359c78c906c9eed0b2d764e6ebf28ca7a4bd9505f072b6",
                        "preview": data,
                        "status": 200,
                        "headers": {"Content-Type": "image/jpeg"},
                        "final_url": "https://web.archive.org/web/20060102030405id_/http://example.com/images/photo.jpg",
                    }

                with patch("archive_scout.cdx.client.HttpClient.download_to_path", fake_download):
                    download_media(config, database, threading.Event())
                row = database.execute("SELECT state,path FROM media_captures").fetchone()
                self.assertEqual(row["state"], "downloaded")
                self.assertTrue(Path(row["path"]).exists())
                database.close()


if __name__ == "__main__":
    unittest.main()
