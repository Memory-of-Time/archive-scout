from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import CDXRows
from archive_scout.config import MediaConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_capture, upsert_document
from archive_scout.media.discovery import discover_media, unwrap_wayback_url
from archive_scout.media.indexer import index_external_embedded_media
from archive_scout.utils import hash_text, normalize_search


class _EmbeddedCDXClient:
    calls = 0
    seen_urls = []

    def __init__(self, *args, **kwargs) -> None:
        pass

    def close(self) -> None:
        pass

    def get_cdx_rows_any(self, urls, params, max_bytes=0, prefer_text=True):
        type(self).calls += 1
        original = dict(params).get("url", "")
        type(self).seen_urls.append(original)
        return CDXRows([(
            "20020102030405", original, "image/jpeg", "200", "DIGEST", "1234"
        )])


class ExternalMediaDiscoveryTests(unittest.TestCase):
    def test_discovers_modern_lazy_social_css_and_video_media(self):
        raw = """
        <html><head>
          <meta property="og:image" content="https://cdn.example.net/share.jpg">
          <meta property="og:video" content="https://video.example.net/watch">
          <link rel="preload" as="image" href="https://cdn.example.net/preload.webp">
        </head><body background="https://cdn.example.net/bg.png">
          <img data-lazy-src="https://cdn.example.net/lazy.jpg"
               data-srcset="https://cdn.example.net/a.jpg 1x, https://cdn.example.net/b.jpg 2x">
          <video data-src="https://video.example.net/stream" data-poster="https://cdn.example.net/poster.jpg"></video>
          <source type="video/mp4" data-src="https://video.example.net/movie.mp4">
          <div style="background-image:url('https://cdn.example.net/css.png')"></div>
        </body></html>
        """
        media = MediaConfig(enabled=True, include_images=True, include_videos=True)
        found = {item.url: item.kind_hint for item in discover_media(raw, "http://example.com/page", media)}
        expected = {
            "https://cdn.example.net/share.jpg": "image",
            "https://video.example.net/watch": "video",
            "https://cdn.example.net/preload.webp": "image",
            "https://cdn.example.net/bg.png": "image",
            "https://cdn.example.net/lazy.jpg": "image",
            "https://cdn.example.net/a.jpg": "image",
            "https://cdn.example.net/b.jpg": "image",
            "https://video.example.net/stream": "video",
            "https://cdn.example.net/poster.jpg": "image",
            "https://video.example.net/movie.mp4": "video",
            "https://cdn.example.net/css.png": "image",
        }
        for url, kind in expected.items():
            self.assertEqual(found.get(url), kind, url)

    def test_wayback_replay_urls_are_unwrapped_before_lookup(self):
        replay = "https://web.archive.org/web/20010911000000im_/https://cdn.example.net/image.jpg?x=1"
        self.assertEqual(unwrap_wayback_url(replay), "https://cdn.example.net/image.jpg?x=1")


    def test_external_boundary_uses_scanned_site_not_optional_media_targets(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = (
                '<html><body>'
                '<img src="http://example.com/internal.jpg">'
                '<img src="https://cdn.example.net/external.jpg">'
                '</body></html>'
            )
            path = root / "page.html"
            path.write_text(raw, encoding="utf-8")
            database = open_database(root)
            target_id = get_or_create_target(database, "example.com/*")
            upsert_capture(database, {
                "original": "http://example.com/page",
                "timestamp": "20020101000000",
                "mimetype": "text/html",
                "statuscode": "200",
                "digest": "PAGE2",
                "length": str(path.stat().st_size),
            }, target_id, "text-sig")
            capture_id = int(database.execute("SELECT id FROM captures").fetchone()[0])
            upsert_document(
                database, capture_id, path, "Page", "", [], hash_text(raw),
                hash_text(normalize_search(raw)), path.stat().st_size,
            )
            database.commit()
            config = ProjectConfig(
                output_dir=root,
                targets=["example.com/*"],
                keywords=["page"],
                media=MediaConfig(
                    enabled=True, discover_embedded=True, allow_external_embeds=True,
                    targets=["cdn.example.net/*"],
                ),
                cdx_delay=0,
            )
            _EmbeddedCDXClient.calls = 0
            _EmbeddedCDXClient.seen_urls = []
            with patch("archive_scout.media.indexer.HttpClient", _EmbeddedCDXClient):
                index_external_embedded_media(config, database, threading.Event())
            self.assertEqual(_EmbeddedCDXClient.seen_urls, ["https://cdn.example.net/external.jpg"])
            database.close()

    def test_external_media_queue_resolves_once_and_reuses_unchanged_document(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = '<html><body><img data-lazy-src="https://cdn.example.net/photo.jpg"></body></html>'
            path = root / "page.html"
            path.write_text(raw, encoding="utf-8")
            database = open_database(root)
            target_id = get_or_create_target(database, "example.com/*")
            upsert_capture(database, {
                "original": "http://example.com/page",
                "timestamp": "20020101000000",
                "mimetype": "text/html",
                "statuscode": "200",
                "digest": "PAGE",
                "length": str(path.stat().st_size),
            }, target_id, "text-sig")
            capture_id = int(database.execute("SELECT id FROM captures").fetchone()[0])
            upsert_document(
                database, capture_id, path, "Page", "", [], hash_text(raw),
                hash_text(normalize_search(raw)), path.stat().st_size,
            )
            database.commit()
            config = ProjectConfig(
                output_dir=root,
                targets=["example.com/*"],
                keywords=["page"],
                media=MediaConfig(enabled=True, discover_embedded=True, allow_external_embeds=True),
                cdx_delay=0,
            )
            _EmbeddedCDXClient.calls = 0
            with patch("archive_scout.media.indexer.HttpClient", _EmbeddedCDXClient):
                signature = index_external_embedded_media(config, database, threading.Event())
            self.assertEqual(_EmbeddedCDXClient.calls, 1)
            row = database.execute(
                "SELECT original_url,source_type,state FROM media_captures WHERE query_signature=?",
                (signature,),
            ).fetchone()
            self.assertEqual(row["original_url"], "https://cdn.example.net/photo.jpg")
            self.assertEqual(row["source_type"], "external_embedded")
            self.assertEqual(row["state"], "pending")

            _EmbeddedCDXClient.calls = 0
            with patch("archive_scout.media.indexer.HttpClient", _EmbeddedCDXClient):
                index_external_embedded_media(config, database, threading.Event())
            self.assertEqual(_EmbeddedCDXClient.calls, 0)
            self.assertEqual(database.execute("SELECT COUNT(*) FROM media_captures").fetchone()[0], 1)
            database.close()


if __name__ == "__main__":
    unittest.main()
