from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.config import KeywordSetConfig, MediaConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_captures, upsert_document
from archive_scout.media.indexer import index_external_embedded_media
from archive_scout.operations import run_project
from archive_scout.scanning.jobs import ScanJob
from archive_scout.utils import hash_text, normalize_cdx_date, normalize_search


class ReleaseTests(unittest.TestCase):
    def test_common_human_dates_are_normalized(self):
        self.assertEqual(normalize_cdx_date("09/01/2008"), "20080901000000")
        self.assertEqual(normalize_cdx_date("12/31/2009", end=True), "20091231235959")
        self.assertEqual(normalize_cdx_date("2008-09-01"), "20080901000000")

    def test_release_icon_assets_exist(self):
        root = Path(__file__).resolve().parents[2]
        png = root / "assets" / "archivescout.png"
        ico = root / "assets" / "archivescout.ico"
        icns = root / "assets" / "archivescout.icns"
        self.assertTrue(png.read_bytes().startswith(b"\x89PNG\r\n\x1a\n"))
        self.assertEqual(ico.read_bytes()[:4], b"\x00\x00\x01\x00")
        self.assertEqual(icns.read_bytes()[:4], b"icns")

    def test_windows_release_requires_valid_signature_when_requested(self):
        root = Path(__file__).resolve().parents[2]
        build = (root / "scripts" / "build_windows.ps1").read_text(encoding="utf-8")
        workflow = (root / ".github" / "workflows" / "build-and-release.yml").read_text(encoding="utf-8")
        windows_readme = (root / "packaging" / "windows" / "README-WINDOWS.txt").read_text(encoding="utf-8")
        self.assertIn("[switch]$RequireSigned", build)
        self.assertIn("Get-AuthenticodeSignature", build)
        self.assertIn("--noupx", build)
        self.assertIn("--icon assets/archivescout.ico", build)
        self.assertIn("uses: azure/login@v3", workflow)
        self.assertIn("uses: azure/artifact-signing-action@v2", workflow)
        self.assertIn("-SkipBuild -RequireSigned", workflow)
        self.assertIn("Unblock", windows_readme)
        self.assertIn("wdsi/filesubmission", windows_readme)
        installer = (root / "packaging" / "windows" / "Install Archive Scout.cmd").read_text(encoding="utf-8")
        uninstaller = (root / "packaging" / "windows" / "Uninstall Archive Scout.cmd").read_text(encoding="utf-8")
        install_script = (root / "packaging" / "windows" / "install.ps1").read_text(encoding="utf-8")
        self.assertNotIn("ExecutionPolicy Bypass", installer + uninstaller + install_script)

    def test_dedicated_operation_waits_for_text_scan_before_external_media(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                output_dir=root,
                targets=["example.com/*"],
                keywords=["mirror"],
                keyword_sets=[KeywordSetConfig("set", ["mirror"], True)],
                from_date="2008",
                to_date="2008",
                media=MediaConfig(include_extensions=["mp4"]),
            )
            order: list[str] = []
            job = ScanJob.create(1, "set", ["mirror"])
            with (
                patch("archive_scout.operations.index_archive", side_effect=lambda *a, **k: order.append("index_text")),
                patch("archive_scout.operations.prepare_scan_jobs", return_value=[job]),
                patch("archive_scout.operations.download_archive", side_effect=lambda *a, **k: order.append("download_scan_text")),
                patch("archive_scout.operations.finish_jobs"),
                patch("archive_scout.operations.generate_job_reports", return_value={}),
                patch("archive_scout.operations.index_external_embedded_media", side_effect=lambda *a, **k: order.append("index_external")),
                patch("archive_scout.operations.download_media", side_effect=lambda *a, **k: order.append("download_external")),
                patch("archive_scout.operations.generate_media_reports", return_value={}),
            ):
                run_project(config, "external_media_after_scan", threading.Event())
            self.assertEqual(
                order,
                ["index_text", "download_scan_text", "index_external", "download_external"],
            )

    def test_external_embedded_media_is_indexed_after_text_scan(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = open_database(root)
            target_id = get_or_create_target(database, "example.com/*")
            raw = '<p>Mirror: http://cdn.example.net/media/clip.mp4</p>'
            capture = {
                "original": "http://example.com/page.html",
                "timestamp": "20080101000000",
                "mimetype": "text/html",
                "statuscode": "200",
                "digest": "A",
                "length": str(len(raw)),
            }
            upsert_captures(database, [capture], target_id, "sig")
            capture_id = database.execute("SELECT id FROM captures").fetchone()[0]
            page = root / "page.html"
            page.write_text(raw, encoding="utf-8")
            upsert_document(
                database,
                capture_id,
                page,
                "External media",
                "Mirror",
                ["http://cdn.example.net/media/clip.mp4"],
                hash_text(raw),
                hash_text(normalize_search(raw)),
                len(raw),
            )
            database.commit()
            config = ProjectConfig(
                output_dir=root,
                targets=["example.com/*"],
                keywords=["mirror"],
                from_date="2008",
                to_date="2008",
                cdx_delay=0,
                media=MediaConfig(
                    include_images=False,
                    include_videos=True,
                    include_extensions=["mp4"],
                    discover_embedded=True,
                    allow_external_embeds=True,
                ),
            )
            payload = [
                ["timestamp", "original", "mimetype", "statuscode", "digest", "length"],
                ["20080102000000", "http://cdn.example.net/media/clip.mp4", "video/mp4", "200", "B", "10"],
            ]
            with patch("archive_scout.cdx.client.HttpClient.get_cdx_any", return_value=payload):
                index_external_embedded_media(config, database, threading.Event())
            row = database.execute(
                "SELECT original_url,source_type FROM media_captures"
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["original_url"], "http://cdn.example.net/media/clip.mp4")
            self.assertEqual(row["source_type"], "external_embedded")
            database.close()


if __name__ == "__main__":
    unittest.main()
