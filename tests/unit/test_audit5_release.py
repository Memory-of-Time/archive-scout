from __future__ import annotations

import hashlib
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from archive_scout.cdx.indexer import PagedBatch, index_archive
from archive_scout.cdx.parameters import build_paged_cdx_params, cdx_query_signature
from archive_scout.classification import RESOURCE_CLASSIFIER_REVISION, classify_indexed_resource
from archive_scout.config import MediaConfig, NetworkConfig, ProjectConfig, ResearchConfig, load_project_config, save_project_config
from archive_scout.constants import VERSION
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import finish_operation_run, start_operation_run
from archive_scout.downloads.downloader import download_archive_only
from archive_scout.media.downloader import download_media, fetch_media
from archive_scout.media.indexer import build_media_params, media_query_signature
from archive_scout.network.transports import PreviewRejected
from archive_scout.cdx.client import RateLimitDeferred
from archive_scout.operations import run_project
from archive_scout.utils import utc_now


JPEG = b"\xff\xd8\xff\xe0" + b"J" * 256
PNG = b"\x89PNG\r\n\x1a\n" + b"P" * 256
HTML = b"<!doctype html><html><body>needle</body></html>"


class PrefixClient:
    def __init__(self, body: bytes, content_type: str):
        self.body = body
        self.content_type = content_type
        self.calls = 0
        self.bytes_consumed = 0

    def close(self):
        return None

    def download_to_path(self, _url, destination, _max_bytes, **kwargs):
        self.calls += 1
        validator = kwargs.get("preview_validator")
        prefix = self.body[:8192]
        self.bytes_consumed += len(prefix)
        if validator:
            rejected = validator({"content-type": self.content_type}, prefix)
            if rejected:
                raise PreviewRejected(rejected)
        Path(destination).write_bytes(self.body)
        self.bytes_consumed = len(self.body)
        return {
            "path": Path(destination),
            "bytes": len(self.body),
            "content_hash": hashlib.sha256(self.body).hexdigest(),
            "preview": self.body[:20000],
            "status": 200,
            "headers": {"content-type": self.content_type},
            "final_url": _url,
        }


class Audit5ReleaseTests(unittest.TestCase):
    def test_release_identity_and_historical_healthy_cdx_default(self):
        self.assertEqual(VERSION, "1.0.3")
        cfg = ProjectConfig(Path("."), ["example.com/*"], []).normalized()
        self.assertEqual(cfg.cdx_delay, 2.5)
        self.assertEqual(cfg.network.cdx_workers, 10)
        self.assertEqual(RESOURCE_CLASSIFIER_REVISION, 3)

    def test_descriptor_policy_includes_legacy_asx_ram_without_changing_default(self):
        self.assertEqual(classify_indexed_resource("http://x.test/list.asx", "video/x-ms-asf").resource_class, "media_descriptor")
        self.assertEqual(classify_indexed_resource("http://x.test/list.ram", "application/octet-stream").resource_class, "media_descriptor")
        cfg = ProjectConfig(Path("."), ["example.com/*"], []).normalized()
        self.assertFalse(cfg.search_media_descriptors)

    def test_paged_inventory_retains_archive_urlkey(self):
        cfg = ProjectConfig(Path("."), ["example.com/*"], [], network=NetworkConfig(index_strategy="paged")).normalized()
        params = dict(build_paged_cdx_params(cfg, "example.com/*", "20010101000000", "20011231235959", 0, 9))
        self.assertEqual(params["fl"], "urlkey,timestamp,original,mimetype,statuscode,digest,length")

    def test_broad_media_query_does_not_server_filter_by_url_extension_or_default_collapse(self):
        cfg = ProjectConfig(
            Path("."), ["example.com/*"], [],
            media=MediaConfig(enabled=True, include_images=True, include_videos=False, include_extensions=["jpg"]),
        ).normalized()
        params = build_media_params(cfg, "example.com/*", "20010101000000", "20011231235959", extensions=["jpg"])
        self.assertFalse(any(key == "filter" and value.startswith("original:") for key, value in params))
        self.assertFalse(any(key == "collapse" and value == "urlkey" for key, value in params))

    def test_text_worker_defers_ambiguous_media_and_consumes_only_prefix(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(
                root, ["example.com/*"], [], from_date="2001", to_date="2001", workers=1, download_delay=0,
                media=MediaConfig(enabled=True, include_images=True, include_videos=False, include_extensions=["png"], discover_embedded=False),
            ).normalized()
            db = open_database(root)
            sig = cdx_query_signature(cfg)
            now = utc_now()
            with db:
                db.execute(
                    """INSERT INTO captures(original_url,timestamp,query_signature,mimetype,statuscode,length,state,created_at,updated_at)
                       VALUES('http://example.com/get?id=1','20010101000000',?,'application/octet-stream','200',100000,'pending',?,?)""",
                    (sig, now, now),
                )
            client = PrefixClient(PNG + b"x" * 20000, "application/octet-stream")
            with mock.patch("archive_scout.downloads.downloader.HttpClient", return_value=client):
                download_archive_only(cfg, db, threading.Event(), None)
            row = db.execute("SELECT state,skip_reason,local_path FROM captures").fetchone()
            self.assertEqual((row["state"], row["skip_reason"]), ("skipped", "deferred_to_media"))
            self.assertIsNone(row["local_path"])
            self.assertEqual(client.calls, 1)
            self.assertLessEqual(client.bytes_consumed, 8192)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM media_captures").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM media_discovery_queue WHERE state='pending'").fetchone()[0], 1)
            db.close()

    def test_standard_media_phase_rejects_png_when_only_jpg_selected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(
                root, ["example.com/*"], [],
                media=MediaConfig(enabled=True, include_images=True, include_videos=False, include_extensions=["jpg"]),
            ).normalized()
            row = {
                "id": 1, "timestamp": "20010101000000", "original_url": "http://example.com/get?id=1",
                "media_kind": "image", "extension": ".jpg", "mimetype": "image/jpeg",
            }
            client = PrefixClient(PNG, "image/png")
            result = fetch_media(row, cfg, client)
            self.assertEqual(result["kind"], "rejected")
            self.assertIn("excluded_media_format:.png", result["reason"])
            self.assertFalse(any((root / "media").rglob("*.png")))

    def test_dynamic_jpeg_uses_detected_format_and_native_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(
                root, ["example.com/*"], [],
                media=MediaConfig(enabled=True, include_images=True, include_videos=False, include_extensions=["jpg"]),
            ).normalized()
            row = {
                "id": 1, "timestamp": "20010101000000", "original_url": "http://example.com/image.php?id=7",
                "media_kind": "image", "extension": ".jpg", "mimetype": "image/jpeg",
            }
            result = fetch_media(row, cfg, PrefixClient(JPEG, "image/jpeg"))
            self.assertEqual(result["kind"], "media")
            self.assertEqual(result["extension"], ".jpg")
            self.assertTrue(str(result["path"]).endswith(".jpg"))
            self.assertEqual(Path(result["path"]).read_bytes(), JPEG)

    def test_genuine_html_from_media_attempt_is_recovered_as_text(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg = ProjectConfig(
                Path(temp), ["example.com/*"], [],
                media=MediaConfig(enabled=True, include_images=True, include_extensions=["jpg"]),
            ).normalized()
            row = {
                "id": 1, "timestamp": "20010101000000", "original_url": "http://example.com/photo.jpg",
                "media_kind": "image", "extension": ".jpg", "mimetype": "image/jpeg",
            }
            result = fetch_media(row, cfg, PrefixClient(HTML, "text/html"))
            self.assertEqual(result["kind"], "recovered_text")

    def test_auto_healthy_resume_keeps_resume_key_without_switching_to_paged(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(
                root, ["example.com/*"], [], from_date="2001", to_date="2001", page_size=1000, cdx_delay=0,
                cdx_collapses=[], network=NetworkConfig(index_strategy="auto", cdx_workers=10),
            ).normalized()
            db = open_database(root)
            rows = [
                (f"com,example)/{i}", "20010101000000", f"http://example.com/{i}", "text/html", "200", str(i), "1")
                for i in range(1000)
            ]
            calls = []
            def fake_resume(_client, _cfg, _target, current):
                calls.append(("resume", current.resume_key))
                if current.resume_key is None:
                    current.resume_key = "next"
                    return list(rows), False
                return [], True
            with mock.patch("archive_scout.cdx.indexer._request_resume", side_effect=fake_resume), \
                 mock.patch("archive_scout.cdx.indexer._request_paged_batch", side_effect=AssertionError("healthy resume must not switch to paged")):
                index_archive(cfg, db, threading.Event())
            self.assertEqual(calls, [("resume", None), ("resume", "next")])
            event = db.execute("SELECT COUNT(*) FROM recovery_events WHERE category='auto_dense_paged'").fetchone()[0]
            self.assertEqual(event, 0)
            db.close()

    def test_combined_operation_calls_standard_media_only_after_text_pipeline(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(
                root, ["example.com/*"], ["needle"], from_date="2001", to_date="2001",
                media=MediaConfig(enabled=True, include_images=True, include_extensions=["jpg"]),
                research=ResearchConfig(enabled=False, auto_build=False),
            ).normalized()
            order = []
            fake_job = SimpleNamespace(scan_run_id=1, name="k")
            with mock.patch("archive_scout.operations.index_archive", side_effect=lambda *a, **k: order.append("index")), \
                 mock.patch("archive_scout.operations.prepare_scan_jobs", return_value=[fake_job]), \
                 mock.patch("archive_scout.operations.download_archive", side_effect=lambda *a, **k: order.append("text")), \
                 mock.patch("archive_scout.operations._run_standard_media_phase", side_effect=lambda c, *a, **k: order.append("media") or c), \
                 mock.patch("archive_scout.operations._pending_media_recovered_text", return_value=0), \
                 mock.patch("archive_scout.operations.finish_jobs"), \
                 mock.patch("archive_scout.operations.generate_job_reports", return_value={}), \
                 mock.patch("archive_scout.operations.generate_media_reports", return_value={}):
                run_project(cfg, "all", threading.Event())
            self.assertEqual(order, ["index", "text", "media"])

    def test_download_only_media_phase_starts_after_text_acquisition(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(
                root, ["example.com/*"], [], from_date="2001", to_date="2001",
                media=MediaConfig(enabled=True, include_images=True, include_extensions=["jpg"]),
                research=ResearchConfig(enabled=False, auto_build=False),
            ).normalized()
            order = []
            with mock.patch("archive_scout.operations.index_archive", side_effect=lambda *a, **k: order.append("index")), \
                 mock.patch("archive_scout.operations.download_archive_only", side_effect=lambda *a, **k: order.append("text") or {"downloaded":0,"skipped":0,"errors":0,"queued":0}), \
                 mock.patch("archive_scout.operations._run_standard_media_phase", side_effect=lambda c, *a, **k: order.append("media") or c), \
                 mock.patch("archive_scout.operations._pending_media_recovered_text", return_value=0), \
                 mock.patch("archive_scout.operations.generate_media_reports", return_value={}):
                run_project(cfg, "download_only", threading.Event())
            self.assertEqual(order, ["index", "text", "media"])

    def test_resume_restores_download_only_contract_and_media_phase(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            frozen = ProjectConfig(
                root, ["example.com/*"], [], from_date="2001", to_date="2001",
                media=MediaConfig(enabled=True, include_images=True, include_extensions=["jpg"]),
                research=ResearchConfig(enabled=False, auto_build=False),
            ).normalized()
            db = open_database(root)
            run_id = start_operation_run(db, "download_only", "old", config_json=__import__("json").dumps(frozen.to_payload()))
            finish_operation_run(db, run_id, "interrupted", "test")
            db.commit(); db.close()
            current = ProjectConfig(root, ["example.com/*"], []).normalized()
            order=[]
            with mock.patch("archive_scout.operations.index_archive", side_effect=lambda *a, **k: order.append("index")), \
                 mock.patch("archive_scout.operations.download_archive_only", side_effect=lambda *a, **k: order.append("text") or {"downloaded":0,"skipped":0,"errors":0,"queued":0}), \
                 mock.patch("archive_scout.operations._run_standard_media_phase", side_effect=lambda c, *a, **k: order.append("media") or c), \
                 mock.patch("archive_scout.operations._pending_media_recovered_text", return_value=0), \
                 mock.patch("archive_scout.operations.generate_media_reports", return_value={}):
                run_project(current, "resume", threading.Event())
            self.assertEqual(order, ["index", "text", "media"])

    def test_explicit_custom_cdx_pacing_round_trips_without_forced_migration(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(root, ["example.com/*"], [], cdx_delay=2.5).normalized()
            loaded = load_project_config(save_project_config(cfg))
            self.assertEqual(loaded.cdx_delay, 2.5)

    def test_known_media_metadata_causes_zero_text_replay_requests(self):
        class NoReplay:
            def __init__(self, *args, **kwargs): pass
            def close(self): pass
            def download_to_path(self, *args, **kwargs):
                raise AssertionError("known media must not reach text replay")
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            cfg=ProjectConfig(root,["example.com/*"],[],from_date="2001",to_date="2001",download_delay=0,
                media=MediaConfig(enabled=True,include_images=True,include_extensions=["jpg"])).normalized()
            db=open_database(root); now=utc_now(); sig=cdx_query_signature(cfg)
            with db:
                db.execute("""INSERT INTO captures(original_url,timestamp,query_signature,mimetype,statuscode,length,state,created_at,updated_at)
                    VALUES('http://example.com/a.jpg','20010101000000',?,'image/jpeg','200',100,'pending',?,?)""",(sig,now,now))
            with mock.patch("archive_scout.downloads.downloader.HttpClient", NoReplay):
                result=download_archive_only(cfg,db,threading.Event(),None)
            self.assertEqual(result["downloaded"],0)
            self.assertEqual(db.execute("SELECT state FROM captures").fetchone()[0],"skipped")
            db.close()

    def test_invalid_earliest_media_snapshot_promotes_next_eligible_snapshot(self):
        class MultiClient:
            def __init__(self,*args,**kwargs): self.calls=[]
            def close(self): pass
            def download_to_path(self,url,destination,_max_bytes,**kwargs):
                self.calls.append(url)
                body = PNG if "20010101000000" in url else JPEG
                ctype = "image/png" if body is PNG else "image/jpeg"
                validator=kwargs.get("preview_validator")
                if validator:
                    rejected=validator({"content-type":ctype},body)
                    if rejected: raise PreviewRejected(rejected)
                Path(destination).write_bytes(body)
                return {"bytes":len(body),"content_hash":hashlib.sha256(body).hexdigest(),"preview":body,
                        "status":200,"headers":{"content-type":ctype},"final_url":url}
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            cfg=ProjectConfig(root,["example.com/*"],[],from_date="2001",to_date="2001",download_delay=0,workers=1,
                media=MediaConfig(enabled=True,include_images=True,include_videos=False,include_extensions=["jpg"],snapshot_strategy="earliest")).normalized()
            db=open_database(root); sig=media_query_signature(cfg); now=utc_now()
            with db:
                for ts,state in (("20010101000000","pending"),("20010102000000","skipped_strategy")):
                    db.execute("""INSERT INTO media_captures(original_url,timestamp,query_signature,urlkey,media_kind,extension,mimetype,statuscode,length,state,created_at,updated_at)
                      VALUES('http://example.com/dynamic',?,?, 'com,example)/dynamic','image','.jpg','image/jpeg','200',100,?,?,?)""",
                      (ts,sig,state,now,now))
            with mock.patch("archive_scout.media.downloader.HttpClient", MultiClient):
                download_media(cfg,db,threading.Event())
            rows=db.execute("SELECT timestamp,state,skip_reason,path FROM media_captures ORDER BY timestamp").fetchall()
            self.assertEqual(rows[0]["state"],"skipped")
            self.assertIn("excluded_media_format:.png",rows[0]["skip_reason"])
            self.assertEqual(rows[1]["state"],"downloaded")
            self.assertTrue(Path(rows[1]["path"]).is_file())
            db.close()

    def test_retryable_text_pause_never_advances_into_media_phase(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            cfg=ProjectConfig(root,["example.com/*"],["needle"],from_date="2001",to_date="2001",
                media=MediaConfig(enabled=True,include_images=True,include_extensions=["jpg"]),
                research=ResearchConfig(enabled=False,auto_build=False)).normalized()
            fake_job=SimpleNamespace(scan_run_id=1,name="k")
            media=mock.Mock()
            with mock.patch("archive_scout.operations.index_archive"),                  mock.patch("archive_scout.operations.prepare_scan_jobs",return_value=[fake_job]),                  mock.patch("archive_scout.operations.download_archive",side_effect=RateLimitDeferred("paused",status=429,waited=0)),                  mock.patch("archive_scout.operations._run_standard_media_phase",media),                  mock.patch("archive_scout.operations.finish_jobs"):
                with self.assertRaises(RateLimitDeferred):
                    run_project(cfg,"all",threading.Event())
            media.assert_not_called()


if __name__ == "__main__":
    unittest.main()
