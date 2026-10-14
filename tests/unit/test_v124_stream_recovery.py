"""End-to-end local retained-body evidence and CDX terminal safeguards."""
from __future__ import annotations

import hashlib
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import CDXRows, HttpClient, MalformedCDXResponse
from archive_scout.cdx.indexer import PendingWindow, _request_resume
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_capture, get_or_create_keyword_set, start_scan_run
from archive_scout.downloads.downloader import fetch_retain_streamed, save_success, capture_path
from archive_scout.network.transports import Urllib3Backend
from archive_scout.scanning.jobs import ScanJob
from archive_scout.scanning.pending import scan_pending_saved, PayloadIntegrityError, _scan_one


class CapturedStreamAndCoverage(unittest.TestCase):
    def create_capture(self, root):
        config=ProjectConfig(output_dir=root,targets=["example.com/*"],keywords=["needle"],
            from_date="2000",to_date="2001",workers=2).normalized()
        db=open_database(root)
        target=get_or_create_target(db,config.targets[0])
        upsert_capture(db,{"original":"http://example.com/test","timestamp":"20010101000000",
            "mimetype":"text/html","statuscode":"200","digest":"","length":"120"},
            target,cdx_query_signature(config))
        scan_id=start_scan_run(db,get_or_create_keyword_set(db,"sample",["needle"]),"test",1,"all")
        db.commit()
        row=db.execute("SELECT * FROM captures LIMIT 1").fetchone()
        return config,db,row,[ScanJob.create(scan_id,"sample",["needle"])]

    def test_streamed_capture_is_checkpointed_scanned_and_hashed(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); config,db,row,jobs=self.create_capture(root)
            payload=b"<html><title>Archive</title><body>needle in a saved body</body></html>"
            class Client:
                def download_to_path(self, url, destination, max_bytes, *, redirect_validator=None):
                    Path(destination).write_bytes(payload)
                    return {"bytes":len(payload),"content_hash":hashlib.sha256(payload).hexdigest(),
                        "preview":payload,"headers":{"Content-Type":"text/html; charset=utf-8"},
                        "status":200,"final_url":url}
            result=fetch_retain_streamed(row,config,Client())
            self.assertTrue(result["path"].exists())
            self.assertEqual(result["content_hash"],hashlib.sha256(payload).hexdigest())
            save_success(db,result)
            self.assertEqual(db.execute("SELECT state FROM captures").fetchone()[0],"downloaded_unscanned")
            self.assertEqual(scan_pending_saved(db,config,jobs,threading.Event(),workers=2),1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0],1)
            self.assertGreater(db.execute("SELECT score FROM document_matches").fetchone()[0],0)
            self.assertEqual(db.execute("SELECT content_hash FROM capture_payload_evidence").fetchone()[0], result["content_hash"])
            db.close()

    def test_short_or_incomplete_response_never_publishes(self):
        with tempfile.TemporaryDirectory() as temp:
            config,db,row,jobs=self.create_capture(Path(temp))
            payload=b"<html>needle</html>"
            class BrokenClient:
                def download_to_path(self, url, destination, max_bytes, *, redirect_validator=None):
                    Path(destination).write_bytes(payload)
                    return {"bytes":len(payload)+10,"content_hash":hashlib.sha256(payload).hexdigest(),
                        "preview":payload,"headers":{"Content-Type":"text/html"},"status":200,"final_url":url}
            with self.assertRaises(OSError):
                fetch_retain_streamed(row,config,BrokenClient())
            final=capture_path(config.output_dir,row["id"],row["timestamp"],row["original_url"])
            self.assertFalse(final.exists());self.assertFalse(final.with_name(final.name+'.transfer').exists())
            db.close()

    def test_partial_stream_and_resume_sidecar_are_discarded(self):
        with tempfile.TemporaryDirectory() as temp:
            config,db,row,_=self.create_capture(Path(temp))
            class Interrupted:
                def download_to_path(self, url, destination, max_bytes, *, redirect_validator=None):
                    Path(destination).write_bytes(b"unverified prefix")
                    Path(str(destination)+".resume.json").write_text('{'+'"url":"old"'+'}')
                    raise OSError("injected connection reset")
            with self.assertRaisesRegex(OSError,"connection reset"):
                fetch_retain_streamed(row,config,Interrupted())
            final=capture_path(config.output_dir,row["id"],row["timestamp"],row["original_url"])
            self.assertFalse(final.exists())
            self.assertFalse(final.with_name(final.name+'.transfer').exists())
            self.assertFalse(final.with_name(final.name+'.transfer.resume.json').exists())
            db.close()

    def test_saved_body_hash_mismatch_blocks_scoring(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);file=root/'body.txt';file.write_bytes(b'altered source')
            with self.assertRaises(PayloadIntegrityError):
                _scan_one({"id":17,"path":str(file),"original_url":"http://example.com/",
                           "mimetype":"text/html","expected_hash":"0"*64,
                           "expected_size":file.stat().st_size},[])

    def test_concurrent_scan_waits_for_download_writer_and_keeps_coverage(self):
        """A long-lived acquisition writer cannot cause scan-commit busy failures."""
        import sqlite3
        import time
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config, db, row, jobs = self.create_capture(root)
            payload = b"<html><body>needle and durable scan evidence</body></html>"
            path = capture_path(root, row["id"], row["timestamp"], row["original_url"])
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
            save_success(db, {"capture_id": row["id"], "path": path,
                "bytes_saved": len(payload), "content_hash": hashlib.sha256(payload).hexdigest(),
                "http_status": 200, "final_url": row["original_url"],
                "response_content_type": "text/html", "deferred_scan": True})
            scanner = sqlite3.connect(root / "archive_scout.sqlite3", timeout=5,
                                      check_same_thread=False)
            scanner.row_factory = sqlite3.Row
            scanner.execute("PRAGMA busy_timeout=5000")
            writer_lock = threading.Lock()
            result = {}
            def run_scanner():
                try:
                    result["count"] = scan_pending_saved(scanner, config, jobs,
                        threading.Event(), workers=2, write_lock=writer_lock)
                except BaseException as exc:
                    result["error"] = exc
            try:
                with writer_lock, db:
                    db.execute("UPDATE captures SET updated_at=datetime('now') WHERE id=?", (row["id"],))
                    thread = threading.Thread(target=run_scanner, daemon=True)
                    thread.start()
                    time.sleep(0.1)
                    self.assertTrue(thread.is_alive(), "scanner must wait while owner writer commits")
                thread.join(timeout=5)
                self.assertFalse(thread.is_alive(), "scanner could not finish after writer released")
                self.assertNotIn("error", result)
                self.assertEqual(result.get("count"), 1)
                self.assertEqual(db.execute("SELECT state FROM captures").fetchone()[0], "downloaded")
                self.assertEqual(db.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0], 1)
            finally:
                scanner.close()
                db.close()

    def test_terminal_full_cdx_page_is_confirmed_once(self):
        config=ProjectConfig(output_dir=Path('/tmp/scout-test'),targets=['example.com/*'],keywords=['needle'],
            from_date='2000',to_date='2000').normalized()
        current=PendingWindow('20000101000000','20000101235959',page_size=2)
        rows=[('20000101010101','http://example.com/a','text/html','200','A','10'),
              ('20000101010202','http://example.com/b','text/html','200','B','10')]
        client=object.__new__(HttpClient)
        with patch('archive_scout.cdx.indexer.request_cdx_rows',side_effect=[CDXRows(rows),CDXRows(rows)]) as request:
            returned,complete,key=_request_resume(client,config,'example.com/*',current)
            self.assertTrue(complete);self.assertEqual(returned,rows);self.assertEqual(request.call_count,2)
        with patch('archive_scout.cdx.indexer.request_cdx_rows',side_effect=[CDXRows(rows),CDXRows(rows[:1])]):
            with self.assertRaises(MalformedCDXResponse):
                _request_resume(client,config,'example.com/*',current)

    def test_urllib3_direct_mode_ignores_proxy_environment(self):
        backend=Urllib3Backend(1,2,2,trust_env=False)
        try:
            with patch('urllib.request.getproxies',return_value={'https':'http://127.0.0.1:3128'}):
                self.assertIs(backend._pool_for('https://example.com'),backend.pool)
        finally:backend.close()

if __name__=='__main__':unittest.main()
