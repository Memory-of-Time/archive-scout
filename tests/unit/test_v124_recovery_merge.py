"""Persistent retry scheduling and evolving-source WAL merge tests."""
from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from archive_scout.database.connection import open_database
from archive_scout.downloads.schedule import record_failure, clear_retry, next_due
from archive_scout.database.repositories import record_error, get_or_create_target, upsert_capture
from archive_scout.projects.merge import merge_projects
from tests.unit.test_hardening import HardeningTests


class PersistentRetryAndMerge(unittest.TestCase):
    def test_retry_checkpoint_survives_reopen_and_clears_on_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = open_database(root)
            target = get_or_create_target(db, "example.com/*")
            upsert_capture(db, {"original":"http://example.com/p", "timestamp":"20050101000000",
                 "mimetype":"text/html","statuscode":"200","digest":"","length":"12"},target,"sig")
            cid=int(db.execute("SELECT id FROM captures").fetchone()[0]); db.execute("UPDATE captures SET state='error' WHERE id=?",(cid,))
            record_error(db,"download","connection","unavailable",capture_id=cid,retryable=True)
            record_failure(db,cid,"connection",None,True,0.25,30)
            first=next_due(db);self.assertIsNotNone(first);self.assertGreater(first,time.time())
            db.commit();db.close()
            reopened=open_database(root)
            self.assertAlmostEqual(next_due(reopened),first,delta=0.001)
            record_failure(reopened,cid,"timeout",None,True,0.25,30)
            self.assertEqual(reopened.execute("SELECT failure_count FROM text_retry_schedule").fetchone()[0],2)
            clear_retry(reopened,cid)
            self.assertIsNone(next_due(reopened))
            reopened.close()

    def test_source_wal_growth_imports_only_new_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory); source=root/"source";dest=root/"destination"
            sd,run=HardeningTests()._project_with_match(source)
            sd.commit()
            destdb=open_database(dest)
            first=merge_projects(dest,source,destdb)
            self.assertEqual(first["scan_runs"],1)
            self.assertEqual(destdb.execute("SELECT COUNT(*) FROM scan_runs").fetchone()[0],1)
            self.assertEqual(destdb.execute("SELECT COUNT(*) FROM notes").fetchone()[0],1)
            # The SQLite WAL can change while the main .sqlite3 mtime/size stays fixed.
            sd.execute("INSERT INTO notes(match_id,capture_id,text,created_at,updated_at) "
                       "SELECT id,NULL,'new note',datetime('now'),datetime('now') FROM document_matches LIMIT 1")
            sd.execute("UPDATE document_matches SET score=12")
            sd.execute("UPDATE scan_runs SET status='complete',match_count=1")
            sd.commit()
            old_files = {str(path) for path in (dest/'captures').rglob('*.txt')}
            changed=merge_projects(dest,source,destdb)
            self.assertEqual(changed["scan_runs"],0)
            self.assertEqual({str(path) for path in (dest/'captures').rglob('*.txt')},old_files)
            self.assertEqual(destdb.execute("SELECT score FROM document_matches").fetchone()[0],12)
            self.assertEqual(destdb.execute("SELECT COUNT(*) FROM scan_runs").fetchone()[0],1)
            self.assertEqual(destdb.execute("SELECT COUNT(*) FROM notes").fetchone()[0],2)
            self.assertEqual(destdb.execute("SELECT COUNT(*) FROM document_matches").fetchone()[0],1)
            self.assertEqual(merge_projects(dest,source,destdb)["notes"],1)  # replay of same summary
            self.assertEqual(destdb.execute("SELECT COUNT(*) FROM notes").fetchone()[0],2)
            sd.close();destdb.close()

if __name__=='__main__': unittest.main()
