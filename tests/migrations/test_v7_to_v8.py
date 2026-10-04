from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from archive_scout.database.connection import open_database


V7_MINIMAL = r'''
CREATE TABLE schema_info(version INTEGER NOT NULL);
INSERT INTO schema_info(version) VALUES(7);
CREATE TABLE project_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE targets(id INTEGER PRIMARY KEY AUTOINCREMENT,pattern TEXT NOT NULL UNIQUE,enabled INTEGER NOT NULL DEFAULT 1,settings_json TEXT,created_at TEXT NOT NULL);
CREATE TABLE captures(
 id INTEGER PRIMARY KEY AUTOINCREMENT, original_url TEXT NOT NULL, timestamp TEXT NOT NULL,
 target_id INTEGER, query_signature TEXT NOT NULL, mimetype TEXT,statuscode TEXT,digest TEXT,
 length INTEGER NOT NULL DEFAULT 0,state TEXT NOT NULL DEFAULT 'pending',download_attempts INTEGER NOT NULL DEFAULT 0,
 document_id INTEGER,http_status INTEGER,final_url TEXT,bytes_saved INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
 UNIQUE(original_url,timestamp,query_signature));
CREATE TABLE documents(
 id INTEGER PRIMARY KEY AUTOINCREMENT,capture_id INTEGER NOT NULL UNIQUE,path TEXT NOT NULL,title TEXT,body_text TEXT,links_json TEXT,
 content_hash TEXT,normalized_hash TEXT,size_bytes INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
CREATE TABLE media_captures(
 id INTEGER PRIMARY KEY AUTOINCREMENT,original_url TEXT NOT NULL,timestamp TEXT NOT NULL,target_id INTEGER,source_document_id INTEGER,
 source_type TEXT NOT NULL DEFAULT 'cdx',query_signature TEXT NOT NULL,media_kind TEXT NOT NULL,extension TEXT,mimetype TEXT,statuscode TEXT,digest TEXT,
 length INTEGER NOT NULL DEFAULT 0,state TEXT NOT NULL DEFAULT 'pending',download_attempts INTEGER NOT NULL DEFAULT 0,path TEXT,http_status INTEGER,
 final_url TEXT,bytes_saved INTEGER NOT NULL DEFAULT 0,content_hash TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
 UNIQUE(original_url,timestamp,query_signature));
'''


class V7ToV8MigrationTests(unittest.TestCase):
    def test_v7_migrates_storage_resume_and_hitlist_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = sqlite3.connect(root / 'archive_scout.sqlite3')
            database.executescript(V7_MINIMAL)
            database.execute(
                "INSERT INTO captures(original_url,timestamp,query_signature,state,created_at,updated_at) VALUES(?,?,?,?,?,?)",
                ('http://example.com/a', '20010101000000', 'sig', 'pending', 'now', 'now'),
            )
            database.commit()
            database.close()

            modern = open_database(root)
            self.assertEqual(modern.execute('SELECT version FROM schema_info').fetchone()[0], 12)
            capture_columns = {row['name'] for row in modern.execute('PRAGMA table_info(captures)')}
            for name in ('skip_reason', 'classifier_revision', 'local_path', 'content_hash', 'detected_encoding'):
                self.assertIn(name, capture_columns)
            document_columns = {row['name'] for row in modern.execute('PRAGMA table_info(documents)')}
            for name in ('body_zlib', 'body_chars', 'original_url'):
                self.assertIn(name, document_columns)
            for table in ('index_pages', 'media_index_pages', 'quick_search_runs', 'quick_search_hits', 'recovery_events', 'storage_objects'):
                self.assertIsNotNone(modern.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone(), table)
            modern.close()
            self.assertTrue(any((root / 'backups').glob('*.sqlite3.gz')))


if __name__ == '__main__':
    unittest.main()
