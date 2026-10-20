"""Completeness-first regressions for the 2026-10-12 Ogrish retry log audit."""
from __future__ import annotations

import base64
import hashlib
import tempfile
import threading
import unittest
from pathlib import Path

import httpx

from archive_scout.config import ProjectConfig
from archive_scout.media.downloader import fetch_media, media_path, _disambiguate_media_path
from archive_scout.network.transports import ResilientTransport, TransportExhaustedError


class ReplayCompletenessTests(unittest.TestCase):
    def make_cfg(self, root):
        return ProjectConfig(output_dir=Path(root), targets=['example.com/*'], keywords=[]).normalized()

    @staticmethod
    def row(data: bytes, *, url: str, timestamp='20010102030405', capture_id=1):
        return {'id': capture_id, 'timestamp': timestamp, 'original_url': url,
                'extension': '.jpg', 'media_kind': 'image',
                'digest': base64.b32encode(hashlib.sha1(data).digest()).decode('ascii').rstrip('=')}

    def test_verified_existing_file_is_reused_without_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = self.make_cfg(tmp)
            content = b'a valid archived image'
            row = self.row(content, url='http://example.com/photo.jpg')
            path = media_path(config.output_dir, row)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            class NoNetwork:
                def download_to_path(self, *args):
                    self.fail('Should not request verified complete bytes')
            result = fetch_media(row, config, NoNetwork())
            self.assertEqual(result['path'], path)
            self.assertEqual(result['bytes'], len(content))

    def test_unrelated_preexisting_basename_must_not_be_reused_or_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = self.make_cfg(tmp)
            expected = b'correct archived payload'
            row = self.row(expected, url='http://example.com/path/photo.jpg')
            original = media_path(config.output_dir, row)
            original.parent.mkdir(parents=True, exist_ok=True)
            original.write_bytes(b'file from another URL')
            class FakeNetwork:
                calls = 0
                def download_to_path(self, url, destination, max_bytes):
                    self.calls += 1
                    destination.write_bytes(expected)
                    return {'bytes': len(expected), 'content_hash': hashlib.sha256(expected).hexdigest(),
                            'headers': {'content-type':'image/jpeg'}, 'preview': expected,
                            'status': 200, 'final_url': url}
            client = FakeNetwork()
            result = fetch_media(row, config, client)
            self.assertEqual(client.calls, 1)
            self.assertEqual(original.read_bytes(), b'file from another URL')
            self.assertEqual(Path(result['path']).read_bytes(), expected)
            self.assertNotEqual(Path(result['path']), original)
            self.assertEqual(Path(result['path']).parent, original.parent)

    def test_unknown_existing_bytes_cannot_be_called_verified(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = self.make_cfg(tmp)
            row = {'id':7,'original_url':'http://example.com/x.jpg','timestamp':'20010101000000',
                   'media_kind':'image','extension':'.jpg'}
            original=media_path(config.output_dir,row)
            original.parent.mkdir(parents=True,exist_ok=True)
            original.write_bytes(b'unknown')
            class Fake:
                def download_to_path(self,url,destination,max_bytes):
                    destination.write_bytes(b'fresh')
                    return {'bytes':5,'content_hash':hashlib.sha256(b'fresh').hexdigest(),
                            'headers':{'content-type':'image/jpeg'},'preview':b'fresh',
                            'status':200,'final_url':url}
            result=fetch_media(row,config,Fake())
            self.assertEqual(original.read_bytes(),b'unknown')
            self.assertEqual(Path(result['path']).read_bytes(),b'fresh')

    def test_collision_suffix_is_stable_per_capture(self):
        row=self.row(b'data',url='http://example.com/x.jpg',capture_id=1)
        path=Path('/project/media/images/x.jpg')
        self.assertEqual(_disambiguate_media_path(path,row),_disambiguate_media_path(path,row))
        different=dict(row, timestamp='20030102030405')
        self.assertNotEqual(_disambiguate_media_path(path,row),_disambiguate_media_path(path,different))


class TransportDiagnosticTests(unittest.TestCase):
    def test_partial_response_is_not_reported_as_connection_setup_error(self):
        class BadResponseBackend:
            def request(self,*args,**kwargs):
                raise httpx.RemoteProtocolError('peer closed after 179278 bytes')
        events=[]
        t=object.__new__(ResilientTransport)
        t.lock=threading.Lock()
        t.cooldown_until={}
        t.last_success=None
        t.backends={'httpx':BadResponseBackend()}
        t.order=['httpx']
        t.callback=events.append
        with self.assertRaises(TransportExhaustedError):
            t.request('https://web.archive.org/example',{},999999,threading.Event())
        self.assertTrue(any('response transfer or TLS handling' in e for e in events))
        self.assertFalse(any('during connection setup' in e for e in events))

    def test_failed_tcp_connection_is_reported_as_connection_setup(self):
        class BadConnectBackend:
            def request(self,*args,**kwargs):
                raise httpx.ConnectError('connection refused')
        events=[]
        t=object.__new__(ResilientTransport)
        t.lock=threading.Lock()
        t.cooldown_until={}
        t.last_success=None
        t.backends={'httpx':BadConnectBackend()}
        t.order=['httpx']
        t.callback=events.append
        with self.assertRaises(TransportExhaustedError):
            t.request('https://web.archive.org/example',{},999999,threading.Event())
        self.assertTrue(any('during connection setup' in e for e in events))

if __name__=='__main__':
    unittest.main()

class CheckpointCommitTests(unittest.TestCase):
    """A DB failure may never advance a resume cursor past uncommitted CDX rows."""

    def test_text_resume_key_survives_failed_upsert_without_advancing(self):
        import sqlite3
        from unittest.mock import patch
        from archive_scout.cdx.client import CDXRows, HttpClient
        from archive_scout.cdx.indexer import index_archive, decode_plan
        from archive_scout.config import NetworkConfig
        from archive_scout.database.connection import open_database
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            cfg=ProjectConfig(output_dir=root,targets=['example.com/*'],keywords=['needle'],
                              from_date='20010101000000',to_date='20010101235959',cdx_delay=0,
                              network=NetworkConfig(index_strategy='resume')).normalized()
            db=open_database(root)
            response=CDXRows([('20010101010203','http://example.com/a.html',
                               'text/html','200','ABC','12')], 'next-page-cursor')
            try:
                with patch.object(HttpClient,'get_cdx_any', return_value=response), \
                     patch('archive_scout.cdx.indexer.upsert_captures',
                           side_effect=sqlite3.OperationalError('simulated disk write failure')):
                    with self.assertRaises(sqlite3.OperationalError):
                        index_archive(cfg,db,threading.Event())
                row=db.execute('SELECT plan_json,seen,complete FROM index_coverage').fetchone()
                self.assertIsNotNone(row)
                plan=decode_plan(row['plan_json'], [('20010101000000','20010101235959')])
                self.assertIsNone(plan.pending[0].resume_key)
                self.assertEqual(row['seen'],0)
                self.assertEqual(row['complete'],0)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],0)
            finally:
                db.close()

    def test_media_resume_key_survives_failed_upsert_without_advancing(self):
        import sqlite3
        from unittest.mock import patch
        from archive_scout.cdx.client import CDXRows, HttpClient
        from archive_scout.cdx.indexer import decode_plan
        from archive_scout.config import NetworkConfig, MediaConfig
        from archive_scout.database.connection import open_database
        from archive_scout.media.indexer import index_media
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            cfg=ProjectConfig(output_dir=root,targets=['example.com/*'],keywords=['needle'],
                              from_date='20010101000000',to_date='20010101235959',cdx_delay=0,
                              network=NetworkConfig(index_strategy='resume'),
                              media=MediaConfig(enabled=True,include_extensions=['jpg'],
                                                include_images=True,include_videos=False)).normalized()
            db=open_database(root)
            response=CDXRows([('20010101010203','http://example.com/a.jpg',
                               'image/jpeg','200','ABC','12')], 'next-media-cursor')
            try:
                with patch.object(HttpClient,'get_cdx_any',return_value=response), \
                     patch('archive_scout.media.indexer.upsert_media_captures',
                           side_effect=sqlite3.OperationalError('simulated disk write failure')):
                    with self.assertRaises(sqlite3.OperationalError):
                        index_media(cfg,db,threading.Event())
                row=db.execute('SELECT resume_key,seen,complete FROM media_index_state').fetchone()
                self.assertIsNotNone(row)
                plan=decode_plan(row['resume_key'], [('20010101000000','20010101235959')])
                self.assertIsNone(plan.pending[0].resume_key)
                self.assertEqual(row['seen'],0)
                self.assertEqual(row['complete'],0)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0],0)
            finally:
                db.close()

class ConcurrentMediaFilenameTests(unittest.TestCase):
    def test_two_different_urls_with_same_basename_both_saved(self):
        import time
        from unittest.mock import patch
        from archive_scout.config import MediaConfig
        from archive_scout.database.connection import open_database
        from archive_scout.database.repositories import get_or_create_media_target, upsert_media_captures
        from archive_scout.media.indexer import media_query_signature
        from archive_scout.media.downloader import download_media
        with tempfile.TemporaryDirectory() as tmp:
            cfg=ProjectConfig(output_dir=Path(tmp), targets=['example.com/*'], keywords=[],
                              workers=2,download_delay=0,
                              media=MediaConfig(enabled=True,include_extensions=['jpg'])).normalized()
            db=open_database(cfg.output_dir)
            media_target=get_or_create_media_target(db,'example.com/*')
            left=b'unique-content-for-first-file';right=b'unique-content-for-second-file'
            rows=[]
            for j,(path,content) in enumerate((('/gallery/photo.jpg',left),('/other/photo.jpg',right))):
                rows.append(({'original': 'http://example.com'+path,
                              'timestamp': f'2001010100000{j}', 'mimetype':'image/jpeg','statuscode':'200',
                              'digest':base64.b32encode(hashlib.sha1(content).digest()).decode('ascii').rstrip('='),
                              'length':str(len(content))},'image','.jpg'))
            upsert_media_captures(db,rows,media_target,media_query_signature(cfg))
            from archive_scout.media.indexer import media_index_state_signature
            db.execute("INSERT INTO media_index_state(target_id,extension,year,query_signature,complete,updated_at) VALUES(?,?,?,?,1,'now')",
                       (media_target,'__all__',0,media_index_state_signature(cfg)))
            db.commit()
            class MockHttpClient:
                def __init__(self,*args,**kwargs):pass
                def close(self):pass
                def download_to_path(self,url,destination,max_bytes):
                    from urllib.parse import unquote
                    content=left if '/gallery/photo.jpg' in unquote(url) else right
                    time.sleep(0.02)
                    destination.write_bytes(content)
                    return {'path':destination,'bytes':len(content),'content_hash':hashlib.sha256(content).hexdigest(),
                            'preview':content,'headers':{'content-type':'image/jpeg'},'status':200,'final_url':url}
            try:
                with patch('archive_scout.media.downloader.HttpClient',MockHttpClient):
                    download_media(cfg,db,threading.Event())
                saved=db.execute('SELECT path,state FROM media_captures ORDER BY id').fetchall()
                self.assertEqual([row['state'] for row in saved],['downloaded','downloaded'])
                paths=[Path(row['path']) for row in saved]
                self.assertNotEqual(paths[0],paths[1])
                self.assertEqual({p.read_bytes() for p in paths},{left,right})
                self.assertEqual({p.parent.name for p in paths},{'images'})
            finally:
                db.close()
