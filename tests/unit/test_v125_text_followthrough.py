"""End-to-end follow-through for v1.2.5 text imports, interpretation and redirects."""
from __future__ import annotations
import hashlib
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from archive_scout.content import decode_bytes, parse_text_representation
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_keyword_set, start_scan_run
from archive_scout.downloads.rate_limit import FixedRateLimiter
from archive_scout.network.transports import ResilientTransport
from archive_scout.projects.importers import import_text_folder
from archive_scout.scanning.full_text import search_documents
from archive_scout.scanning.hitlist import search_with_hitlist
from archive_scout.scanning.jobs import ScanJob
from archive_scout.scanning.rescanner import rescan_keyword_sets


class TextInterpretationFollowthrough(unittest.TestCase):
    def test_historical_import_suffixes_and_cp1252(self):
        suffixes = ('.htm', '.shtm', '.dhtm', '.xhtm', '.phtm', '.php', '.dat', '.cgi', '.txt')
        with tempfile.TemporaryDirectory() as temp:
            source, root = Path(temp)/'source', Path(temp)/'project'
            source.mkdir(); root.mkdir()
            for suffix in suffixes:
                payload = ('<html><body>café needle</body></html>' if suffix not in ('.dat','.txt')
                           else 'café needle literal <formatneedle> token').encode('cp1252')
                (source / ('fixture'+suffix)).write_bytes(payload)
            (source/'binary.dat').write_bytes(b'\x89PNG\r\n\x1a\n'+b'\x00'*100)
            db = open_database(root)
            try:
                self.assertEqual(import_text_folder(root,source,db,threading.Event()),9)
                saved = db.execute('SELECT body_text,path FROM documents').fetchall()
                self.assertEqual(len(saved),9)
                self.assertTrue(all('café needle' in row['body_text'] for row in saved))
                self.assertTrue(all(Path(row['path']).exists() for row in saved))
                self.assertEqual(len(search_documents(db,'café',field='body')),9)
                self.assertEqual(len(search_documents(db,'formatneedle',field='body')),2)
                self.assertEqual(search_with_hitlist(root,db,['café needle'],threading.Event())['matches'],9)
            finally:
                db.close()

    def test_plain_angle_bracket_token_is_not_html(self):
        raw = 'The literal <formatneedle> token is evidence.'
        self.assertEqual(parse_text_representation(raw,'http://example.org/file.txt','text/plain')[1],raw)
        self.assertIn('mixed together',parse_text_representation(
            '<html><body>mixed <b>together</b></body></html>',
            'http://example.org/wrong.dat','text/plain')[1])

    def test_rescan_reinterprets_unchanged_bytes_and_updates_fts(self):
        with tempfile.TemporaryDirectory() as temp:
            root, source = Path(temp)/'project', Path(temp)/'source'
            root.mkdir(); source.mkdir()
            original=b'The literal <formatneedle> token is evidence.'
            (source/'fixture.txt').write_bytes(original)
            db=open_database(root)
            try:
                import_text_folder(root,source,db,threading.Event())
                with db:
                    db.execute("UPDATE documents SET body_text='stale cached HTML body'")
                ks=get_or_create_keyword_set(db,'rescan',['formatneedle'])
                run_id=start_scan_run(db,ks,'rescan',1,'rescan',{})
                db.commit()
                rescan_keyword_sets(db,[ScanJob.create(run_id,'rescan',['formatneedle'])],
                                    threading.Event(),workers=1)
                row=db.execute('SELECT body_text FROM documents').fetchone()
                self.assertIn('<formatneedle>',row[0])
                self.assertEqual(len(search_documents(db,'formatneedle',field='body')),1)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM document_matches WHERE scan_run_id=?',(run_id,)).fetchone()[0],1)
                self.assertEqual(hashlib.sha256(Path(db.execute('SELECT path FROM documents').fetchone()[0]).read_bytes()).digest(),
                                 hashlib.sha256(original).digest())
            finally:
                db.close()


class RedirectPhysicalPacingTests(unittest.TestCase):
    def test_redirect_hops_all_backends(self):
        # Measure request-slot admissions rather than server arrival deltas.
        # The first HTTPX GET can spend time opening a TCP connection *after*
        # obtaining its slot; redirects often reuse that connection. Server
        # arrival spacing is therefore not the limiter's scheduling invariant.
        class RecordingPacer(FixedRateLimiter):
            def __init__(self, delay):
                super().__init__(delay)
                self.admitted_at = []

            def wait(self, stop_event):
                with self.slot(stop_event):
                    # The slot reservation timestamp is recorded by the pacer
                    # under its condition lock, independent of thread wake-up
                    # or per-backend socket-connection overhead.
                    self.admitted_at.append(self.next_request - self.delay)

        arrivals=[]
        class Handler(BaseHTTPRequestHandler):
            protocol_version='HTTP/1.1'
            def do_GET(self):
                arrivals.append((self.path,time.monotonic()))
                if self.path != '/last':
                    target='/middle' if self.path=='/first' else '/last'
                    self.send_response(302); self.send_header('Location',target)
                    self.send_header('Content-Length','0'); self.end_headers()
                else:
                    body=b'consistent'; self.send_response(200)
                    self.send_header('Content-Length',str(len(body)))
                    self.end_headers(); self.wfile.write(body)
            def log_message(self,*args):pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        runner=threading.Thread(target=server.serve_forever,daemon=True);runner.start()
        try:
            for mode in ('httpx','urllib3','curl'):
                try:
                    transport=ResilientTransport(mode=mode,pool_size=2,connect_timeout=2,
                                                 read_timeout=2,trust_env=False)
                except Exception:
                    if mode=='curl': continue
                    raise
                try:
                    arrivals.clear()
                    pacer=RecordingPacer(0.018)
                    transport.physical_pacer=pacer
                    with tempfile.TemporaryDirectory() as temp:
                        response=transport.download(
                            f'http://127.0.0.1:{server.server_port}/first',
                            {},Path(temp)/'file',1024,threading.Event(),
                            redirect_validator=lambda a,b: None)
                        self.assertEqual(response.status,200)
                        self.assertEqual(response.path.read_bytes(),b'consistent')
                    self.assertEqual([p for p,_ in arrivals],['/first','/middle','/last'],mode)
                    # One physical slot is required for the initial GET and
                    # one for *each* redirect, on every available backend.
                    self.assertEqual(len(pacer.admitted_at), len(arrivals), mode)
                    for a,b in zip(pacer.admitted_at,pacer.admitted_at[1:]):
                        self.assertGreaterEqual(b-a,0.012,mode)
                    for (_,arrived),admitted in zip(arrivals,pacer.admitted_at):
                        self.assertGreaterEqual(arrived,admitted,mode)
                finally:transport.close()
        finally:
            server.shutdown();server.server_close();runner.join(timeout=2)

    def test_curl_reads_rate_limit_headers_without_body_completion(self):
        try:
            transport=ResilientTransport(mode='curl',pool_size=2,connect_timeout=2,
                                         read_timeout=6,trust_env=False)
        except Exception:self.skipTest('curl not installed')
        class Handler(BaseHTTPRequestHandler):
            protocol_version='HTTP/1.1'
            def do_GET(self):
                self.send_response(503);self.send_header('Retry-After','2')
                self.send_header('Content-Length','1000000');self.end_headers()
                time.sleep(2)
            def log_message(self,*args):pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        runner=threading.Thread(target=server.serve_forever,daemon=True);runner.start()
        try:
            with tempfile.TemporaryDirectory() as temp:
                begun=time.monotonic()
                response=transport.download(f'http://127.0.0.1:{server.server_port}/slow',
                    {},Path(temp)/'body',1024*1024,threading.Event())
                self.assertEqual(response.status,503)
                self.assertEqual(response.headers.get('retry-after'),'2')
                self.assertLess(time.monotonic()-begun,1.5)
                self.assertFalse(response.path.exists())
        finally:
            transport.close();server.shutdown();server.server_close();runner.join(timeout=2)

class ExtendedRepresentationFollowthrough(unittest.TestCase):
    def test_media_descriptors_require_a_recognized_root(self):
        from archive_scout.media.downloader import validate_media_representation
        def specimen(data, path):
            return dict(status=200, bytes=len(data), preview=data,
                        headers={'content-type':'text/html', 'content-length':str(len(data))})
        valid = [(b'<?xml version="1.0"?><ASX><ENTRY></ENTRY></ASX>','http://example.org/m.asx'),
                 (b'<?xml version="1.0"?><smil><body/></smil>','http://example.org/list.wpl'),
                 (b'#EXTM3U\nhttp://example.org/clip.mp4','http://example.org/music.m3u')]
        for data,path in valid:
            with self.subTest(path=path):
                validate_media_representation(specimen(data,path),path_hint=path)
        bad=b'<?xml version="1.0"?><Error><Message>not found</Message></Error>'
        with self.assertRaises(RuntimeError):
            validate_media_representation(specimen(bad,'http://example.org/a.asx'),path_hint='http://example.org/a.asx')

    def test_redirect_gate_revocation_prevents_second_hop(self):
        from archive_scout.network.transports import PermitRevoked
        requests=[]
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                requests.append(self.path)
                self.send_response(302 if self.path=='/a' else 200)
                if self.path=='/a':self.send_header('Location','/b')
                self.send_header('Content-Length','0');self.end_headers()
            def log_message(self,*args):pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        runner=threading.Thread(target=server.serve_forever,daemon=True);runner.start()
        try:
            client=ResilientTransport(mode='httpx',pool_size=2,connect_timeout=2,read_timeout=2,trust_env=False)
            try:
                gate_calls=[0]
                def permit():
                    gate_calls[0]+=1
                    return gate_calls[0]==1
                with self.assertRaises(PermitRevoked):
                    client.request(f'http://127.0.0.1:{server.server_port}/a',{},1024,
                                   threading.Event(),permit_checker=permit)
                self.assertEqual(requests,['/a'])
            finally:client.close()
        finally:server.shutdown();server.server_close();runner.join(timeout=2)
