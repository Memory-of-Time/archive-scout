from __future__ import annotations

import shutil
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock

import httpx

from archive_scout.cdx.client import HttpClient
from archive_scout.config import ProjectConfig
from archive_scout.downloads.downloader import _download_capture, _scan_saved_capture
from archive_scout.downloads.rate_limit import FixedRateLimiter
from archive_scout.events import Stopped
from archive_scout.network.transports import ResilientTransport, PayloadValidationError, ServiceStatusResponse
from archive_scout.scanning.jobs import ScanJob


class LocalNetworkTests(unittest.TestCase):
    def setUp(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.body = b'<html><body>needle archive healthy words</body></html>' * 400
        owner = self
        class Handler(BaseHTTPRequestHandler):
            protocol_version='HTTP/1.1'
            def log_message(self, *args):
                pass
            def do_GET(self):
                if self.path == '/headers':
                    owner.entered.set(); owner.release.wait(10)
                raw = owner.body
                self.send_response(200)
                self.send_header('Content-Length',str(len(raw)))
                self.send_header('Content-Type','text/html; charset=' + ('utf-16' if self.path == '/charset' else 'utf-8'))
                self.send_header('Memento-Datetime','Sat, 01 Jan 2005 00:00:00 GMT')
                self.end_headers()
                try:
                    if self.path == '/body':
                        self.wfile.write(raw[:1024]); self.wfile.flush()
                        owner.entered.set(); owner.release.wait(10)
                        self.wfile.write(raw[1024:])
                    else:
                        self.wfile.write(raw)
                except (BrokenPipeError,ConnectionResetError,OSError):
                    pass
        self.server = ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.server.daemon_threads=True
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.url=f'http://127.0.0.1:{self.server.server_port}'
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)

    def tearDown(self):
        self.release.set();self.server.shutdown();self.server.server_close();self.thread.join(2);self.temp.cleanup()

    def backends(self):
        return ['httpx','urllib3'] + (['curl'] if shutil.which('curl') else [])

    def transport(self, backend):
        return ResilientTransport(pool_size=2,connect_timeout=2,read_timeout=30,mode=backend,trust_env=False)

    def test_bad_charset_does_not_cool_down_healthy_backend_or_lose_score(self):
        row={'id':1,'timestamp':'20050101000000','original_url':'http://example.com/a','mimetype':'text/html'}
        config=ProjectConfig(self.root,['example.com/*'],['needle']).normalized()
        job=ScanJob.create(1,'Rules',['needle'])
        for backend in self.backends():
            with self.subTest(backend=backend):
                stop=threading.Event();transport=self.transport(backend)
                client=HttpClient(FixedRateLimiter(0),1,30,'Offline regression',stop,transport=transport)
                path=self.root/(backend+'.txt')
                try:
                    with mock.patch('archive_scout.downloads.downloader.replay_url',return_value=self.url+'/charset'):
                        result=_download_capture(row,path,config,client)
                    self.assertEqual(result['encoding'],'utf-8')
                    scanned=_scan_saved_capture({**row,'detected_encoding':result['encoding']},path,config,[job])
                    self.assertGreater(scanned['analyses'][1]['score'],0)
                    self.assertEqual(client.get(self.url+'/healthy',100000)['data'],self.body)
                    self.assertFalse(transport._health_locked(self.url).cooldown_until)
                finally:
                    client.close()

    def test_validator_failure_keeps_backend_and_recovery_probe_healthy(self):
        for backend in self.backends():
            with self.subTest(backend=backend):
                transport=self.transport(backend);stop=threading.Event()
                client=HttpClient(FixedRateLimiter(0),1,30,'Offline regression',stop,transport=transport)
                def fail(*args):
                    raise ValueError('injected payload classification failure')
                try:
                    with self.assertRaises(PayloadValidationError):
                        client.download_to_path(self.url+'/healthy',self.root/(backend+'.part'),100000,preview_validator=fail)
                    self.assertFalse(transport._health_locked(self.url).cooldown_until)
                    self.assertEqual(client.get(self.url+'/healthy',100000)['data'],self.body)
                    metrics=client.metrics_snapshot()
                    self.assertEqual(metrics['validation_failures'],1)
                    self.assertEqual(metrics.get('transport_failures',0),0)
                finally:
                    client.close()

    def check_cancellation(self, endpoint, download=False):
        for backend in self.backends():
            with self.subTest(backend=backend,endpoint=endpoint):
                self.entered.clear();self.release.clear()
                transport=self.transport(backend);stop=threading.Event();errors=[]
                destination=self.root/(backend+'-cancel.part')
                # Establish and reuse a pooled connection before the stall.
                self.assertEqual(bytes(transport.request(self.url+'/healthy',{},100000,stop).data),self.body)
                def run():
                    try:
                        if download:
                            transport.download(self.url+endpoint,{},destination,100000,stop)
                        else:
                            transport.request(self.url+endpoint,{},100000,stop)
                    except Exception as exc:
                        errors.append(type(exc))
                worker=threading.Thread(target=run);worker.start()
                self.assertTrue(self.entered.wait(4))
                if download:
                    time.sleep(.05)
                before=time.monotonic();stop.set();worker.join(2)
                self.release.set()
                try:
                    self.assertFalse(worker.is_alive(),'Cancelled request still owns its socket/file')
                    self.assertLess(time.monotonic()-before,2)
                    self.assertEqual(errors,[Stopped])
                    if download:
                        self.assertEqual(destination.read_bytes(),self.body[:1024])
                    self.assertFalse(transport._health_locked(self.url).cooldown_until)
                    self.assertEqual(bytes(transport.request(self.url+'/healthy',{},100000,threading.Event()).data),self.body)
                finally:
                    transport.close()

    def test_header_stall_is_interruptible_on_reused_connection(self):
        self.check_cancellation('/headers')

    def test_body_stall_is_interruptible_and_keeps_exact_partial(self):
        self.check_cancellation('/body',download=True)

    def test_service_response_is_not_counted_as_transport_failure(self):
        transport=self.transport('httpx')
        client=HttpClient(FixedRateLimiter(0),1,30,'Offline regression',threading.Event(),transport=transport)
        try:
            with self.assertRaises(ServiceStatusResponse):
                with client._wire_attempt(self.url):
                    raise ServiceStatusResponse(429,{'retry-after':'1'},self.url,'httpx')
            metrics=client.metrics_snapshot()
            self.assertEqual(metrics['service_response_failures'],1)
            self.assertEqual(metrics.get('transport_failures',0),0)
        finally:
            client.close()

    def test_repeated_connection_failure_renews_only_after_all_owners_drain(self):
        transport=self.transport('httpx')
        old=transport.backends['httpx'];replacement=mock.Mock()
        transport._factories['httpx']=lambda:replacement
        with mock.patch.object(old,'close') as closed:
            self.assertTrue(transport._claim_backend(self.url,'httpx'))
            self.assertTrue(transport._claim_backend(self.url,'httpx'))
            transport._backend_failed(self.url,'httpx',httpx.ConnectError('lost connection'))
            transport._backend_failed(self.url,'httpx',httpx.ConnectError('lost connection'))
            transport._release_backend(self.url,'httpx')
            closed.assert_not_called()
            self.assertIs(transport.backends['httpx'],old)
            transport._release_backend(self.url,'httpx')
            closed.assert_called_once()
            self.assertIs(transport.backends['httpx'],replacement)
            self.assertEqual(transport.metrics_snapshot()['pool_renewals'],1)
            self.assertGreater(transport._health_locked(self.url).cooldown_until['httpx'],time.monotonic())
        old.close();transport.close()
