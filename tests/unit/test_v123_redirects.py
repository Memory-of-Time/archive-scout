from __future__ import annotations

import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock, patch

from archive_scout.cdx.client import HttpClient
from archive_scout.config import ProjectConfig, load_project_config, save_project_config
from archive_scout.downloads.downloader import fetch_parse_scan
from archive_scout.downloads.rate_limit import FixedRateLimiter
from archive_scout.downloads.redirects import make_replay_redirect_validator
from archive_scout.network.transports import (
    BackendUnavailable, CurlBackend, HttpxBackend, RedirectPolicyError,
    ResilientTransport, Urllib3Backend,
)


class RedirectRestorationTests(unittest.TestCase):
    def test_policy_source_selected_domain_external_option_and_live_web(self):
        config = ProjectConfig(Path('.'), ['selected.org/*'], [], cdx_match_type='domain')
        source = 'https://web.archive.org/web/20010101000000id_/http://source.org/a'
        validate = make_replay_redirect_validator(config, 'http://source.org/a')
        for original in ('http://source.org/b', 'https://sub.selected.org/b'):
            validate(source, 'https://web.archive.org/web/20010102000000id_/'+original)
        external = 'https://web.archive.org/web/20010102000000id_/https://elsewhere.org/b'
        with self.assertRaises(RedirectPolicyError) as caught:
            validate(source, external)
        self.assertEqual(caught.exception.category, 'external_redirect_blocked')
        config.download_external_redirects = True
        validate = make_replay_redirect_validator(config, 'http://source.org/a')
        validate(source, external)
        for destination in ('https://elsewhere.org/live', 'https://web.archive.org.evil.example/a', 'file:///tmp/body'):
            with self.assertRaises(RedirectPolicyError) as caught:
                validate(source, destination)
            self.assertEqual(caught.exception.category, 'live_redirect_blocked')

    def test_all_transports_validate_each_hop_before_contacting_destination(self):
        hits = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_GET(self):
                hits.append(self.path)
                destinations = {'/start': '/middle', '/middle': '/blocked', '/allowed': '/saved'}
                if self.path in destinations:
                    self.send_response(302)
                    self.send_header('Location', destinations[self.path])
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                else:
                    self.send_response(200)
                    self.send_header('Content-Length', '8')
                    self.end_headers()
                    self.wfile.write(b'evidence')
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        base = f'http://127.0.0.1:{server.server_port}'
        try:
            factories = (lambda: HttpxBackend(2, 2, 2, trust_env=False),
                         lambda: Urllib3Backend(2, 2, 2), lambda: CurlBackend(2, 2))
            for factory in factories:
                try:
                    backend = factory()
                except BackendUnavailable:
                    continue  # curl is an optional OS backend, both Python backends always run.
                with self.subTest(backend=backend.name):
                    checked = []
                    def validate(source, destination):
                        checked.append(destination)
                        if destination.endswith('/blocked'):
                            raise RedirectPolicyError(source, destination, 'live_redirect_blocked')
                    hits.clear()
                    try:
                        with self.assertRaises(RedirectPolicyError):
                            backend.request(base+'/start', {}, 1000, threading.Event(), validate)
                        self.assertEqual(hits, ['/start', '/middle'])
                        self.assertEqual(checked, [base+'/middle', base+'/blocked'])
                        hits.clear()
                        response = backend.request(base+'/allowed', {}, 1000, threading.Event(), validate)
                        self.assertEqual(hits, ['/allowed', '/saved'])
                        self.assertEqual(response.data, b'evidence')
                        self.assertEqual(response.final_url, base+'/saved')
                    finally:
                        backend.close()
        finally:
            server.shutdown(); server.server_close(); thread.join(timeout=5)

    def test_rejected_redirect_does_not_rotate_backend_or_retry_or_cool_down(self):
        transport = ResilientTransport(pool_size=2, connect_timeout=2, read_timeout=2, trust_env=False)
        for backend in transport.backends.values(): backend.close()
        failure = RedirectPolicyError('https://web.archive.org/web/a', 'https://live.org/', 'live_redirect_blocked')
        first, second = Mock(), Mock()
        first.request.side_effect = failure
        transport.backends = {'one': first, 'two': second}; transport.order = ['one', 'two']
        client = HttpClient(FixedRateLimiter(0), 4, 2, 'test', threading.Event(), transport=transport)
        try:
            with patch.object(client, 'retry_wait') as wait, patch.object(client.host_gate, 'finish_request', wraps=client.host_gate.finish_request) as finish:
                with self.assertRaises(RedirectPolicyError):
                    client.get('https://web.archive.org/web/a', 1000, redirect_validator=lambda *args: None)
                wait.assert_not_called()
                self.assertTrue(finish.call_args.kwargs['recovered'])
            first.request.assert_called_once(); second.request.assert_not_called()
            self.assertEqual(transport.cooldown_until, {})
        finally:
            client.close()

    def test_text_downloader_passes_policy_and_setting_persists(self):
        with tempfile.TemporaryDirectory() as temp:
            config = ProjectConfig(Path(temp), ['source.org/*'], [], download_external_redirects=True)
            path = save_project_config(config)
            loaded = load_project_config(path)
            self.assertTrue(loaded.download_external_redirects)
            client = HttpClient(FixedRateLimiter(0), 1, 2, 'test', threading.Event())
            try:
                def response(url, budget, *, redirect_validator):
                    destination = 'https://web.archive.org/web/20010101000000id_/http://elsewhere.org/page'
                    redirect_validator(url, destination)
                    return dict(headers={'content-type': 'text/html'}, data=b'<html><body>evidence</body></html>',
                                status=200, final_url=destination)
                row = {'id': 1, 'timestamp': '20010101000000', 'original_url': 'http://source.org/a', 'mimetype': 'text/html'}
                with patch.object(client, 'get', side_effect=response):
                    result = fetch_parse_scan(row, loaded, [], client, False)
                self.assertIn('elsewhere.org', result['final_url'])
                self.assertEqual(result['path'].read_bytes(), b'<html><body>evidence</body></html>')
            finally:
                client.close()
