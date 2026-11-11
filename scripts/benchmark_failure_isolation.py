from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
import tempfile
import threading
import time
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from archive_scout.cdx.client import HttpClient
from archive_scout.cdx.indexer import save_coverage_state
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_captures
from archive_scout.downloads.downloader import download_archive
from archive_scout.downloads.rate_limit import reset_shared_traffic_state_for_tests
from archive_scout.downloads.diagnostics import _process_memory


def run(count):
    body = b'<html><body>needle '+b'x'*65536+b'</body></html>'
    digest = base64.b32encode(hashlib.sha1(body).digest()).decode()
    corrupt = b'<img src="http://truncated.example'
    calls = {}
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def do_GET(self):
            number = int(self.path.rsplit('/', 1)[1])
            with lock:
                calls[number] = calls.get(number, 0)+1
                first = calls[number] == 1
            status = 429 if number == 1 and first else 200
            data = corrupt if number % 20 == 0 else body
            self.send_response(status)
            self.send_header('Content-Length', str(len(data)))
            self.send_header('Content-Type', 'text/html')
            if number % 20 == 0:
                self.send_header('Content-Encoding', 'gzip')
            if status == 429:
                self.send_header('Retry-After', '1')
            self.end_headers()
            try:
                self.wfile.write(data)
            except OSError:
                pass

        def log_message(self, *_):
            pass

    class QuietServer(ThreadingHTTPServer):
        def handle_error(self, *_):
            pass

    server = QuietServer(('127.0.0.1', 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        with tempfile.TemporaryDirectory() as folder, closing(open_database(Path(folder))) as db:
            root = Path(folder)
            cfg = ProjectConfig(root, ['example.test/*'], ['needle'], download_delay=.125,
                from_date='2001', to_date='2001',
                network=NetworkConfig(backend='httpx', trust_environment=False, retry_base_seconds=1,
                                      retry_max_seconds=1)).normalized()
            target = get_or_create_target(db, cfg.targets[0])
            sig = cdx_query_signature(cfg)
            with db:
                upsert_captures(db, (('20010101000000', f'http://example.test/{n}', 'text/html',
                    '200', digest, len(body)) for n in range(count)), target, sig)
                save_coverage_state(db, target, sig, cfg.from_date, cfg.to_date, None, True, count, None)
            started = time.monotonic()
            samples = []
            resources = []

            def progress(event):
                if event.stage == 'download' and event.detail:
                    saved = event.detail.get('new_saves', 0)
                    samples.append((saved, time.monotonic()-started))
                    if len(resources) < 3 and saved >= (len(resources)+1)*count*.95/3:
                        resources.append({'saved':saved, 'elapsed':time.monotonic()-started,
                                          'threads':threading.active_count(), **_process_memory()})

            reset_shared_traffic_state_for_tests()
            with patch('archive_scout.downloads.downloader.replay_url',
                       side_effect=lambda stamp, url: f'http://127.0.0.1:{server.server_port}/'+url.rsplit('/',1)[1]):
                download_archive(cfg, db, 0, threading.Event(), progress, acquire_only=True)
            elapsed = time.monotonic()-started
            rows = db.execute("SELECT id,state FROM captures").fetchall()
            saved = sum(row['state'] == 'downloaded_unscanned' for row in rows)
            bad = len([n for n in range(count) if n % 20 == 0])
            assert saved == count-bad, (saved, count-bad)
            assert db.execute("SELECT COUNT(*) FROM captures WHERE state='error'").fetchone()[0] == bad
            assert db.execute('SELECT COUNT(*) FROM text_retry_schedule').fetchone()[0] == bad
            paths = list((root/'captures').rglob('*.txt'))
            assert len(paths) == saved
            assert all(hashlib.sha256(path.read_bytes()).digest() == hashlib.sha256(body).digest() for path in paths)
            phases = []
            for name, lo, hi in [('early',0,saved//3), ('middle',saved//3,2*saved//3), ('late',2*saved//3,saved)]:
                a = next((t for n,t in samples if n>=lo), 0) if lo else 0
                b = next(t for n,t in samples if n>=hi)
                phases.append({'phase':name, 'validated_saves_per_second':(hi-lo)/(b-a)})
            return {'scope':'localhost mixed capture failures; no live Wayback claim', 'captures':count,
                'validated_saves':saved, 'malformed_captures_kept_retryable':bad, 'elapsed_seconds':elapsed,
                'segment_rates':phases, 'resources':resources, 'sha256_all_saved_files_verified':True,
                'control_capture_attempts':calls.get(1), 'physical_requests':sum(calls.values())}
    finally:
        server.shutdown()
        server.server_close()
        reset_shared_traffic_state_for_tests()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', type=int, default=320)
    parser.add_argument('--output', type=Path, default=Path('validation/failure-isolation.json'))
    args = parser.parse_args()
    if args.count < 20:
        parser.error('--count must be at least 20')
    result = run(args.count)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))
