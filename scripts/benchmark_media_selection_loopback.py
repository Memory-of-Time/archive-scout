"""Local HTTP timestamp-proof and media-publication fixture; no live-service claim."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

try:
    import resource
except ImportError:
    resource = None


def run(args):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from archive_scout.config import ProjectConfig, MediaConfig, NetworkConfig
    from archive_scout.database.connection import open_database
    from archive_scout.database.repositories import get_or_create_media_target, upsert_media_captures
    from archive_scout.media.downloader import download_media
    from archive_scout.media.indexer import media_query_signature, media_index_state_signature, _save_media_state
    from archive_scout.downloads.rate_limit import reset_shared_traffic_state_for_tests
    from archive_scout.coverage import media_coverage_gaps
    body = b'\xff\xd8\xff' + b'x' * (65536 - 3)
    digest = base64.b32encode(hashlib.sha1(body).digest()).decode().rstrip('=')
    sha256 = hashlib.sha256(body).hexdigest()
    requests = {'exact': 0, 'replay': 0}
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def do_GET(self):
            if self.path.startswith('/cdx?'):
                requests['exact'] += 1
                q = parse_qs(urlsplit(self.path).query)
                assert q['matchType'] == ['exact'] and q['limit'] == ['1']
                data = json.dumps([['urlkey','timestamp','original','mimetype','statuscode','digest','length'],
                    ['org,example)/', '20080101000000', q['url'][0], 'image/jpeg', '200', digest, str(len(body))]]).encode()
                mime = 'application/json'
                time.sleep(.05)
            else:
                requests['replay'] += 1
                data, mime = body, 'image/jpeg'
                time.sleep(.4)
            self.send_response(200)
            self.send_header('Content-Type', mime)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory(prefix='scout-direct-selection-') as tmp:
            cfg = ProjectConfig(Path(tmp), targets=['example.org/*'], keywords=[], from_date='2008', to_date='2008',
                workers=9, download_delay=.125, cdx_delay=.125, cdx_collapses=[],
                media=MediaConfig(enabled=True, discover_embedded=False, snapshot_strategy='earliest'),
                network=NetworkConfig(backend='httpx', trust_environment=False, cdx_workers=10)).normalized()
            db = open_database(cfg.output_dir)
            target = get_or_create_media_target(db, cfg.targets[0])
            signature = media_query_signature(cfg)
            seeded = time.perf_counter()
            with db:
                upsert_media_captures(db, ((('20080102000000', f'http://example.org/history/{i}.jpg',
                    'image/jpeg', '200', digest, str(len(body))), 'image', '.jpg')
                    for i in range(args.historical_captures)), target, 'inactive-history')
                upsert_media_captures(db, ((('20080102000000', f'http://example.org/active/{i}.jpg',
                    'image/jpeg', '200', digest, str(len(body))), 'image', '.jpg')
                    for i in range(args.count)), target, signature)
                _save_media_state(db, target, 2008, media_index_state_signature(cfg), None, False, args.count, None)
            seed_seconds = time.perf_counter() - seeded
            samples, resources = [], []
            started, cpu = time.perf_counter(), time.process_time()
            def progress(event):
                if event.stage == 'media_download' and event.current:
                    n = len(samples) + 1
                    samples.append((n, time.perf_counter() - started))
                    if len(resources) < 3 and n >= (len(resources)+1)*args.count//3:
                        resources.append({'saved': n, 'cpu_seconds': time.process_time()-cpu,
                            'peak_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024) if resource else None,
                            'threads': threading.active_count()})
            reset_shared_traffic_state_for_tests()
            endpoint = f'http://127.0.0.1:{server.server_port}'
            try:
                with patch('archive_scout.media.selection.cdx_endpoints', return_value=(endpoint+'/cdx',)), \
                     patch('archive_scout.media.downloader.media_replay_url', return_value=endpoint+'/replay'):
                    download_media(cfg, db, threading.Event(), progress, allow_partial_inventory=True)
                elapsed = time.perf_counter() - started
                count = 0
                for row in db.execute("SELECT timestamp,path,content_hash,bytes_saved FROM media_captures WHERE query_signature=? AND state='downloaded'", (signature,)):
                    assert row['timestamp'] == '20080101000000'
                    assert row['content_hash'] == sha256 and row['bytes_saved'] == len(body)
                    assert hashlib.sha256(Path(row['path']).read_bytes()).hexdigest() == sha256
                    count += 1
                assert count == args.count and len(samples) == args.count
                assert requests == {'exact': args.count, 'replay': args.count}
                assert media_coverage_gaps(db, cfg)
                assert db.execute("SELECT COUNT(*) FROM media_captures WHERE query_signature='inactive-history' AND state='pending'").fetchone()[0] == args.historical_captures
                segments = []
                for label, low, high in [('early',0,args.count//3),('middle',args.count//3,2*args.count//3),('late',2*args.count//3,args.count)]:
                    a = next((t for n,t in samples if n>=low), 0) if low else 0
                    b = next(t for n,t in samples if n>=high)
                    segments.append({'phase':label,'successful_saves_per_second':(high-low)/(b-a)})
                return {'validated_files':count, 'sha256_verified':True,'historical_captures':args.historical_captures,
                    'historical_states_preserved':True, 'broad_coverage_remains_incomplete':True,
                    'seed_seconds':seed_seconds, 'elapsed_seconds':elapsed,'cpu_seconds':time.process_time()-cpu,
                    'successful_saves_per_second':count/elapsed,'requests':requests,
                    'workers':cfg.workers,'cdx_workers':cfg.network.cdx_workers,'replay_delay':.125,'cdx_delay':.125,
                    'segments':segments,'resources':resources,
                    'scope':'Local HTTP selection and real validated publication; fixture server included in resources; not live Wayback or a days-long run.'}
            finally:
                db.close()
    finally:
        server.shutdown(); server.server_close(); thread.join(3)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--count', type=int, default=120)
    parser.add_argument('--historical-captures', type=int, default=0)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.count < 3 or args.historical_captures < 0:
        parser.error('invalid fixture size')
    result = run(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
