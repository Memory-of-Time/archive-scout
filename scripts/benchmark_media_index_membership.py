"""Localhost index membership and early/middle/late resource benchmark."""
from __future__ import annotations

import argparse
import json
import re
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--rows', type=int, default=1_000_000)
    parser.add_argument('--page-size', type=int, default=25000)
    args = parser.parse_args()
    sys.path.insert(0, str(args.source.resolve()))
    from archive_scout.downloads.diagnostics import _process_memory
    from archive_scout.database.connection import DATABASE_NAME
    from archive_scout.config import ProjectConfig, NetworkConfig, MediaConfig
    from archive_scout.database.connection import open_database
    from archive_scout.media.indexer import index_media
    requests = []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_GET(self):
            params = parse_qs(urlsplit(self.path).query)
            if 'showNumPages' in params:
                requests.append(('count', 0))
                data = str((args.rows + args.page_size - 1) // args.page_size).encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            offset = int(params['page'][0]) * args.page_size if 'page' in params else int(params.get('resumeKey', ['0'])[0])
            limit = int(params.get('limit', [str(args.page_size)])[0])
            end = min(args.rows, offset + limit)
            requests.append((offset, end))
            fields = params['fl'][0].split(',')
            def item(i):
                return dict(urlkey=f'org,example)/{i:08d}', timestamp='20080101000000',
                    original=f'http://example.org/{i:08d}.jpg', mimetype='image/jpeg', statuscode='200', digest=str(i), length='12')
            cursor = str(end) if end < args.rows and 'page' not in params else None
            if params['output'][0] == 'json':
                payload = [fields] + [[item(i)[field] for field in fields] for i in range(offset, end)]
                if cursor: payload += [[], [cursor]]
                data = json.dumps(payload).encode()
            else:
                data = ''.join(' '.join(item(i)[field] for field in fields) + '\n' for i in range(offset, end)).encode()
                if cursor: data += ('\n' + cursor + '\n').encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    samples = []
    with tempfile.TemporaryDirectory() as temp:
        cfg = ProjectConfig(Path(temp), targets=['example.org/*'], keywords=[], from_date='2008', to_date='2008',
            page_size=args.page_size, cdx_collapses=[], cdx_delay=0,
            network=NetworkConfig(backend='httpx', index_strategy='resume', trust_environment=False),
            media=MediaConfig(enabled=True, discover_embedded=False, snapshot_strategy='all', include_images=True, include_videos=False, include_extensions=['jpg'])).normalized()
        db = open_database(cfg.output_dir)
        started = time.monotonic()
        cpu = time.process_time()
        def progress(event):
            match = re.search(r': received ([0-9,]+), accepted ', event.message)
            if match:
                samples.append(dict(seconds=time.monotonic()-started, cpu_seconds=time.process_time()-cpu,
                    received=int(match[1].replace(',', '')),
                    peak_rss_mb=_process_memory().get('peak_rss_bytes', 0)/1024**2))
        endpoint = f'http://127.0.0.1:{server.server_port}/cdx'
        with patch('archive_scout.media.indexer.cdx_endpoints', return_value=(endpoint,)), \
             patch('archive_scout.media.indexer.cdx_paged_endpoints', return_value=(endpoint,)):
            index_media(cfg, db, threading.Event(), progress)
            deferred = bool(db.execute("SELECT COUNT(*) FROM media_index_state WHERE complete=0").fetchone()[0])
        elapsed = time.monotonic()-started
        count, distinct, first, last = db.execute('SELECT COUNT(*),COUNT(DISTINCT original_url),MIN(original_url),MAX(original_url) FROM media_captures').fetchone()
        assert not deferred and count == distinct == args.rows, (deferred, count, distinct)
        mismatches = db.execute("SELECT COUNT(*) FROM media_captures WHERE original_url != 'http://example.org/' || printf('%08d',CAST(digest AS INTEGER)) || '.jpg'").fetchone()[0]
        assert mismatches == 0
        stages = []
        for n in range(3):
            group = samples[n*len(samples)//3:(n+1)*len(samples)//3]
            start = samples[n*len(samples)//3-1]['seconds'] if n else 0
            stages.append(dict(label=('early','middle','late')[n], rows=sum(r['received'] for r in group),
                rows_per_second=sum(r['received'] for r in group)/(group[-1]['seconds']-start),
                peak_rss_mb=group[-1]['peak_rss_mb']))
        result = dict(source=str(args.source), rows=count, requests=len(requests), elapsed_seconds=elapsed,
            cpu_seconds=time.process_time()-cpu, rows_per_second=count/elapsed,
            peak_rss_mb=_process_memory().get('peak_rss_bytes', 0)/1024**2,
            db_mb=(cfg.output_dir/DATABASE_NAME).stat().st_size/1e6,
            stages=stages, exact_membership_verified=True,
            limitation='Localhost synthetic media indexing, not live Wayback or download-save throughput; stage samples cover resume traversal only')
        db.close()
    server.shutdown()
    server.server_close()
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
