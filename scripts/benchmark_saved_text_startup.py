"""Local retry startup with a large synthetic saved history, not an integrity audit.

Historical document rows deliberately have no physical bodies. Only the selected
new captures are transferred, hashed and scanned. Run roots in separate processes.
"""
import argparse
import hashlib
import json
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch


def run(args):
    sys.path.insert(0, str(args.source_root.resolve()))
    from archive_scout.config import ProjectConfig, NetworkConfig
    from archive_scout.cdx.parameters import cdx_query_signature
    from archive_scout.cdx.indexer import save_coverage_state
    from archive_scout.database.connection import open_database
    from archive_scout.database.repositories import get_or_create_target, get_or_create_keyword_set, start_scan_run
    from archive_scout.downloads.downloader import download_archive
    from archive_scout.downloads.diagnostics import _process_memory
    from archive_scout.scanning.jobs import ScanJob
    body = b'<html><body>needle archived text</body></html>'
    requests = []
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def do_GET(self):
            requests.append(time.perf_counter())
            self.send_response(200)
            self.send_header('Content-Type', 'text/html')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *unused):
            pass
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            cfg = ProjectConfig(root, ['example.org/*'], ['needle'], from_date='2008', to_date='2008',
                cdx_collapses=[], download_delay=.125, workers=9,
                network=NetworkConfig(backend='httpx', trust_environment=False)).normalized()
            db = open_database(root)
            try:
                tid = get_or_create_target(db, cfg.targets[0]); sig = cdx_query_signature(cfg)
                with db:
                    db.execute("""WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?)
                        INSERT INTO captures(original_url,timestamp,target_id,query_signature,mimetype,statuscode,length,state,created_at,updated_at)
                        SELECT 'http://example.org/'||i,'20080101000000',?,?,'text/html','200',100,
                        CASE WHEN i<=? THEN 'downloaded' ELSE 'error' END,'now','now' FROM n""",
                        (args.history + args.count, tid, sig, args.history))
                    db.execute("""INSERT INTO documents(capture_id,path,title,body_text,links_json,content_hash,normalized_hash,size_bytes,created_at,updated_at)
                        SELECT id,'synthetic-not-read','','','[]','','',100,'now','now' FROM captures WHERE state='downloaded'""")
                    db.execute('UPDATE captures SET document_id=id WHERE id<=?', (args.history,))
                    save_coverage_state(db, tid, sig, cfg.from_date, cfg.to_date, None, True, args.history + args.count, None)
                    kid = get_or_create_keyword_set(db, 'Test', ['needle'])
                    run_id = start_scan_run(db, kid, 'startup', 1, 'all', {})
                ids = list(range(args.history + 1, args.history + args.count + 1))
                samples = []; resources = []
                started = time.perf_counter(); cpu = time.process_time()
                def progress(event):
                    if event.stage == 'download' and event.detail and event.detail.get('new_saves'):
                        samples.append((event.detail['new_saves'], time.perf_counter() - started))
                        if len(resources) < 3 and event.detail['new_saves'] >= (len(resources) + 1) * args.count // 3:
                            resources.append({'saved': event.detail['new_saves'], 'cpu_seconds': time.process_time() - cpu,
                                              'threads': threading.active_count(), **_process_memory()})
                with patch('archive_scout.downloads.downloader.replay_url',
                           return_value=f'http://127.0.0.1:{server.server_port}/body'):
                    download_archive(cfg, db, run_id, threading.Event(), progress, states=('error',),
                        capture_ids=ids, scan_jobs=[ScanJob.create(run_id, 'Test', ['needle'])])
                elapsed = time.perf_counter() - started
                rows = db.execute('SELECT d.path,d.content_hash FROM documents d WHERE capture_id>?', (args.history,)).fetchall()
                assert len(rows) == args.count == len(requests)
                assert all(hashlib.sha256(Path(r['path']).read_bytes()).hexdigest() == r['content_hash'] == hashlib.sha256(body).hexdigest() for r in rows)
                assert db.execute('SELECT COUNT(*) FROM document_matches WHERE scan_run_id=?', (run_id,)).fetchone()[0] == args.count
                return {'source_root': str(args.source_root.resolve()), 'synthetic_saved_history': args.history,
                        'validated_new_files': args.count, 'new_scores': args.count,
                        'first_request_seconds': requests[0] - started, 'first_save_seconds': samples[0][1],
                        'last_save_seconds': samples[-1][1], 'elapsed_seconds': elapsed,
                        'cpu_seconds': time.process_time() - cpu, 'resource_samples': resources,
                        'scope': 'Local HTTP plus synthetic DB history; no historical-body integrity or live-service claim'}
            finally:
                db.close()
    finally:
        server.shutdown(); server.server_close(); thread.join(timeout=3)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--history', type=int, default=1_000_000)
    parser.add_argument('--count', type=int, default=24)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = run(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
