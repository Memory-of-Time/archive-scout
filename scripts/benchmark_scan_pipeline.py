"""Offline synthetic scanner benchmark; pass the repository root as the first argument."""
import argparse, hashlib, json, statistics, sys, tempfile, threading, time
from pathlib import Path
p = argparse.ArgumentParser()
p.add_argument('repository')
p.add_argument('--count', type=int, default=200)
p.add_argument('--repeat', type=int, default=3)
p.add_argument('--output')
a = p.parse_args()
sys.path.insert(0, str(Path(a.repository).resolve()))
from archive_scout.config import ProjectConfig
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_capture, get_or_create_keyword_set, start_scan_run
from archive_scout.downloads.downloader import _scan_pending_captures
from archive_scout.scanning.jobs import ScanJob
from archive_scout.scanning.rescanner import rescan_keyword_sets
terms = ['needle', 'archive', 'mirror', 'episode', 'rare footage', 'exclude: irrelevant ad']
summary = []
for workers in (1, 4):
    durations = []
    rescans = []
    hashes = []
    for repeat in range(a.repeat):
        with tempfile.TemporaryDirectory(prefix='scanner-pipeline-') as temp:
            root = Path(temp)
            cfg = ProjectConfig(root, ['example.com/*'], terms, from_date='2005', to_date='2005', scan_workers=workers).normalized()
            db = open_database(root)
            try:
                sig = cdx_query_signature(cfg)
                with db:
                    target = get_or_create_target(db, 'example.com/*')
                    for i in range(a.count):
                        url = f'http://example.com/{i}'
                        raw = '<html><head><title>Archive</title></head><body>' + ''.join((f'<p>Record {j} for item {i}. needle archive mirror episode rare footage.</p>' for j in range(75))) + '</body></html>'
                        path = root / f'{i}.txt'
                        path.write_text(raw)
                        upsert_capture(db, {'original': url, 'timestamp': '20050101000000', 'mimetype': 'text/html', 'statuscode': '200', 'digest': '', 'length': str(path.stat().st_size)}, target, sig)
                        db.execute("UPDATE captures SET state='downloaded_unscanned',local_path=?,payload_availability='retained_unscanned' WHERE original_url=?", (str(path), url))
                    ks = get_or_create_keyword_set(db, 'Benchmark', terms)
                    run = start_scan_run(db, ks, 'Benchmark', 1, 'download')
                jobs = [ScanJob.create(run, 'Benchmark', terms)]
                start = time.perf_counter()
                stats = _scan_pending_captures(cfg, db, jobs, threading.Event(), None)
                durations.append(time.perf_counter() - start)
                assert stats['scanned'] == a.count and stats['errors'] == 0
                rows = [tuple(r) for r in db.execute('SELECT score,hits_json,fields_json,snippets_json,interesting_links_json,excluded,required_missing,proximity_json FROM document_matches m JOIN documents d ON d.id=m.document_id ORDER BY d.capture_id')]
                rows = [[json.loads(x) if isinstance(x, str) else x for x in r] for r in rows]
                hashes.append(hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest())
                with db:
                    newrun = start_scan_run(db, ks, 'Rescan', 1, 'rescan')
                start = time.perf_counter()
                rescan_keyword_sets(db, [ScanJob.create(newrun, 'Benchmark', terms)], threading.Event(), workers=workers, report_config=cfg.report)
                rescans.append(time.perf_counter() - start)
                assert db.execute('SELECT COUNT(*) FROM document_matches WHERE scan_run_id=?', (newrun,)).fetchone()[0] == a.count
            finally:
                db.close()
    assert len(set(hashes)) == 1
    result = {'workers': workers, 'pages': a.count, 'scan_seconds_median': statistics.median(durations), 'rescan_seconds_median': statistics.median(rescans), 'result_sha256': hashes[0]}
    summary.append(result)
    print(json.dumps(result), flush=True)
if a.output:
    Path(a.output).write_text(json.dumps(summary, indent=2) + '\n')
