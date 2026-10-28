"""Stage-boundary outcome/dashboard costs as a synthetic project grows.

No HTTP, payload retention, scanning or live-service throughput is measured.
Rows are generated in SQLite; Python never holds the project inventory.
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from pathlib import Path


def run(source_root):
    sys.path.insert(0, str(source_root.resolve()))
    from archive_scout.config import ProjectConfig
    from archive_scout.cdx.parameters import cdx_query_signature
    from archive_scout.database.connection import open_database
    from archive_scout.database.repositories import get_or_create_target
    from archive_scout.downloads.diagnostics import _process_memory
    from archive_scout.downloads.outcomes import text_outcome_counts
    from archive_scout.ui.dashboard import read_dashboard_counts
    samples = []
    with tempfile.TemporaryDirectory(prefix='scout-outcome-scale-') as folder:
        root = Path(folder)
        cfg = ProjectConfig(root, targets=['example.org/*'], keywords=['needle'],
                            from_date='2008', to_date='2008').normalized()
        db = open_database(root)
        try:
            tid = get_or_create_target(db, cfg.targets[0])
            signature = cdx_query_signature(cfg)
            previous = 0
            for label, count in [('early', 100_000), ('middle', 500_000), ('late', 1_000_000)]:
                with db:
                    db.execute('''WITH RECURSIVE n(i) AS (VALUES(?) UNION ALL SELECT i+1 FROM n WHERE i<?)
                        INSERT INTO captures(original_url,timestamp,target_id,query_signature,mimetype,statuscode,
                            length,state,created_at,updated_at)
                        SELECT 'http://example.org/'||i,'20080101000000',?,?,'text/html','200',1024,
                            CASE i%1000 WHEN 0 THEN 'error' WHEN 1 THEN 'error' WHEN 2 THEN 'pending'
                                 ELSE 'downloaded' END,'now','now' FROM n''', (previous + 1, count, tid, signature))
                    db.execute('''INSERT INTO capture_routing(capture_id,resource_class,evidence,confident,routing,updated_at)
                        SELECT id,'text','fixture',1,'failed_nonretryable:missing_capture','now'
                        FROM captures WHERE id>? AND id%1000=0''', (previous,))
                    db.execute('''INSERT INTO errors(capture_id,operation,category,message,http_status,
                        attempt_count,retryable,resolved,ignored,first_seen,last_seen)
                        SELECT id,'download','missing_capture','fixture HTTP 404',404,1,0,0,0,'now','now'
                        FROM captures WHERE id>? AND id%1000=0''', (previous,))
                previous = count
                cpu = time.process_time()
                started = time.perf_counter()
                outcomes = text_outcome_counts(db, cfg)
                outcome_seconds = time.perf_counter() - started
                assert outcomes == {'unfinished': count // 500, 'unavailable': count // 1000}, outcomes
                started = time.perf_counter()
                dashboard = read_dashboard_counts(root / 'archive_scout.sqlite3', max_query_seconds=20)
                dashboard_seconds = time.perf_counter() - started
                assert dashboard['captures'] == dashboard['accounted_captures'] == count
                assert dashboard['unavailable_captures'] == count // 1000
                samples.append({'phase': label, 'captures': count, **outcomes,
                    'outcome_seconds': outcome_seconds, 'dashboard_seconds': dashboard_seconds,
                    'cpu_seconds_for_queries': time.process_time() - cpu,
                    'database_bytes': db.execute('PRAGMA page_count').fetchone()[0] * db.execute('PRAGMA page_size').fetchone()[0],
                    'temp_database_bytes': db.execute('PRAGMA temp.page_count').fetchone()[0] * db.execute('PRAGMA temp.page_size').fetchone()[0],
                    **_process_memory()})
        finally:
            db.close()
    return {'scope': 'synthetic SQL costs at stage boundaries; no replay throughput or days-long test',
            'samples': samples}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--output', type=Path, default=Path('validation/outcome-accounting.json'))
    args = parser.parse_args()
    result = run(args.source_root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
