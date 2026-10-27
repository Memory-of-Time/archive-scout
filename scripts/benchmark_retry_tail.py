"""Synthetic dashboard and retry-tail costs at increasing project sizes."""
from __future__ import annotations
import argparse
import json
import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import patch


def run(source_root):
    sys.path.insert(0,str(source_root))
    from archive_scout.config import ProjectConfig, MediaConfig
    from archive_scout.downloads.diagnostics import _process_memory
    from archive_scout.database.connection import open_database
    from archive_scout.database.repositories import get_or_create_target, record_error
    from archive_scout.media.indexer import media_query_signature
    from archive_scout.media.recovery import recover_active_media
    from archive_scout.ui.dashboard import read_dashboard_counts
    result={'scope':'synthetic SQL only; no live service or replay throughput measured','samples':[]}
    with tempfile.TemporaryDirectory() as folder:
        root=Path(folder)
        cfg=ProjectConfig(root,targets=['example.org/*'],keywords=[],from_date='2008',to_date='2008',
            max_attempts=2,media=MediaConfig(enabled=True,discover_embedded=False,snapshot_strategy='all')).normalized()
        db=open_database(root);tid=get_or_create_target(db,cfg.targets[0]);signature=media_query_signature(cfg)
        previous=0
        try:
            for label,count in [('early',100000),('middle',500000),('late',1000000)]:
                with db:
                    db.execute('''WITH RECURSIVE n(i) AS (VALUES(?) UNION ALL SELECT i+1 FROM n WHERE i<?)
                        INSERT INTO captures(original_url,timestamp,target_id,query_signature,mimetype,statuscode,length,state,created_at,updated_at)
                        SELECT 'http://example.org/'||i,'20080101000000',?,'fixture','text/html','200',1024,
                        CASE i%10 WHEN 0 THEN 'skipped' WHEN 1 THEN 'skipped' WHEN 2 THEN 'downloaded'
                        WHEN 3 THEN 'downloaded_unscanned' WHEN 4 THEN 'downloading' WHEN 5 THEN 'error'
                        ELSE 'pending' END,'now','now' FROM n''',(previous+1,count,tid))
                    db.execute("""INSERT INTO capture_routing SELECT id,'text','fixture',1,
                        CASE id%10 WHEN 0 THEN 'skipped_non_text' WHEN 1 THEN 'deferred_to_media'
                        ELSE 'pending' END,'now' FROM captures WHERE id>?""",(previous,))
                    db.execute("""INSERT INTO media_captures(original_url,timestamp,query_signature,
                        media_kind,extension,mimetype,statuscode,length,state,created_at,updated_at)
                        SELECT original_url||'.jpg',timestamp,?,'image','.jpg','image/jpeg','200',1024,
                        'downloaded','now','now' FROM captures WHERE id>?""",(signature,previous))
                previous=count
                cpu=time.process_time();start=time.perf_counter()
                counts=read_dashboard_counts(root/'archive_scout.sqlite3',max_query_seconds=10)
                dashboard_seconds=time.perf_counter()-start
                assert counts['captures']==counts['accounted_captures']==count
                ids=[row[0] for row in db.execute('SELECT id FROM media_captures ORDER BY id DESC LIMIT 20')]
                with db:
                    db.executemany("UPDATE media_captures SET state='error' WHERE id=?",((i,) for i in ids))
                    for cid in ids:
                        record_error(db,'media_download','connection','offline fixture',media_capture_id=cid,retryable=True)
                        db.execute("INSERT OR REPLACE INTO media_retry_schedule VALUES(?,0,1,'connection',NULL,'now')",(cid,))
                calls=[];vm=[0]
                db.set_progress_handler(lambda:vm.__setitem__(0,vm[0]+1000),1000)
                start=time.perf_counter()
                with patch('archive_scout.media.downloader.download_media',
                           side_effect=lambda *a,**kw:calls.extend(kw['media_capture_ids'])):
                    recover_active_media(cfg,db,threading.Event(),allow_partial_inventory=True)
                retry_seconds=time.perf_counter()-start;db.set_progress_handler(None,0)
                assert len(calls)==40 and set(calls)==set(ids)
                result['samples'].append({'phase':label,'text_captures':count,'media_captures':count,
                    'accounted_captures':counts['accounted_captures'],'dashboard_seconds':dashboard_seconds,
                    'dashboard_fits_default_2s_deadline':dashboard_seconds<2,
                    'retry_tail_seconds':retry_seconds,'retry_vm_steps_approx':vm[0],
                    'attempts':len(calls),'cpu_seconds':time.process_time()-cpu,
                    'threads':threading.active_count(),
                    **_process_memory()})
                with db:
                    db.executemany("UPDATE media_captures SET state='downloaded' WHERE id=?",((i,) for i in ids))
                    db.execute('DELETE FROM media_retry_schedule')
        finally:db.close()
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--source-root',type=Path,default=Path(__file__).resolve().parents[1])
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();result=run(args.source_root.resolve())
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))
