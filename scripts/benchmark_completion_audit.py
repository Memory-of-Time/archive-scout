"""Synthetic large-project SQL costs. No HTTP or live-service speed claims.

Compare the same workload against two source roots in separate processes.
The database lives in a temporary directory and is removed after the run.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import sys
import tempfile
import time
from pathlib import Path


def run(args):
    sys.path.insert(0, str(args.source_root.resolve()))
    from archive_scout.config import ProjectConfig
    from archive_scout.database.connection import open_database
    from archive_scout.database.repositories import get_or_create_target
    from archive_scout.cdx.parameters import cdx_query_signature
    from archive_scout.downloads.downloader import prepare_download_rows
    from archive_scout.media.downloader import iter_media_download_rows
    from archive_scout.downloads.diagnostics import build_fingerprint
    def memory():
        try:
            import resource
            value=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return int(value*(1 if sys.platform=='darwin' else 1024))
        except ImportError:return None

    started=time.perf_counter();cpu=time.process_time()
    with tempfile.TemporaryDirectory(prefix='scout-completion-benchmark-') as tmp:
        cfg=ProjectConfig(Path(tmp),targets=['example.org/*'],keywords=['needle'],
                          from_date='2008',to_date='2008').normalized()
        db=open_database(cfg.output_dir);target=get_or_create_target(db,cfg.targets[0]);sig=cdx_query_signature(cfg)
        try:
            with db:
                db.execute('''WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?)
                    INSERT INTO captures(original_url,timestamp,target_id,query_signature,mimetype,statuscode,length,state,created_at,updated_at)
                    SELECT 'http://example.org/history/'||i||'.html','20080101000000',?,?,'text/html','200',1024,
                           CASE WHEN i>? THEN 'error' ELSE 'downloaded' END,'now','now' FROM n''',
                    (args.history,target,sig,args.history-20))
                db.execute('''INSERT INTO documents(capture_id,path,title,body_text,links_json,content_hash,normalized_hash,size_bytes,created_at,updated_at)
                    SELECT id,'synthetic-only-'||id,'','','[]','','',1024,'now','now' FROM captures WHERE state='downloaded' ''')
            ids=[r[0] for r in db.execute("SELECT id FROM captures WHERE state='error' ORDER BY id")]
            selected=[]
            for iteration in range(3):
                vm=[0];db.set_progress_handler(lambda:vm.__setitem__(0,vm[0]+1),1000)
                began=time.perf_counter()
                total,items=prepare_download_rows(db,cfg,[],states=('error',),capture_ids=ids)
                result=list(items);elapsed=time.perf_counter()-began;db.set_progress_handler(None,0)
                assert total==len(result)==20 and {r['id'] for r in result}==set(ids)
                selected.append({'iteration':iteration,'seconds':elapsed,'sqlite_vm_steps_approx':vm[0]*1000})
            with db:
                db.execute('''WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?)
                    INSERT INTO media_captures(original_url,timestamp,query_signature,media_kind,extension,mimetype,statuscode,length,state,created_at,updated_at)
                    SELECT 'http://example.org/media/'||i||'.jpg','20080101000000','benchmark','image','.jpg','image/jpeg','200',
                    CASE WHEN i%101=0 THEN 0 ELSE (i*7919)%100000 END,'pending','now','now' FROM n''',(args.media,))
            began=time.perf_counter();total,items=iter_media_download_rows(db,["query_signature=?","state='pending'"],['benchmark'])
            prepared=time.perf_counter()-began
            checkpoints=[];count=0;last_time=time.perf_counter();last_count=0;seen_sum=0
            segment_boundaries=[('early',args.media//3),('middle',2*args.media//3),('late',args.media)]
            segment=0;batch=[];last_order=(-1,-1,0)
            for row in items:
                length=max(0,int(row['length'] or 0));order=(int(length<=0),length,int(row['id']))
                assert order>last_order,'queue repeated or reordered an identity';last_order=order
                count+=1;seen_sum+=int(row['id']);batch.append((int(row['id']),))
                if len(batch)>=1000:
                    with db:db.executemany("UPDATE media_captures SET state='downloaded' WHERE id=?",batch)
                    batch.clear()
                if segment<len(segment_boundaries) and count>=segment_boundaries[segment][1]:
                    now=time.perf_counter()
                    checkpoints.append({'phase':segment_boundaries[segment][0], 'items':count-last_count,
                        'seconds':now-last_time,'items_per_second':(count-last_count)/max(.000001,now-last_time),
                        'peak_rss_bytes':memory(),'cpu_seconds':time.process_time()-cpu})
                    last_time=now;last_count=count;segment+=1
            if batch:
                with db:db.executemany("UPDATE media_captures SET state='downloaded' WHERE id=?",batch)
            assert count==total==args.media
            assert seen_sum==args.media*(args.media+1)//2
            assert db.execute("SELECT COUNT(*) FROM media_captures WHERE state='pending'").fetchone()[0]==0
            return {'scope':'synthetic SQL only; no replay transfer or scan throughput measured',
                'source_root':str(args.source_root.resolve()),'build':build_fingerprint(),
                'history_rows':args.history,'historical_documents':args.history-20,'selected_tail_captures':20,
                'selected_retry_preparation':selected,'media_rows':args.media,'media_queue_prepare_seconds':prepared,
                'media_segments':checkpoints,'all_identities_accounted_for':True,
                'peak_rss_bytes':memory(),'cpu_seconds':time.process_time()-cpu,
                'elapsed_seconds':time.perf_counter()-started,'python':platform.python_version(),
                'logical_cpus':os.cpu_count(),'database_bytes':db.execute('PRAGMA page_count').fetchone()[0]*db.execute('PRAGMA page_size').fetchone()[0],
                'temp_database_bytes':db.execute('PRAGMA temp.page_count').fetchone()[0]*db.execute('PRAGMA temp.page_size').fetchone()[0]}
        finally:db.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root',type=Path,default=Path(__file__).resolve().parents[1])
    parser.add_argument('--history',type=int,default=1_000_000)
    parser.add_argument('--media',type=int,default=100_000)
    parser.add_argument('--output',type=Path,default=Path('validation/completion-scale.json'))
    args=parser.parse_args()
    if args.history<20 or args.media<3:parser.error('history must be >=20 and media >=3')
    result=run(args);args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result,indent=2))
