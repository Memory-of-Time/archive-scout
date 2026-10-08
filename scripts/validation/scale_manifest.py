"""Production million-row selection/state exercise. NO bodies or HTTP are created."""
from __future__ import annotations
import argparse, json, sqlite3, sys, threading, time
from pathlib import Path
from runtime_metrics import peak_rss_mib

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--repository',type=Path,required=True)
    p.add_argument('--workspace',type=Path,required=True);p.add_argument('--count',type=int,default=1000000)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--disable-mmap',action='store_true');args=p.parse_args()
    sys.path.insert(0,str(args.repository.resolve()))
    from archive_scout.config import ProjectConfig
    from archive_scout.cdx.parameters import cdx_query_signature
    from archive_scout.database.connection import open_database
    from archive_scout.database.repositories import get_or_create_target,upsert_captures
    from archive_scout.downloads.downloader import prepare_acquisition_rows
    from archive_scout.utils import utc_now
    args.workspace.mkdir(parents=True,exist_ok=True)
    cfg=ProjectConfig(args.workspace,['example.com/*'],[],from_date='2001',to_date='2009',cdx_collapses=['digest']).normalized()
    db=open_database(args.workspace)
    if args.disable_mmap: db.execute('PRAGMA mmap_size=0')
    checks=[]
    def check(label, connection):
        findings=[row[0] for row in connection.execute('PRAGMA quick_check')]
        entry={'phase':label,'ok':findings==['ok'],'findings':findings,
               'page_count':connection.execute('PRAGMA page_count').fetchone()[0],
               'database_bytes':(args.workspace/'archive_scout.sqlite3').stat().st_size}
        checks.append(entry)
        print(json.dumps({'integrity':entry}),flush=True)
        return entry['ok']
    assert db.execute('SELECT COUNT(*) FROM captures').fetchone()[0]==0,'Use a new workspace.'
    started=time.perf_counter();sig=cdx_query_signature(cfg)
    with db:
        target=get_or_create_target(db,'example.com/*')
        for begin in range(0,args.count,10000):
            rows=(('20050101000000','http://example.com/'+str(i),'text/html','200','D'+str(i),'8192')
                  for i in range(begin,min(args.count,begin+10000)))
            upsert_captures(db,rows,target,sig)
    seeded=time.perf_counter()-started
    db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    check('after_seed',db)
    started=time.perf_counter()
    total,rows,stats=prepare_acquisition_rows(db,cfg,stop_event=threading.Event())
    prep=time.perf_counter()-started
    count=0;samples=[];batch=[];last=time.perf_counter();segment=0
    now=utc_now()
    for row in rows:
        count+=1;batch.append((now,row['id']))
        if len(batch)>=2000:
            with db:db.executemany("UPDATE captures SET state='downloaded_unscanned',updated_at=? WHERE id=?",batch)
            batch.clear()
        if count%100000==0 or count==args.count:
            current=time.perf_counter();segment+=1
            samples.append({'completed_metadata_rows':count,'segment_seconds':current-last,
                            'cumulative_seconds':current-started,'peak_rss_mib':peak_rss_mib()})
            last=current
    if batch:
        with db:db.executemany("UPDATE captures SET state='downloaded_unscanned',updated_at=? WHERE id=?",batch)
    assert count==total==args.count
    assert db.execute("SELECT COUNT(*) FROM captures WHERE state='pending'").fetchone()[0]==0
    db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    check('after_updates',db)
    result={'note':'Metadata selection/update simulation, not downloaded or scanned bodies, not a paced or live soak.',
            'http_requests':0,'created_payload_files':0,'seeded_rows':args.count,'selected_rows':count,
            'seed_seconds':seeded,'preparation_seconds':prep,'selection_and_update_seconds':time.perf_counter()-started,
            'samples':samples,'database_bytes':(args.workspace/'archive_scout.sqlite3').stat().st_size,'selection_stats':stats,
            'python':sys.version,'sqlite':sqlite3.sqlite_version,
            'mmap_size':db.execute('PRAGMA mmap_size').fetchone()[0],
            'explicit_mmap_override_requested':args.disable_mmap,'integrity_checks':checks}
    result['mmap_disabled']=result['mmap_size']==0
    db.close()
    reader=sqlite3.connect((args.workspace/'archive_scout.sqlite3').resolve().as_uri()+'?mode=ro',uri=True)
    try:
        check('after_close',reader)
        result['final_count']=reader.execute('SELECT COUNT(*) FROM captures').fetchone()[0]
    finally:
        reader.close()
    result['all_integrity_checks_ok']=all(c['ok'] for c in checks)
    args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
    assert result['all_integrity_checks_ok'], 'Fixture integrity check failed; timings are not validated scale evidence.'
