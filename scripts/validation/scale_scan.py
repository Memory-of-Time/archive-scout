"""Real local files, fresh database/process per worker count; no public traffic."""
from __future__ import annotations
import argparse, hashlib, json, os, platform, shutil, sqlite3, subprocess
import sys, threading, time, multiprocessing
from pathlib import Path
from runtime_metrics import snapshot as process_snapshot

RULES = [
    ['needle', 'archive', 'rare footage', 'exact: café', 'require: archive', 'exclude: irrelevant ad'],
    ['regex: needle\\s+archive', 'exact: record', 'mirror', 'episode'],
]

def config(root, workers):
    from archive_scout.config import ProjectConfig
    return ProjectConfig(root, ['example.com/*'], RULES[0], from_date='2001', to_date='2009',
                         cdx_collapses=['digest'], scan_workers=workers).normalized()

def resources():
    result = process_snapshot()
    result['children'] = len(multiprocessing.active_children())
    # A child PID exposed through the runtime can identify an unrelated /proc
    # entry across PID namespaces. Worker-owned shared peak measurements below
    # are the portable source; do not report a misleading sampled total.
    result['external_child_rss_sampled'] = False
    return result

def child(args):
    from archive_scout.database.connection import open_database
    from archive_scout.database.repositories import get_or_create_keyword_set, start_scan_run
    from archive_scout.downloads.downloader import _scan_pending_captures
    from archive_scout.scanning.jobs import ScanJob
    from archive_scout.scanning.automaton import ahocorasick_rs
    root = args.workspace / ('worker-'+str(args.one_worker))
    root.mkdir(exist_ok=True)
    assert not (root/'archive_scout.sqlite3').exists(), 'Use a fresh worker directory; do not overwrite SQLite with live sidecars.'
    shutil.copy2(args.workspace/'seed.sqlite3', root/'archive_scout.sqlite3')
    db = open_database(root)
    if args.disable_mmap: db.execute('PRAGMA mmap_size=0')
    cfg = config(root, args.one_worker)
    checks=[]
    def check(label, connection):
        findings=[row[0] for row in connection.execute('PRAGMA quick_check')]
        checks.append({'phase':label,'ok':findings==['ok'],'findings':findings})
    check('before_scan',db)
    jobs = []
    with db:
        for index, rules in enumerate(RULES):
            kid = get_or_create_keyword_set(db, 'Set '+str(index), rules)
            rid = start_scan_run(db, kid, 'Set '+str(index), 1, 'download')
            jobs.append(ScanJob.create(rid, 'Set '+str(index), rules))
    samples = []
    worker_peak_sum = 0.0
    started = time.perf_counter()
    next_fraction = 1
    def progress(event):
        nonlocal next_fraction, worker_peak_sum
        worker_peak_sum = max(worker_peak_sum, float((event.detail or {}).get("scan_worker_peak_rss_sum_mib") or 0))
        if event.stage != 'scan' or not event.current or not event.total:
            return
        fraction = event.current/event.total
        if fraction >= next_fraction/10:
            samples.append({'completed':event.current,'wall_seconds':time.perf_counter()-started,
                            **resources()})
            next_fraction += 1
    before = resources()
    if args.profile:
        import cProfile, pstats
        profiler=cProfile.Profile();profiler.enable()
        result = _scan_pending_captures(cfg, db, jobs, threading.Event(), progress)
        profiler.disable();profiler.dump_stats(str(args.workspace/'scanner-profile.prof'))
        with (args.workspace/'scanner-profile.txt').open('w') as handle:
            pstats.Stats(profiler,stream=handle).sort_stats('cumulative').print_stats(25)
    else:
        result = _scan_pending_captures(cfg, db, jobs, threading.Event(), progress)
    elapsed = time.perf_counter()-started
    check('after_scan',db)
    assert result['scanned'] == args.count and result['errors'] == 0, result
    assert db.execute('SELECT COUNT(*) FROM document_matches').fetchone()[0] == args.count*len(RULES)
    digest = hashlib.sha256()
    for row in db.execute('SELECT d.capture_id,m.scan_run_id,m.score,m.hits_json,m.fields_json,m.snippets_json,m.excluded,m.required_missing FROM document_matches m JOIN documents d ON d.id=m.document_id ORDER BY d.capture_id,m.scan_run_id'):
        digest.update(json.dumps(list(row),ensure_ascii=False,separators=(',',':')).encode())
        digest.update(b'\n')
    output = {'workers_requested':args.one_worker,'native_matcher_loaded':ahocorasick_rs is not None,
              'captures':args.count,'keyword_sets':len(RULES),'rules':RULES,
              'elapsed_seconds':elapsed,'captures_per_second':args.count/elapsed,
              'result_sha256':digest.hexdigest(),'stats':result,'samples':samples,
              'before':before,'after':resources(), 'worker_peak_rss_sum_mib':worker_peak_sum,
              'conservative_parent_plus_worker_peak_rss_mib':resources()['peak_rss_mib'] + worker_peak_sum,
              'cache_note':'Fresh process and copied seed DB; filesystem cache is warm/ uncontrolled.',
              'python':sys.version,'sqlite':sqlite3.sqlite_version,'integrity_checks':checks}
    output['mmap_size']=db.execute('PRAGMA mmap_size').fetchone()[0]
    output['mmap_disabled']=output['mmap_size']==0
    output['explicit_mmap_override_requested']=args.disable_mmap
    if args.checkpoint_before_close:
        output['explicit_checkpoint']=list(db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone())
        check('after_explicit_checkpoint',db)
    db.close()
    reader=sqlite3.connect((root/'archive_scout.sqlite3').resolve().as_uri()+'?mode=ro',uri=True)
    try:
        check('after_close',reader)
        assert reader.execute('SELECT COUNT(*) FROM documents').fetchone()[0]==args.count
    finally:
        reader.close()
    fts_check=sqlite3.connect(root/'archive_scout.sqlite3')
    try:
        fts_check.execute("INSERT INTO documents_fts(documents_fts) VALUES('integrity-check')")
        fts_check.rollback()
        output['fts_integrity_after_close'] = 'passed'
        output['final_checkpoint']=list(fts_check.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone())
        assert output['final_checkpoint']==[0,0,0]
    finally:
        fts_check.close()
    reader=sqlite3.connect((root/'archive_scout.sqlite3').resolve().as_uri()+'?mode=ro',uri=True)
    try:
        check('after_all_validation_connections_closed',reader)
        assert reader.execute('SELECT COUNT(*) FROM documents').fetchone()[0]==args.count
        assert reader.execute('SELECT COUNT(*) FROM document_matches').fetchone()[0]==args.count*len(RULES)
    finally:
        reader.close()
    wal=root/'archive_scout.sqlite3-wal'
    output['closed_wal_bytes']=wal.stat().st_size if wal.exists() else 0
    assert output['closed_wal_bytes']==0
    output['all_integrity_checks_ok']=all(c['ok'] for c in checks)
    (args.workspace/('scan-workers-'+str(args.one_worker)+'.json')).write_text(json.dumps(output,indent=2)+'\n')
    print(json.dumps({k:output[k] for k in ['workers_requested','captures_per_second','elapsed_seconds','result_sha256','after']}),flush=True)
    assert output['all_integrity_checks_ok'],'Fixture integrity check failed; timings are not validated scale evidence.'

def parent(args):
    from archive_scout.database.connection import open_database
    from archive_scout.database.repositories import get_or_create_target
    from archive_scout.cdx.parameters import cdx_query_signature
    from archive_scout.utils import utc_now
    args.workspace.mkdir(parents=True,exist_ok=True)
    corpus = args.workspace/'corpus'
    corpus.mkdir(exist_ok=True)
    seed_root = args.workspace/'seed'
    seed_root.mkdir(exist_ok=True)
    db = open_database(seed_root)
    assert db.execute('SELECT COUNT(*) FROM captures').fetchone()[0] == 0, 'Use a new workspace.'
    cfg=config(seed_root,1)
    sig=cdx_query_signature(cfg)
    sizes=[]
    source_hash=hashlib.sha256()
    now=utc_now()
    started=time.perf_counter()
    with db:
        target=get_or_create_target(db,'example.com/*')
        for i in range(args.count):
            phrase = 'ordinary notes only' if i%7==0 else 'needle archive mirror episode rare footage café'
            if i%11==0: phrase+=' irrelevant ad'
            repeats=6000 if i%1000==0 else 48+(i%16)
            raw=('<html><head><title>Record '+str(i)+'</title></head><body>'+''.join('<p>'+phrase+' record '+str(j)+'</p>' for j in range(repeats))+'</body></html>').encode()
            path=corpus/(str(i)+'.txt')
            path.write_bytes(raw)
            sizes.append(len(raw))
            source_hash.update(hashlib.sha256(raw).digest())
            db.execute("INSERT INTO captures(original_url,timestamp,query_signature,target_id,mimetype,statuscode,digest,length,resource_class,state,local_path,payload_availability,bytes_saved,created_at,updated_at) VALUES(?,'20050101000000',?,?,'text/html','200',?,?,'text','downloaded_unscanned',?,'retained_unscanned',?,?,?)",
                       ('http://example.com/'+str(i),sig,target,hashlib.sha256(raw).hexdigest(),len(raw),str(path),len(raw),now,now))
    db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    db.close()
    shutil.copy2(seed_root/'archive_scout.sqlite3',args.workspace/'seed.sqlite3')
    ordered=sorted(sizes)
    meta={'captures':args.count,'bytes':sum(sizes),'size_median':ordered[len(ordered)//2],
          'size_p95':ordered[int((len(ordered)-1)*.95)],'size_max':max(sizes),
          'corpus_sha256':source_hash.hexdigest(),'seed_seconds':time.perf_counter()-started,
          'live_network':False,'platform':platform.platform(),'cpu_count':os.cpu_count()}
    (args.workspace/'corpus.json').write_text(json.dumps(meta,indent=2)+'\n')
    print(json.dumps(meta),flush=True)
    for worker in map(int,args.workers.split(',')):
        subprocess.run([sys.executable,__file__,'--repository',str(args.repository),'--workspace',str(args.workspace),'--count',str(args.count),'--one-worker',str(worker)]+(['--profile'] if args.profile else [])+(['--disable-mmap'] if args.disable_mmap else [])+(['--checkpoint-before-close'] if args.checkpoint_before_close else []),check=True)
    results=[json.loads((args.workspace/('scan-workers-'+w+'.json')).read_text()) for w in args.workers.split(',')]
    assert len({r['result_sha256'] for r in results})==1,'Matching/persistence parity failed'
    (args.workspace/'summary.json').write_text(json.dumps({'corpus':meta,'results':results},indent=2)+'\n')

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--repository',type=Path,required=True)
    p.add_argument('--workspace',type=Path,required=True)
    p.add_argument('--count',type=int,default=33000)
    p.add_argument('--workers',default='1,2,3,4,8,0')
    p.add_argument('--one-worker',type=int,default=None)
    p.add_argument('--profile',action='store_true')
    p.add_argument('--disable-mmap',action='store_true')
    p.add_argument('--checkpoint-before-close',action='store_true')
    args=p.parse_args()
    args.repository=args.repository.resolve();args.workspace=args.workspace.resolve()
    sys.path.insert(0,str(args.repository))
    (child if args.one_worker is not None else parent)(args)
