"""Paced real HTTP acquisition with local faults; extend --count for long soaks.

Uses the normal replay floor, real unique local files and production scheduler.
This tests production logic against localhost, not Internet Archive availability.
"""
from __future__ import annotations
import argparse, hashlib, json, socket, sqlite3, sys, threading, time
from collections import defaultdict,deque,OrderedDict
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from unittest import mock
from runtime_metrics import peak_rss_mib

def body(i):
    return ('<html><head><title>Record '+str(i)+'</title></head><body>needle archive '+str(i)+' '+'ordinary text '*1200+'</body></html>').encode()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--repository',type=Path,required=True)
    p.add_argument('--workspace',type=Path,required=True);p.add_argument('--count',type=int,default=1200)
    p.add_argument('--faults',action='store_true');p.add_argument('--output',type=Path,required=True)
    p.add_argument('--scan-after',action='store_true',help='Scan every acquired body after retained acquisition.')
    p.add_argument('--overlap',action='store_true',help='Use the retained download-and-scan pipeline with overlapping local workers.')
    p.add_argument('--prefill',type=int,default=0,help='Add this many already-classified, excluded historical metadata rows before real acquisitions; no historical payload files are synthesized.')
    p.add_argument('--resume-fixture',action='store_true',help='Resume this harness\'s previously interrupted fixture and verify the entire final corpus.')
    args=p.parse_args();sys.path.insert(0,str(args.repository.resolve()))
    from archive_scout.config import ProjectConfig,NetworkConfig
    from archive_scout.cdx.parameters import cdx_query_signature
    from archive_scout.database.connection import open_database
    from archive_scout.database.repositories import get_or_create_target,upsert_captures
    from archive_scout.downloads.downloader import download_archive_only,download_archive
    from archive_scout.downloads.rate_limit import reset_shared_traffic_state_for_tests
    reset_shared_traffic_state_for_tests()
    lock=threading.Lock();attempts=OrderedDict();wire_count=0;fault_counts=defaultdict(int)
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def log_message(self,*a):pass
        def do_GET(self):
            global wire_count
            i=int(self.path.rsplit('/',1)[-1]);raw=body(i)
            with lock:
                wire_count+=1;attempt=1
                if args.faults and i and (i%400==0 or i%137==0):
                    attempts[i]=attempts.get(i,0)+1;attempt=attempts[i];attempts.move_to_end(i)
                    if len(attempts)>4096:attempts.popitem(last=False)
            if args.faults and i and i%400==0 and attempt==1:
                with lock:fault_counts['http_429']+=1
                self.send_response(429);self.send_header('Retry-After','1');self.send_header('Content-Length','0');self.end_headers();return
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8')
            self.send_header('Memento-Datetime','Sat, 01 Jan 2005 00:00:00 GMT')
            self.send_header('Content-Length',str(len(raw)));self.end_headers()
            try:
                if args.faults and i and i%137==0 and attempt==1:
                    with lock:fault_counts['partial_body_drop']+=1
                    self.wfile.write(raw[:1024]);self.wfile.flush();self.connection.shutdown(socket.SHUT_RDWR);self.connection.close();return
                if self.headers.get('Range'):
                    # Safely ignore Range: HTTP 200 means the production client
                    # must replace the prefix with this complete representation.
                    pass
                self.wfile.write(raw);self.wfile.flush()
            except (BrokenPipeError,ConnectionResetError):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.daemon_threads=True
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    args.workspace.mkdir(parents=True,exist_ok=True)
    cfg=ProjectConfig(args.workspace,['example.com/*'],[],from_date='2001',to_date='2009',cdx_collapses=['digest'],workers=10,
                      network=NetworkConfig(backend='auto',trust_environment=False),connect_timeout=2,read_timeout=5).normalized()
    signature=cdx_query_signature(cfg)
    db=open_database(args.workspace)
    initial_rows=db.execute('SELECT COUNT(*) FROM captures').fetchone()[0]
    assert initial_rows==(args.count+args.prefill if args.resume_fixture else 0),'Use a new workspace or the exact interrupted fixture.'
    initial_saved=db.execute("SELECT COUNT(*) FROM captures WHERE state IN ('downloaded','downloaded_unscanned')").fetchone()[0]
    if args.prefill and not args.resume_fixture:
        from archive_scout.downloads.downloader import CLASSIFIER_REVISION
        from archive_scout.classification import RESOURCE_CLASSIFIER_REVISION
        with db:
            db.executemany("""INSERT INTO captures(original_url,timestamp,query_signature,mimetype,statuscode,
                state,resource_class,skip_reason,classifier_revision,resource_classifier_revision,created_at,updated_at)
                VALUES(?,'20050101000000',?,'image/jpeg','200','skipped','image','metadata_non_text',?,?,'fixture','fixture')""",
                ((f'http://example.com/history-{i}.jpg',signature,CLASSIFIER_REVISION,RESOURCE_CLASSIFIER_REVISION) for i in range(args.prefill)))
    if not args.resume_fixture:
        with db:
            target=get_or_create_target(db,'example.com/*')
            upsert_captures(db,(('20050101000000','http://example.com/'+str(i),'text/html','200','D'+str(i),str(len(body(i)))) for i in range(args.count)),target,cdx_query_signature(cfg))
    jobs = None
    if args.overlap:
        from archive_scout.database.repositories import get_or_create_keyword_set,start_scan_run
        from archive_scout.scanning.jobs import ScanJob
        rules=['needle','archive','regex: ordinary\\s+text']
        with db:
            key=get_or_create_keyword_set(db,'Soak rules',rules)
            run=start_scan_run(db,key,'Soak rules',1,'download')
        jobs=[ScanJob.create(run,'Soak rules',rules)]
    samples=deque(maxlen=640);events=defaultdict(int);started=time.monotonic()
    segment_first={};segment_last={}
    sample_path=args.output.with_suffix('.samples.jsonl');sample_path.parent.mkdir(parents=True,exist_ok=True)
    sample_stream=sample_path.open('w')
    def progress(event):
        events[event.stage]+=1
        if event.stage in {'download_only','download'}:
            sample={'elapsed':time.monotonic()-started,'completed':event.detail.get('downloaded',0),
                            'wire_starts':event.detail.get('replay_started',0),'interval':event.detail.get('effective_request_interval'),
                            'peak_rss_mib':peak_rss_mib(),
                            'fds':len(list(Path('/proc/self/fd').iterdir())) if Path('/proc/self/fd').exists() else None,
                            'threads':threading.active_count(),
                            'fresh_committed':event.detail.get('fresh_committed'),
                            'fresh_rate_60s':event.detail.get('fresh_rate_60s'),
                            'scan_completed':event.detail.get('scan_completed',0)}
            samples.append(sample);sample_stream.write(json.dumps(sample)+'\n')
            index=min(2,max(0,(max(1,sample['completed'])-1)*3//args.count))
            segment_first.setdefault(index,sample);segment_last[index]=sample
    try:
        with mock.patch('archive_scout.downloads.downloader.replay_url',side_effect=lambda timestamp,url:f'http://127.0.0.1:{server.server_port}/web/{timestamp}id_/http://example.com/'+url.rsplit('/',1)[-1]):
            result=(download_archive(cfg,db,run,threading.Event(),progress,scan_jobs=jobs) if args.overlap
                    else download_archive_only(cfg,db,threading.Event(),progress))
        elapsed=time.monotonic()-started
        validated=0;total_bytes=0;digest=hashlib.sha256()
        for row in db.execute("SELECT * FROM captures WHERE state IN ('downloaded','downloaded_unscanned') ORDER BY id"):
            assert row['state']==('downloaded' if args.overlap else 'downloaded_unscanned'),dict(row)
            i=int(row['original_url'].rsplit('/',1)[-1]);raw=Path(row['local_path']).read_bytes()
            assert raw==body(i),(i,len(raw))
            assert row['bytes_saved']==len(raw)
            digest.update(hashlib.sha256(raw).digest());validated+=1;total_bytes+=len(raw)
        assert validated==args.count
        if args.overlap:
            assert result['scanned']==args.count and result['scan_errors']==0,result
            assert db.execute('SELECT COUNT(*) FROM errors WHERE resolved=0').fetchone()[0]==0
            assert db.execute('SELECT COUNT(*) FROM document_matches').fetchone()[0]==args.count
        segments=[]
        for index in sorted(segment_first):
            first,last=segment_first[index],segment_last[index]
            if last['elapsed']>first['elapsed']:
                segments.append({'completion_range':[first['completed'],last['completed']],
                                 'measured_saves_per_second':(last['completed']-first['completed'])/(last['elapsed']-first['elapsed'])})
        scan_result=None
        if args.scan_after:
            from archive_scout.database.repositories import get_or_create_keyword_set,start_scan_run
            from archive_scout.scanning.jobs import ScanJob
            from archive_scout.downloads.downloader import _scan_pending_captures
            rules=['needle','archive','regex: ordinary\\s+text']
            with db:
                key=get_or_create_keyword_set(db,'Soak rules',rules)
                run=start_scan_run(db,key,'Soak rules',1,'download')
            scan_result=_scan_pending_captures(cfg,db,[ScanJob.create(run,'Soak rules',rules)],threading.Event(),progress)
            assert scan_result['scanned']==args.count and scan_result['errors']==0
        checks=[{'phase':'before_close','findings':[r[0] for r in db.execute('PRAGMA quick_check')]}]
        output={'note':'Real paced local HTTP; includes injected faults when selected. This duration does not prove live overnight consistency.',
                'faults':args.faults,'overlap':args.overlap,'actual_validated_files':validated,'actual_bytes':total_bytes,'elapsed_seconds':elapsed,
                'historical_metadata_rows_without_payloads':args.prefill,
                'resumed_fixture':args.resume_fixture,'initial_saved_files':initial_saved,
                'overall_saves_per_second':(validated-initial_saved)/elapsed,'actual_server_wire_requests':wire_count,
                'result':result,'segments':segments,'samples':list(samples),'event_counts':dict(events),'corpus_sha256':digest.hexdigest(),
                'scan_result':scan_result,'fault_counts':dict(fault_counts),'sample_log':str(sample_path),
                'sampling_note':'JSON holds only the latest 640 samples; complete samples stream to JSONL. Counts sampled from scheduler, validated fully at completion.',
                'integrity_checks':checks,'python':sys.version,'sqlite':sqlite3.sqlite_version}
        db.close()
        reader=sqlite3.connect((args.workspace/'archive_scout.sqlite3').resolve().as_uri()+'?mode=ro',uri=True)
        try:
            checks.append({'phase':'after_close','findings':[r[0] for r in reader.execute('PRAGMA quick_check')]})
        finally:
            reader.close()
        output['all_integrity_checks_ok']=all(c['findings']==['ok'] for c in checks)
        args.output.write_text(json.dumps(output,indent=2)+'\n');print(json.dumps({k:v for k,v in output.items() if k!='samples'},indent=2))
        assert output['all_integrity_checks_ok'],'Database integrity failed; acquisition/scanning timings are not fully validated.'
    finally:
        db.close();sample_stream.close();server.shutdown();server.server_close()
