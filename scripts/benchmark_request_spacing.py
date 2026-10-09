"""Fixed pacing simulation and optional validated loopback save benchmark.

Loopback results are local capacity measurements, never a live-service guarantee.
"""
from __future__ import annotations
import argparse, ctypes, hashlib, json, math, os, sys, tempfile, threading, time
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from archive_scout.downloads import rate_limit as rate

def pacing(count):
    rate.reset_shared_traffic_state_for_tests()
    clock=[1000.0]
    def wait(timeout): clock[0]+=timeout
    stop=threading.Event(); first=previous=None; smallest=math.inf
    with patch.object(rate.time,'monotonic',lambda:clock[0]):
        limiter=rate.SharedFixedRateLimiter(.125,rate.WAYBACK_REPLAY_RATE_KEY)
        limiter.condition.wait=wait
        for i in range(count):
            limiter.wait(stop)
            if first is None:first=clock[0]
            if previous is not None:smallest=min(smallest,clock[0]-previous)
            previous=clock[0]
            if i%1000==0:limiter.note_rate_limit(i)
            assert limiter.effective_delay==.125
        limiter.close()
    return {'admissions':count,'virtual_seconds':clock[0]-first,'starts_per_second':(count-1)/(clock[0]-first),
            'minimum_spacing':smallest,'throttle_signals_changed_spacing':False,'real_downloads':0}

def rss():
    if os.name!='nt':
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/(1024*1024 if sys.platform=='darwin' else 1024)
    from ctypes import wintypes
    class Counters(ctypes.Structure):
        _fields_=[('cb',wintypes.DWORD),('faults',wintypes.DWORD)]+[(n,ctypes.c_size_t) for n in ('peak','working','a','b','c','d','e','f')]
    c=Counters();c.cb=ctypes.sizeof(c)
    process=ctypes.windll.kernel32.GetCurrentProcess;process.restype=wintypes.HANDLE
    measure=ctypes.windll.psapi.GetProcessMemoryInfo;measure.argtypes=[wintypes.HANDLE,ctypes.POINTER(Counters),wintypes.DWORD]
    return c.working/(1024*1024) if measure(process(),ctypes.byref(c),c.cb) else None

def saves(count,history):
    from archive_scout.cdx.client import HttpClient
    from archive_scout.cdx.parameters import cdx_query_signature
    from archive_scout.config import ProjectConfig
    from archive_scout.database.connection import open_database
    from archive_scout.downloads.downloader import download_archive_only
    from archive_scout.utils import utc_now
    body=b'<html><body>complete needle evidence '+b'x'*16384+b'</body></html>'
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    local=f'http://127.0.0.1:{server.server_port}/capture'
    class LocalClient(HttpClient):
        def download_to_path(self,url,*args,**kwargs):
            kwargs.pop('redirect_validator',None)
            result=super().download_to_path(local,*args,**kwargs);result['final_url']=url;return result
    try:
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);cfg=ProjectConfig(root,['example.com/*'],[],from_date='2001',to_date='2001').normalized()
            cfg.network.trust_environment=False
            db=open_database(root);signature=cdx_query_signature(cfg);now=utc_now()
            sql="INSERT INTO captures(original_url,timestamp,query_signature,mimetype,statuscode,length,state,resource_class,resource_classifier_revision,created_at,updated_at,classifier_revision) VALUES(?,?,?,?,?,?,?,?,?,?,?,2)"
            seed_start=time.perf_counter()
            with db:
                for start in range(0,history,10000):
                    db.executemany(sql,((f'http://example.com/history/{i}','19950101000000',signature,'image/jpeg','200',1,'skipped','image',3,now,now) for i in range(start,min(start+10000,history))))
                db.executemany(sql,((f'http://example.com/current/{i}.html','20010101000000',signature,'text/html','200',len(body),'pending','text',3,now,now) for i in range(count)))
            seeded=time.perf_counter()-seed_start; samples=[];started=time.perf_counter();cpu=time.process_time()
            def progress(event):
                if event.detail and event.detail.get('downloaded'):
                    samples.append({'seconds':time.perf_counter()-started,'saved':int(event.detail['downloaded']),'rss_mib':rss()})
            rate.reset_shared_traffic_state_for_tests()
            with patch('archive_scout.downloads.downloader.HttpClient',LocalClient):
                result=download_archive_only(cfg,db,threading.Event(),progress)
            elapsed=time.perf_counter()-started
            rows=db.execute("SELECT local_path,state FROM captures WHERE timestamp='20010101000000'").fetchall()
            digest=hashlib.sha256(body).digest()
            for row in rows:
                assert row['state']=='downloaded_unscanned'
                assert hashlib.sha256(Path(row['local_path']).read_bytes()).digest()==digest
            assert result['downloaded']==count and result['errors']==0
            windows=[]
            for label,low,high in [('early',0,count//3),('middle',count//3,2*count//3),('late',2*count//3,count)]:
                a=next((x for x in samples if x['saved']>=low),None) if low else {'seconds':0,'saved':0}
                b=next((x for x in samples if x['saved']>=high),None)
                windows.append({'phase':label,'saved_per_second':round((b['saved']-a['saved'])/(b['seconds']-a['seconds']),3) if a and b and b['seconds']>a['seconds'] else None})
            db.close()
            return {'history_rows':history,'seed_seconds':round(seeded,3),'validated_saves':count,'elapsed_seconds':round(elapsed,3),
                    'validated_saves_per_second':round(count/elapsed,3),'cpu_seconds':round(time.process_time()-cpu,3),
                    'samples':samples,'windows':windows,'bytes_per_capture':len(body),'rss_final_mib':rss(),
                    'scope':'Healthy local HTTP fixture, real transport/pacing/files/SQLite. No live Wayback or overnight claim.'}
    finally:server.shutdown();server.server_close();thread.join(timeout=3)

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--admissions',type=int,default=10000);p.add_argument('--captures',type=int,default=0);p.add_argument('--history',type=int,default=0);p.add_argument('--output',type=Path,default=ROOT/'validation/request-spacing.json');a=p.parse_args()
    if a.admissions<2 or a.captures<0 or a.history<0:p.error('invalid workload size')
    report={'status':'passed','pacing':pacing(a.admissions)}
    if a.captures:report['loopback']=saves(a.captures,a.history)
    a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8');print(json.dumps(report,indent=2))
if __name__=='__main__':main()
