"""90-save localhost end-to-end acquisition and scan smoke test; never a live Wayback benchmark."""
from __future__ import annotations
import argparse,hashlib,json,sys,tempfile,threading,time
from pathlib import Path
from http.server import ThreadingHTTPServer,BaseHTTPRequestHandler
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from archive_scout.config import ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target,upsert_captures,get_or_create_keyword_set,start_scan_run
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.downloads.downloader import download_archive
from archive_scout.downloads.rate_limit import reset_shared_traffic_state_for_tests
from archive_scout.scanning.jobs import ScanJob


def run(count:int=90)->dict:
    body=b'<html><body>archived needle '+b'x'*4096+b'</body></html>'
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def do_GET(self):
            self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8')
            self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder)
            config=ProjectConfig(root,targets=['example.net/*'],keywords=['needle'],cdx_delay=0,download_delay=0.125).normalized()
            config.network.trust_environment=False
            db=open_database(root)
            signature=cdx_query_signature(config)
            target=get_or_create_target(db,'example.net/*')
            with db:
                upsert_captures(db,(("20010101000000",f"http://example.net/page/{n}","text/html","200",f"D{n}",len(body)) for n in range(count)),target,signature)
            keyword_set_id=get_or_create_keyword_set(db,'Test',['needle'])
            run_id=start_scan_run(db,keyword_set_id,'loopback test',1,'all',{})
            db.commit()
            samples=[];started=time.perf_counter()
            def progress(e):
                if e.stage=='download' and e.detail and e.detail.get('new_saves'):
                    samples.append((int(e.detail['new_saves']),time.perf_counter()-started))
            reset_shared_traffic_state_for_tests()
            try:
                with patch('archive_scout.downloads.downloader.replay_url',return_value=f'http://127.0.0.1:{server.server_port}/data'):
                    download_archive(config,db,run_id,threading.Event(),progress,scan_jobs=[ScanJob.create(run_id,'Test',['needle'])])
                elapsed=time.perf_counter()-started
                assert int(db.execute("SELECT COUNT(*) FROM captures WHERE state='downloaded'").fetchone()[0]) == count
                files=list((root/'captures').rglob('*.txt'))
                assert len(files)==count
                assert all(hashlib.sha256(p.read_bytes()).digest()==hashlib.sha256(body).digest() for p in files)
                windows=[]
                for label,low,high in [('early',0,count//3),('middle',count//3,2*count//3),('late',2*count//3,count)]:
                    a=next((t for n,t in samples if n>=low),0.0) if low else 0
                    b=next((t for n,t in samples if n>=high),None)
                    windows.append({'phase':label,'saves_per_second':round((high-low)/(b-a),3) if b and b>a else None})
                acquisition_seconds=samples[-1][1] if samples else elapsed
                return {'validated':count,'elapsed_seconds':round(elapsed,3),'saves_per_second':round(count/elapsed,3),
                        'acquisition_seconds':round(acquisition_seconds,3),
                        'acquisition_saves_per_second':round(count/max(.001,acquisition_seconds),3),
                        'scan_drain_seconds':round(max(0,elapsed-acquisition_seconds),3),
                        'segment_rates':windows,'sha256_verified':True,'scope':'localhost HTTP, not live Wayback'}
            finally:db.close()
    finally:server.shutdown();server.server_close();thread.join(timeout=3)

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--count',type=int,default=90);parser.add_argument('--output',type=Path,default=Path('validation/loopback.json'))
    args=parser.parse_args();result=run(args.count);args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
