"""Paced media acquisition against a local fixture, including durable publication.

No live-service, long-duration, player-decoding, or native Windows claim.
"""
from __future__ import annotations
import argparse
import base64
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
    sys.path.insert(0,str(args.source_root.resolve()))
    from archive_scout.config import ProjectConfig, MediaConfig, NetworkConfig
    from archive_scout.database.connection import open_database
    from archive_scout.database.repositories import get_or_create_media_target, upsert_media_captures
    from archive_scout.media.indexer import media_query_signature, media_index_state_signature, _save_media_state
    from archive_scout.media.downloader import download_media
    from archive_scout.downloads.rate_limit import reset_shared_traffic_state_for_tests
    body=b'\xff\xd8\xff'+b'x'*(args.body_bytes-3)
    sha256=hashlib.sha256(body).hexdigest()
    digest=base64.b32encode(hashlib.sha1(body).digest()).decode()
    class Handler(BaseHTTPRequestHandler):
        protocol_version='HTTP/1.1'
        def do_GET(self):
            if args.response_delay:time.sleep(args.response_delay)
            self.send_response(200);self.send_header('Content-Type','image/jpeg')
            self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
        def log_message(self,*args):pass
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        with tempfile.TemporaryDirectory(prefix='scout-media-benchmark-') as tmp:
            root=Path(tmp);cfg=ProjectConfig(root,targets=['example.org/*'],keywords=[],
                from_date='2008',to_date='2008',workers=9,download_delay=.125,
                media=MediaConfig(enabled=True,discover_embedded=False,snapshot_strategy='all'),
                network=NetworkConfig(backend='httpx',trust_environment=False)).normalized()
            db=open_database(root);target=get_or_create_media_target(db,cfg.targets[0])
            signature=media_query_signature(cfg)
            with db:
                upsert_media_captures(db,[(('20080101000000',f'http://example.org/{i}.jpg',
                    'image/jpeg','200',digest,str(len(body))),'image','.jpg') for i in range(args.count)],target,signature)
                _save_media_state(db,target,2008,media_index_state_signature(cfg),None,True,args.count,None)
            samples=[];resources=[];start=time.perf_counter();cpu=time.process_time()
            def progress(event):
                if event.stage=='media_download':
                    # Each successful-only workload outcome is one durable save
                    # on both the original and patched progress APIs.
                    n=int(event.current or 0);samples.append((n,time.perf_counter()-start))
                    if len(resources)<3 and n>=(len(resources)+1)*args.count//3:
                        try:
                            import resource
                            peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*(1 if sys.platform=='darwin' else 1024)
                        except ImportError:peak=None
                        resources.append({'saved':n,'cpu_seconds':time.process_time()-cpu,
                            'peak_rss_bytes':peak,'threads':threading.active_count()})
            reset_shared_traffic_state_for_tests()
            try:
                with patch('archive_scout.media.downloader.media_replay_url',
                           return_value=f'http://127.0.0.1:{server.server_port}/body'):
                    download_media(cfg,db,threading.Event(),progress)
                elapsed=time.perf_counter()-start
                count=0
                for row in db.execute("SELECT state,path,content_hash,bytes_saved FROM media_captures"):
                    assert row['state']=='downloaded'
                    assert row['content_hash']==sha256 and row['bytes_saved']==len(body)
                    assert hashlib.sha256(Path(row['path']).read_bytes()).hexdigest()==sha256
                    count+=1
                assert count==args.count and db.execute('SELECT COUNT(*) FROM media_retry_schedule').fetchone()[0]==0
                segments=[]
                for label,low,high in [('early',0,args.count//3),('middle',args.count//3,2*args.count//3),('late',2*args.count//3,args.count)]:
                    a=next((t for n,t in samples if n>=low),0) if low else 0
                    b=next(t for n,t in samples if n>=high)
                    segments.append({'phase':label,'successful_saves_per_second':(high-low)/(b-a)})
                return {'validated_files':count,'sha256_verified':True,'body_bytes':len(body),
                    'response_delay_seconds':args.response_delay,'workers':cfg.workers,'request_delay_seconds':.125,
                    'elapsed_seconds':elapsed,'successful_saves_per_second':count/elapsed,
                    'segments':segments,'resources':resources,
                    'scope':'local fixture media acquisition only; not live Wayback or overnight consistency'}
            finally:db.close()
    finally:server.shutdown();server.server_close();thread.join(3)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root',type=Path,default=Path(__file__).resolve().parents[1])
    parser.add_argument('--count',type=int,default=240)
    parser.add_argument('--body-bytes',type=int,default=65536)
    parser.add_argument('--response-delay',type=float,default=.4)
    parser.add_argument('--output',type=Path,default=Path('validation/media-loopback.json'))
    args=parser.parse_args()
    if args.count<3 or args.body_bytes<3 or args.response_delay<0:parser.error('invalid workload size or delay')
    result=run(args);args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8');print(json.dumps(result,indent=2))
