import hashlib,json,tempfile,threading,unittest
from pathlib import Path
from unittest.mock import patch
from archive_scout.cdx.client import HttpClient,MalformedCDXResponse
from archive_scout.config import ProjectConfig,MediaConfig,NetworkConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_media_target,upsert_media_captures
from archive_scout.media.indexer import index_media,media_query_signature,media_index_state_signature,_save_media_state,_request_media_resume
from archive_scout.cdx.indexer import PendingWindow
from archive_scout.operations import run_project
from archive_scout.network.transports import TransportResponse

HEADER=['urlkey','timestamp','original','mimetype','statuscode','digest','length']
def body(rows,cursor=None):
 data=[HEADER]+[['com,phoenixtv)/',*r] for r in rows]
 if cursor:data += [[],[cursor]]
 return json.dumps(data).encode()
def row(url,mime='image/jpeg',stamp='20020101000000'):return (stamp,url,mime,'200','','4')
class Transport:
 def __init__(self,bodies):self.bodies=iter(bodies);self.urls=[]
 def request(self,url,*args):
  self.urls.append(url)
  assert 'showNumPages' not in url and 'page=' not in url and 'matchType=exact' not in url,url
  return TransportResponse(200,{},url,next(self.bodies),'fixture',.001)
 def close(self):pass
class SinglePass(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.cfg=ProjectConfig(Path(self.tmp.name),targets=['*.phoenixtv.com/*'],keywords=['x'],from_date='2001',to_date='2003',cdx_delay=0,download_delay=0,cdx_collapses=[],network=NetworkConfig(index_strategy='resume',trust_environment=False),media=MediaConfig(enabled=True,discover_embedded=False,include_extensions=['jpg'],snapshot_strategy='earliest')).normalized();self.db=open_database(self.cfg.output_dir)
 def tearDown(self):self.db.close();self.tmp.cleanup()
 def use(self,transport):
  original=HttpClient.__init__
  def init(c,*a,**kw):kw['transport']=transport;original(c,*a,**kw)
  return patch.object(HttpClient,'__init__',init)
 def test_encoded_and_extensionless_media_survive_one_traversal_and_download(self):
  rows=[row('http://phoenixtv.com/a.%6a%70%67'),row('http://phoenixtv.com/image'),row('http://phoenixtv.com/page','text/html')]
  t=Transport([body(rows,'NEXT'),body([]),body([])])
  def fetch(r,c,client,dest):
   dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(b'test')
   return dict(id=r['id'],path=dest,bytes=4,hash=hashlib.sha256(b'test').hexdigest(),status=200,final_url=r['original_url'])
  with self.use(t),patch('archive_scout.media.downloader.fetch_media',side_effect=fetch) as saved:
   result=run_project(self.cfg,'media_all',threading.Event())
  self.assertEqual(result.status,'complete');self.assertEqual(saved.call_count,2);self.assertEqual(len(t.urls),3)
  self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_discovery_queue').fetchone()[0],0)
 def test_false_empty_terminal_recovers_rows(self):
  rows=[row('http://phoenixtv.com/image')];t=Transport([body([]),body(rows)])
  with self.use(t):index_media(self.cfg,self.db,threading.Event())
  self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0],1)
 def test_bad_confirmation_keeps_inventory_unfinished(self):
  t=Transport([body([row('http://phoenixtv.com/a.jpg')]),body([row('http://phoenixtv.com/b.jpg')])])
  from archive_scout.downloads.rate_limit import FixedRateLimiter
  c=HttpClient(FixedRateLimiter(0),1,1,'test',threading.Event(),transport=t)
  w=PendingWindow(self.cfg.from_date,self.cfg.to_date,resume_key='SAFE')
  with self.assertRaises(MalformedCDXResponse):_request_media_resume(self.cfg,c,self.cfg.targets[0],w,['.jpg'])
  self.assertEqual(w.resume_key,'SAFE');c.close()
 def test_full_tokenless_page_recovers_cursor_with_smaller_probe(self):
  from archive_scout.downloads.rate_limit import FixedRateLimiter
  rows=[row('http://phoenixtv.com/a.jpg'),row('http://phoenixtv.com/b.jpg')]
  t=Transport([body(rows),body(rows),body(rows[:1],'RECOVERED')]);c=HttpClient(FixedRateLimiter(0),1,1,'test',threading.Event(),transport=t)
  w=PendingWindow(self.cfg.from_date,self.cfg.to_date,page_size=2)
  actual=_request_media_resume(self.cfg,c,self.cfg.targets[0],w,['.jpg'])
  self.assertEqual(actual,(rows[:1],False,'RECOVERED'));c.close()
 def test_old_completed_inventory_retraverses_without_resetting_saved_rows(self):
  tid=get_or_create_media_target(self.db,self.cfg.targets[0]);sig=media_query_signature(self.cfg)
  with self.db:
   _save_media_state(self.db,tid,2001,media_index_state_signature(self.cfg,revision=7),None,True,1,None)
   upsert_media_captures(self.db,[(row('http://phoenixtv.com/a.jpg'),'image','.jpg')],tid,sig)
   self.db.execute("UPDATE media_captures SET state='downloaded',path='keep.jpg'")
  before=tuple(self.db.execute('SELECT id,state,path FROM media_captures').fetchone())
  t=Transport([body([]),body([])]*3)
  with self.use(t):index_media(self.cfg,self.db,threading.Event())
  self.assertEqual(tuple(self.db.execute('SELECT id,state,path FROM media_captures').fetchone()),before)
