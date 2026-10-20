"""Full selected-range enumeration, safe legacy migration and resume regressions."""
from __future__ import annotations
import json,tempfile,threading,unittest
from pathlib import Path
from unittest.mock import patch
from archive_scout.config import ProjectConfig,NetworkConfig,MediaConfig,load_project_config,save_project_config
from archive_scout.cdx.client import HttpClient,CDXRows,TransientRequestError
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.cdx.indexer import (index_archive,IndexPlan,PendingWindow,encode_plan,decode_plan,
    save_state,save_coverage_state,coalesce_untouched_range_plan,window_label)
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target,get_or_create_media_target,upsert_captures
from archive_scout.events import Stopped
from archive_scout.media.indexer import (index_media,_save_media_state,media_index_state_signature)

START='20050101000000';END='20081231235959'
def config(root,**kw):
 return ProjectConfig(Path(root),targets=['example.com/*'],keywords=['needle'],
   from_date=START,to_date=END,cdx_delay=0,network=NetworkConfig(index_strategy='resume'),**kw).normalized()
def row(number=1):
 return ('20060101000000',f'http://example.com/{number}.jpg','image/jpeg','200',str(number),'100')

class FullRangeIndexing(unittest.TestCase):
 def test_multi_year_labels_never_claim_one_year_or_month(self):
  self.assertEqual(window_label(START,END),'2005-01-01–2008-12-31')
  self.assertEqual(window_label(START,'20060131235959'),'2005-01-01–2006-01-31')
  self.assertEqual(window_label(START,'20051231235959'),'2005')
  self.assertEqual(window_label(START,'20050131235959'),'2005-01')

 def test_known_gateway_split_is_not_reassembled_on_restart(self):
  plan=IndexPlan([PendingWindow(START,'20061231235959'),PendingWindow('20070101000000',END)],0,2,1)
  self.assertFalse(coalesce_untouched_range_plan(plan,START,END,0))

 def test_legacy_empty_failed_years_upgrade_and_saved_choice_roundtrips(self):
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);db=open_database(root);cfg=config(root,text_collapse_scope='year')
   tid=get_or_create_target(db,cfg.targets[0]);sig=cdx_query_signature(cfg)
   with db:
    for year in range(2005,2009):
     plan=IndexPlan([PendingWindow(f'{year}0101000000',f'{year}1231235959',failures=30)],0,1)
     save_state(db,tid,year,sig,encode_plan(plan),False,0,None)
   db.close()
   path=root/'project.json';path.write_text(json.dumps({'version':'1.2.4','output_dir':temp,'targets':cfg.targets,'from_date':START,'to_date':END,'text_collapse_scope':'year'}))
   loaded=load_project_config(path);self.assertEqual(loaded.text_collapse_scope,'range')
   save_project_config(loaded);self.assertEqual(load_project_config(path).text_collapse_scope,'range')
   loaded.text_collapse_scope='year';save_project_config(loaded)
   self.assertEqual(load_project_config(path).text_collapse_scope,'year')

 def test_future_version_missing_scope_does_not_fall_back_to_year(self):
  with tempfile.TemporaryDirectory() as temp:
   root=Path(temp);cfg=config(root);db=open_database(root);tid=get_or_create_target(db,cfg.targets[0])
   with db:upsert_captures(db,[row()],tid,cdx_query_signature(cfg))
   db.close();path=root/'project.json';path.write_text(json.dumps({'version':'1.2.5','output_dir':temp,'targets':cfg.targets}))
   self.assertEqual(load_project_config(path).text_collapse_scope,'range')

 def test_progressed_or_opaque_legacy_state_is_not_reset(self):
  for saved,seen in [('old-opaque-cursor',0),(encode_plan(IndexPlan([PendingWindow(START,END,'exact')],0,1)),0),(None,1)]:
   with self.subTest(saved=saved),tempfile.TemporaryDirectory() as temp:
    root=Path(temp);db=open_database(root);cfg=config(root,text_collapse_scope='year');tid=get_or_create_target(db,cfg.targets[0])
    with db:save_state(db,tid,2005,cdx_query_signature(cfg),saved,False,seen,None)
    db.close();path=root/'project.json';path.write_text(json.dumps({'version':'1.2.2','output_dir':temp,'text_collapse_scope':'year'}))
    self.assertEqual(load_project_config(path).text_collapse_scope,'year')

 def test_all_years_resume_after_stop_without_restarting_lower_bound(self):
  with tempfile.TemporaryDirectory() as temp:
   cfg=config(temp);db=open_database(cfg.output_dir);stop=threading.Event();calls=[]
   def first(_client,endpoints,params,**kwargs):
    calls.append(dict(params));stop.set();return CDXRows([row(1)],'opaque+cursor%2F')
   with patch.object(HttpClient,'get_cdx_any',first):
    with self.assertRaises(Stopped):index_archive(cfg,db,stop)
   self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],1)
   db.close();db=open_database(cfg.output_dir)
   def second(_client,endpoints,params,**kwargs):calls.append(dict(params));return CDXRows([row(2)])
   with patch.object(HttpClient,'get_cdx_any',second):index_archive(cfg,db,threading.Event())
   with patch.object(HttpClient,'get_cdx_any',side_effect=AssertionError('completed range fetched again')):index_archive(cfg,db,threading.Event())
   self.assertEqual(len(calls),2)
   self.assertEqual([p.get('resumeKey') for p in calls],[None,'opaque+cursor%2F'])
   self.assertTrue(all((p['from'],p['to'],p['limit'])==(START,END,'100000') for p in calls))
   self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],2)
   self.assertEqual(db.execute('SELECT COUNT(*) FROM index_state').fetchone()[0],0)
   db.close()

 def test_untouched_inherited_year_windows_reassemble_before_request(self):
  with tempfile.TemporaryDirectory() as temp:
   cfg=config(temp);db=open_database(cfg.output_dir);tid=get_or_create_target(db,cfg.targets[0]);sig=cdx_query_signature(cfg)
   plan=IndexPlan([PendingWindow(f'{y}0101000000',f'{y}1231235959',failures=99) for y in range(2005,2009)],0,4)
   with db:save_coverage_state(db,tid,sig,START,END,encode_plan(plan),False,0,None,'resume','')
   calls=[]
   def respond(_client,endpoints,params,**kwargs):calls.append(dict(params));return CDXRows([])
   with patch.object(HttpClient,'get_cdx_any',respond):index_archive(cfg,db,threading.Event())
   self.assertEqual([(c['from'],c['to']) for c in calls],[(START,END)])
   self.assertEqual(db.execute('SELECT complete FROM index_coverage').fetchone()[0],1);db.close()

 def test_reassembly_rejects_any_committed_layout_or_cursor(self):
  for extra in ({'resume_key':'cursor'},{'page':1},{'page_count':8},{'page_layout':'layout'},{'retry_pages':[0]}):
   with self.subTest(extra=extra):
    plan=IndexPlan([PendingWindow('20050101000000','20051231235959',**extra),PendingWindow('20060101000000',END)],0,2)
    self.assertFalse(coalesce_untouched_range_plan(plan,START,END,0))
  plan=IndexPlan([PendingWindow(START,'20051231235959'),PendingWindow('20060101000000',END)],0,2)
  self.assertFalse(coalesce_untouched_range_plan(plan,START,END,1))

 def test_failed_full_range_recovers_without_annual_fallback(self):
  with tempfile.TemporaryDirectory() as temp:
   cfg=config(temp);db=open_database(cfg.output_dir);calls=[]
   def respond(_client,endpoints,params,**kwargs):
    calls.append(dict(params))
    if len(calls)==1:raise TransientRequestError('HTTP 504',status=504)
    return CDXRows([row()])
   with patch.object(HttpClient,'get_cdx_any',respond),patch('archive_scout.cdx.indexer.transient_backoff',return_value=0):
    index_archive(cfg,db,threading.Event())
   self.assertEqual(len(calls),2);self.assertTrue(all((p['from'],p['to'])==(START,END) for p in calls));db.close()

 def test_multiple_failure_rounds_keep_each_full_range_cursor(self):
  with tempfile.TemporaryDirectory() as temp:
   cfg=config(temp);cfg.targets=['example.com/*','other.com/*'];db=open_database(cfg.output_dir);calls={t:[] for t in cfg.targets}
   def respond(_client,endpoints,params,**kwargs):
    p=dict(params);target=p['url'];calls[target].append(p);n=len(calls[target])
    if n==1:return CDXRows([('20060101000000','http://'+target.removesuffix('/*')+'/a','text/html','200','A','10')],'saved-'+target)
    if n<=3:raise TransientRequestError('HTTP 504',status=504)
    return CDXRows([])
   with patch.object(HttpClient,'get_cdx_any',respond),patch('archive_scout.cdx.indexer.transient_backoff',return_value=0):index_archive(cfg,db,threading.Event())
   for target,requests in calls.items():
    self.assertEqual(len(requests),4)
    self.assertEqual([p.get('resumeKey') for p in requests],[None]+['saved-'+target]*3)
    self.assertTrue(all((p['from'],p['to'])==(START,END) for p in requests))
   self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],2)
   self.assertEqual(db.execute('SELECT COUNT(*) FROM index_coverage WHERE complete=0').fetchone()[0],0);db.close()

 def test_rotating_fragmented_task_does_not_double_count_progress(self):
  with tempfile.TemporaryDirectory() as temp:
   cfg=config(temp,cdx_collapses=[]);cfg.targets=['example.com/*','other.com/*'];cfg=cfg.normalized();db=open_database(cfg.output_dir)
   tid=get_or_create_target(db,cfg.targets[0]);sig=cdx_query_signature(cfg)
   windows=[PendingWindow(START,'20051231235959'),PendingWindow('20060101000000','20061231235959'),PendingWindow('20070101000000',END)]
   with db:save_coverage_state(db,tid,sig,START,END,encode_plan(IndexPlan(windows,0,3)),False,1,None,'resume','')
   failed=False;events=[]
   def respond(_client,endpoints,params,**kwargs):
    nonlocal failed
    p=dict(params)
    if p['url']=='example.com/*' and p['from'].startswith('2006') and not failed:
     failed=True;raise TransientRequestError('HTTP 504',status=504)
    return CDXRows([])
   with patch.object(HttpClient,'get_cdx_any',respond),patch('archive_scout.cdx.indexer.transient_backoff',return_value=0):index_archive(cfg,db,threading.Event(),events.append)
   done=[e for e in events if e.detail and e.detail.get('phase')=='inventory complete']
   self.assertTrue(done);self.assertEqual((done[-1].current,done[-1].total),(4,4));db.close()

class MediaFullRangeIndexing(unittest.TestCase):
 def test_multi_year_media_uses_full_range_and_exact_resume(self):
  with tempfile.TemporaryDirectory() as temp:
   cfg=config(temp,media=MediaConfig(enabled=True,include_extensions=['jpg'],discover_embedded=False));db=open_database(cfg.output_dir);calls=[]
   def respond(_client,endpoints,params,**kwargs):
    calls.append(dict(params));return CDXRows([row(len(calls))], 'media-cursor' if len(calls)==1 else None)
   with patch.object(HttpClient,'get_cdx_any',respond):index_media(cfg,db,threading.Event())
   self.assertEqual(len(calls),2);self.assertTrue(all((p['from'],p['to'])==(START,END) for p in calls))
   self.assertEqual([p.get('resumeKey') for p in calls],[None,'media-cursor'])
   self.assertEqual(db.execute('SELECT year,complete FROM media_index_state').fetchone()[:],(0,1))
   events=[]
   with patch.object(HttpClient,'get_cdx_any',side_effect=AssertionError('complete media range repeated')):index_media(cfg,db,threading.Event(),events.append)
   self.assertTrue(any('2005-01-01–2008-12-31' in e.message and 'Already indexed' in e.message for e in events))
   db.close()

 def test_media_range_restarts_from_exact_saved_cursor(self):
  with tempfile.TemporaryDirectory() as temp:
   cfg=config(temp,media=MediaConfig(enabled=True,include_extensions=['jpg'],discover_embedded=False));db=open_database(cfg.output_dir);stop=threading.Event();calls=[]
   def first(_client,endpoints,params,**kwargs):
    calls.append(dict(params));stop.set();return CDXRows([row()], 'range-media-checkpoint')
   with patch.object(HttpClient,'get_cdx_any',first):
    with self.assertRaises(Stopped):index_media(cfg,db,stop)
   db.close();db=open_database(cfg.output_dir)
   def second(_client,endpoints,params,**kwargs):calls.append(dict(params));return CDXRows([row(2)])
   with patch.object(HttpClient,'get_cdx_any',second):index_media(cfg,db,threading.Event())
   self.assertEqual([p.get('resumeKey') for p in calls],[None,'range-media-checkpoint'])
   self.assertTrue(all((p['from'],p['to'])==(START,END) for p in calls))
   self.assertEqual(db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0],2);db.close()

 def test_empty_annual_media_failure_plans_upgrade_to_one_range(self):
  with tempfile.TemporaryDirectory() as temp:
   cfg=config(temp,media=MediaConfig(enabled=True,include_extensions=['jpg'],discover_embedded=False));db=open_database(cfg.output_dir);tid=get_or_create_media_target(db,cfg.targets[0]);sig=media_index_state_signature(cfg)
   with db:
    for year in range(2005,2009):_save_media_state(db,tid,year,sig,encode_plan(IndexPlan([PendingWindow(f'{year}0101000000',f'{year}1231235959',failures=40)],0,1)),False,0,None)
   calls=[]
   def respond(_client,endpoints,params,**kwargs):calls.append(dict(params));return CDXRows([])
   with patch.object(HttpClient,'get_cdx_any',respond):index_media(cfg,db,threading.Event())
   self.assertEqual([(p['from'],p['to']) for p in calls],[(START,END)])
   self.assertEqual(db.execute('SELECT COUNT(*) FROM media_index_state WHERE complete=0').fetchone()[0],0);db.close()

 def test_target_date_override_keeps_global_media_state_identity(self):
  with tempfile.TemporaryDirectory() as temp:
   cfg=config(temp,media=MediaConfig(enabled=True,include_extensions=['jpg'],discover_embedded=False))
   cfg.target_settings={'example.com/*':{'from_date':'20060101000000'}};cfg=cfg.normalized()
   db=open_database(cfg.output_dir);tid=get_or_create_media_target(db,cfg.targets[0]);sig=media_index_state_signature(cfg)
   with db:_save_media_state(db,tid,2006,sig,encode_plan(IndexPlan([PendingWindow('20060101000000','20061231235959','override-cursor')],0,1)),False,1,None)
   calls=[]
   def respond(_client,endpoints,params,**kwargs):calls.append(dict(params));return CDXRows([])
   with patch.object(HttpClient,'get_cdx_any',respond):index_media(cfg,db,threading.Event())
   self.assertIsNone(calls[0].get('resumeKey'))
   self.assertEqual(calls[0]['from'],'20060101000000');self.assertEqual(calls[0]['to'],END);self.assertEqual(len(calls),1)
   self.assertEqual(db.execute('SELECT COUNT(*) FROM media_index_state WHERE year=2006 AND complete=0').fetchone()[0],1);db.close()

 def test_committed_annual_media_cursor_is_preserved(self):
  with tempfile.TemporaryDirectory() as temp:
   cfg=config(temp,media=MediaConfig(enabled=True,include_extensions=['jpg'],discover_embedded=False));db=open_database(cfg.output_dir);tid=get_or_create_media_target(db,cfg.targets[0]);sig=media_index_state_signature(cfg)
   with db:_save_media_state(db,tid,2005,sig,encode_plan(IndexPlan([PendingWindow(START,'20051231235959','annual-exact')],0,1)),False,1,None)
   calls=[]
   def respond(_client,endpoints,params,**kwargs):calls.append(dict(params));return CDXRows([])
   with patch.object(HttpClient,'get_cdx_any',respond):index_media(cfg,db,threading.Event())
   self.assertIsNone(calls[0].get('resumeKey'));self.assertEqual(calls[0]['to'],END);self.assertEqual(len(calls),1)
   annual=db.execute('SELECT resume_key,complete FROM media_index_state WHERE year=2005').fetchone()
   self.assertEqual(decode_plan(annual['resume_key'],[(START,'20051231235959')]).pending[0].resume_key,'annual-exact')
   self.assertEqual(annual['complete'],0)
   self.assertEqual(db.execute('SELECT complete FROM media_index_state WHERE year=0').fetchone()[0],1);db.close()

if __name__=='__main__':unittest.main()
