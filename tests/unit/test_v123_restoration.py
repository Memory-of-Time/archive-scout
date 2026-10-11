from __future__ import annotations

import hashlib
import gc
import json
import os
import tempfile
import subprocess
import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import HttpClient, TransientRequestError
from archive_scout.cdx.indexer import index_archive, _adopt_compatible_index_state
from archive_scout.cdx.parameters import cdx_query_signature, cdx_query_signatures
from archive_scout.config import ProjectConfig, NetworkConfig, load_project_config, save_project_config
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_capture
from archive_scout.downloads.downloader import capture_path
from archive_scout.downloads.rate_limit import FixedRateLimiter
from archive_scout.events import Stopped
from archive_scout.network.transports import TransportResponse
from archive_scout.scanning.hitlist import search_with_hitlist
from archive_scout.ui.dashboard import read_dashboard_counts

HEADER = ['timestamp', 'original', 'mimetype', 'statuscode', 'digest', 'length']


def capture(database, root, target, number, body=None, kind='text'):
    url=f'http://example.org/{number}'
    timestamp='20010101000000'
    upsert_capture(database, dict(zip(HEADER,[timestamp,url,'text/html','200',str(number),'100'])),target,'fixture')
    row=database.execute('SELECT id FROM captures WHERE original_url=?',(url,)).fetchone()
    capture_id=int(row[0])
    path=capture_path(root,capture_id,timestamp,url)
    if body is not None:
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(body)
        database.execute("UPDATE captures SET state='downloaded_unscanned' WHERE id=?",(capture_id,))
    database.execute("INSERT INTO capture_routing(capture_id,resource_class,evidence,confident,routing,updated_at) VALUES(?,?, 'fixture',1,'downloaded_awaiting_scan','fixture')",(capture_id,kind))
    database.commit()
    return capture_id,path


class HitlistRestorationTests(unittest.TestCase):
    def test_existing_imported_source_outside_project_remains_searchable_in_place(self):
        from archive_scout.projects.importers import import_text_folder
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'project'; source=Path(temp)/'source'
            root.mkdir(); source.mkdir()
            path=source/'saved.html'; body=b'<html><body>imported needle</body></html>'
            path.write_bytes(body)
            database=open_database(root)
            try:
                import_text_folder(root,source,database,threading.Event())
                result=search_with_hitlist(root,database,['imported needle'],threading.Event())
                self.assertEqual(result['matches'],1)
                self.assertEqual(result['local_checked'],1)
                self.assertEqual(path.read_bytes(),body)
                stored_path = database.execute('SELECT path FROM documents').fetchone()[0]
                # Windows 8.3 aliases and macOS /var -> /private/var are the same file.
                # Verify physical identity rather than imposing one path spelling.
                self.assertTrue(os.path.samefile(stored_path, path),
                                f'Imported document is not the original source: {stored_path!r}')
            finally:
                database.close()

    def test_full_saved_unscanned_body_and_html_visible_text_are_searchable(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); database=open_database(root)
            try:
                target=get_or_create_target(database,'example.org/*')
                body=b'<html><body>'+b'a '*300000+b'late <b>needle</b> &amp; evidence</body></html>'
                _,path=capture(database,root,target,1,body)
                before=hashlib.sha256(path.read_bytes()).hexdigest()
                result=search_with_hitlist(root,database,['late needle','& evidence'],threading.Event())
                self.assertEqual((result['indexed_checked'],result['local_checked'],result['matches']),(1,1,1))
                hits=database.execute('SELECT keyword,fields FROM quick_search_hits ORDER BY keyword').fetchall()
                self.assertEqual(len(hits),2)
                self.assertIn('rendered',str([tuple(row) for row in hits]))
                self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),before)
                self.assertEqual(database.execute('SELECT COUNT(*) FROM documents').fetchone()[0],0)
            finally:
                database.close()

    def test_resume_verifies_bytes_and_freezes_inventory_boundary(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); database=open_database(root)
            try:
                target=get_or_create_target(database,'example.org/*')
                first,path=capture(database,root,target,1,b'needle one')
                capture(database,root,target,2,b'needle two')
                stop=threading.Event()
                def pause(event):
                    if event.stage=='hitlist' and event.current==1:
                        stop.set()
                with self.assertRaises(Stopped):
                    search_with_hitlist(root,database,['needle'],stop,pause,batch_size=1)
                stat=path.stat(); path.write_bytes(b'absent one'); os.utime(path,ns=(stat.st_atime_ns,stat.st_mtime_ns))
                capture(database,root,target,3,b'needle new')
                result=search_with_hitlist(root,database,['needle'],threading.Event(),batch_size=1)
                self.assertEqual((result['indexed_checked'],result['matches']),(2,1))
                self.assertEqual(database.execute('SELECT COUNT(*) FROM quick_search_hits WHERE capture_id=? AND run_id=?',(first,result['run_id'])).fetchone()[0],0)
                fresh=search_with_hitlist(root,database,['needle'],threading.Event(),batch_size=1)
                self.assertEqual((fresh['indexed_checked'],fresh['matches']),(3,2))
            finally:
                database.close()

    def test_url_matches_include_missing_bodies_but_binary_bodies_are_excluded(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); database=open_database(root)
            try:
                target=get_or_create_target(database,'example.org/*')
                capture(database,root,target,'needle-missing')
                capture(database,root,target,'image',b'needle in binary',kind='image')
                before=[tuple(row) for row in database.execute('SELECT * FROM capture_routing ORDER BY capture_id')]
                result=search_with_hitlist(root,database,['needle'],threading.Event())
                self.assertEqual((result['matches'],result['non_text'],result['missing']),(1,1,1))
                self.assertEqual([tuple(row) for row in database.execute('SELECT * FROM capture_routing ORDER BY capture_id')],before)
                self.assertEqual(database.execute('SELECT fields FROM quick_search_hits').fetchone()[0],'url')
            finally:
                database.close()

    def test_operation_and_cli_mode_have_no_network_or_scan_jobs(self):
        from archive_scout.operations import run_project
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); database=open_database(root)
            try:
                target=get_or_create_target(database,'example.org/*')
                capture(database,root,target,1,b'needle')
            finally:
                database.close()
            config=ProjectConfig(root,hitlist_keywords=['needle'],targets=[],keywords=[])
            with patch.object(HttpClient,'get',side_effect=AssertionError('Hitlist used the network')):
                result=run_project(config,'hitlist')
            self.assertTrue(result['hitlist_csv'].exists())
            database=open_database(root)
            try:
                self.assertEqual(database.execute('SELECT COUNT(*) FROM scan_runs').fetchone()[0],0)
                self.assertEqual(database.execute('SELECT status FROM operation_runs').fetchone()[0],'complete')
            finally:
                database.close()


class IndexRestorationTests(unittest.TestCase):
    def test_six_isolated_page_timeouts_recover_without_manual_resume_or_repeating_good_pages(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            config=ProjectConfig(root,['example.org/*'],[],from_date='20010101',to_date='20010101',cdx_delay=0,
                                 network=NetworkConfig(index_strategy='paged',page_blocks=2,cdx_workers=3)).normalized()
            database=open_database(root)
            calls={}
            def response(_client,_urls,params,**kwargs):
                values=dict(params)
                if values.get('showNumPages')=='true':
                    return 3
                page=int(values['page']); calls[page]=calls.get(page,0)+1
                if page==1 and calls[page]<=6:
                    raise TransientRequestError('temporary replay inventory read timeout',timed_out=True)
                return [HEADER,['20010101000000',f'http://example.org/{page}','text/html','200',str(page),'10']]
            try:
                with patch.object(HttpClient,'get_cdx_any',new=response):
                    index_archive(config,database,threading.Event())
                self.assertEqual(calls,{0:1,1:7,2:1})
                self.assertEqual(database.execute('SELECT COUNT(*) FROM captures').fetchone()[0],3)
                self.assertEqual(database.execute('SELECT COUNT(*) FROM index_pages WHERE status="complete"').fetchone()[0],3)
            finally:
                database.close()

    def test_new_range_query_is_one_request_across_years_and_complete_work_is_reused(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); database=open_database(root)
            try:
                config=ProjectConfig(root,keywords=[],targets=['example.org/*'],from_date='2001',to_date='2010',cdx_delay=0).normalized()
                with patch.object(HttpClient,'get_cdx_any',return_value=[]) as getter:
                    index_archive(config,database,threading.Event())
                    index_archive(config,database,threading.Event())
                self.assertEqual(getter.call_count,1)
                params=dict(getter.call_args.args[1])
                self.assertEqual((params['from'],params['to']),('20010101000000','20101231235959'))
                self.assertEqual(database.execute('SELECT complete FROM index_coverage').fetchone()[0],1)
                self.assertEqual(database.execute('SELECT COUNT(*) FROM index_state').fetchone()[0],0)
            finally:
                database.close()

    def test_old_config_uses_original_year_collapse_and_roundtrips_settings(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); path=root/'project.json'
            path.write_text(json.dumps({'version':'1.2.2','output_dir':temp,'targets':['example.org/*'],'from_date':'2001','to_date':'2002','cdx_filters':['statuscode:200','mimetype:text/.*'],'cdx_collapses':['urlkey'],'network':{'backend':'urllib3','trust_environment':False}}))
            config=load_project_config(path)
            self.assertEqual(config.text_collapse_scope,'year')
            save_project_config(config)
            reloaded=load_project_config(path)
            self.assertEqual(reloaded.text_collapse_scope,'year')
            self.assertEqual((reloaded.cdx_filters,reloaded.cdx_collapses,reloaded.network.backend,reloaded.network.trust_environment),(config.cdx_filters,config.cdx_collapses,'urllib3',False))

    def test_signature_adoption_retains_colliding_capture_ids_and_routing(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); database=open_database(root)
            try:
                config=ProjectConfig(root,keywords=[],targets=['example.org/*'],from_date='2001',to_date='2001',text_collapse_scope='year').normalized()
                target=get_or_create_target(database,config.targets[0])
                signature=cdx_query_signature(config)
                old=next(value for value in cdx_query_signatures(config) if value!=signature)
                row=dict(zip(HEADER,['20010101000000','http://example.org/','text/html','200','hash','12']))
                upsert_capture(database,row,target,old); upsert_capture(database,row,target,signature)
                database.execute("INSERT INTO index_state(target_id,year,query_signature,complete,seen,updated_at) VALUES(?,2001,?,1,1,'fixture')",(target,old))
                before=[tuple(row) for row in database.execute('SELECT id,query_signature FROM captures ORDER BY id')]
                _adopt_compatible_index_state(database,target,2001,config,signature)
                self.assertEqual([tuple(row) for row in database.execute('SELECT id,query_signature FROM captures ORDER BY id')],before)
            finally:
                database.close()

    def test_numbered_json_failure_does_not_make_a_text_request(self):
        class Transport:
            calls=[]
            def request(self,url,headers,max_bytes,stop_event):
                self.calls.append(url)
                return TransportResponse(data=b'<html>service problem</html>',status=200,headers={'Content-Type':'text/html'},final_url=url,backend='fixture',elapsed=0)
            def close(self):
                pass
        transport=Transport()
        client=HttpClient(FixedRateLimiter(0),1,5,'fixture',threading.Event(),transport=transport)
        try:
            with self.assertRaises(TransientRequestError):
                client.get_cdx_json_rows_any(('https://web.archive.org/web/timemap/json',),[('output','json'),('page','1')])
            self.assertEqual(len(transport.calls),1)
            self.assertIn('output=json',transport.calls[0])
        finally:
            client.close()


class MigrationRestorationTests(unittest.TestCase):
    def test_schema8_upgrade_is_additive_and_keeps_payload_routing(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); database=open_database(root)
            target=get_or_create_target(database,'example.org/*')
            capture(database,root,target,1,b'needle')
            before=[tuple(row) for row in database.execute('SELECT * FROM capture_routing')]
            for name in ('quick_search_coverage','quick_search_hits','quick_search_runs','index_pages','index_coverage'):
                database.execute('DROP TABLE '+name)
            database.execute('UPDATE schema_info SET version=8'); database.commit(); database.close()
            database=open_database(root)
            try:
                self.assertEqual(database.execute('SELECT version FROM schema_info').fetchone()[0],9)
                self.assertEqual([tuple(row) for row in database.execute('SELECT * FROM capture_routing')],before)
                self.assertEqual(database.execute('SELECT COUNT(*) FROM captures').fetchone()[0],1)
                result=search_with_hitlist(root,database,['needle'],threading.Event())
                self.assertEqual(result['matches'],1)
                counts=read_dashboard_counts(root/'archive_scout.sqlite3')
                self.assertEqual(counts['downloaded_unscanned'],1)
                self.assertEqual(counts['documents'],1)
            finally:
                database.close()


class GuiRestorationTests(unittest.TestCase):
    def test_all_pages_config_hitlist_theme_borders_and_readonly_evidence(self):
        if os.environ.get('ARCHIVE_SCOUT_GUI_TEST_CHILD') != '1':
            # Isolate Tcl lifetime from the HTTP fault tests' worker threads.
            gc.collect()
            environment=dict(os.environ,ARCHIVE_SCOUT_GUI_TEST_CHILD='1')
            result=subprocess.run([sys.executable,'-m','unittest',
                'tests.unit.test_v123_restoration.GuiRestorationTests.'+self._testMethodName,'-v'],
                cwd=Path(__file__).resolve().parents[2],env=environment,
                text=True,capture_output=True,timeout=60)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            if 'skipped=1' in result.stderr:
                self.skipTest('No desktop display available')
            gc.collect()
            return
        import tkinter as tk
        from archive_scout.ui.main_window import ArchiveScoutApp
        with tempfile.TemporaryDirectory() as temp:
            with patch('archive_scout.ui.main_window.app_support_dir',return_value=Path(temp)),patch.object(ArchiveScoutApp,'show_welcome'):
                try:
                    app=ArchiveScoutApp()
                except tk.TclError as exc:
                    if 'display' in str(exc).casefold():
                        self.skipTest(str(exc))
                    raise
                try:
                    app.withdraw(); app.update_idletasks()
                    app.hitlist_text.insert('1.0','needle\nother')
                    app.dashboard_refresh_mode_var.set('manual')
                    app.dashboard_eta_enabled_var.set(True)
                    config=app.build_config(False)
                    self.assertEqual(config.hitlist_keywords,['needle','other'])
                    self.assertTrue(config.dashboard_eta_enabled)
                    self.assertNotIn('adaptive_rate_limiting',config.to_payload())
                    config.text_collapse_scope='year'
                    config.output_dir=Path(temp)
                    config.download_external_redirects=True
                    config.read_timeout=91
                    config.user_agent='saved-project-agent'
                    config.network.connection_retry_seconds=7
                    config.network.diagnostics=False
                    config.ai.request_timeout=73
                    config.research.candidate_limit=321
                    app.apply_config(config)
                    saved=app.build_config(False)
                    self.assertEqual(saved.text_collapse_scope,'year')
                    self.assertTrue(saved.download_external_redirects)
                    self.assertEqual(saved.read_timeout,91)
                    self.assertEqual(saved.user_agent,'saved-project-agent')
                    self.assertEqual(saved.network.connection_retry_seconds,7)
                    self.assertFalse(saved.network.diagnostics)
                    self.assertEqual(saved.ai.request_timeout,73)
                    self.assertEqual(saved.research.candidate_limit,321)
                    self.assertEqual(len(app.notebook.tabs()),14)
                    for tab in app.notebook.tabs():
                        app.notebook.select(tab); app.update_idletasks()
                    for name in ('hitlist_text','targets_text','keywords_text','result_snippets_text','ai_detail_text','research_detail_text'):
                        self.assertGreaterEqual(int(getattr(app,name).cget('highlightthickness')),1)
                    app._set_readonly_text(app.result_snippets_text,'evidence')
                    self.assertEqual(app.result_snippets_text.get('1.0','end-1c'),'evidence')
                    self.assertEqual(app.result_snippets_text.cget('state'),'disabled')
                    database=open_database(Path(temp))
                    database.execute("INSERT INTO keyword_sets(name,fingerprint,keywords_json,created_at,updated_at) VALUES('fixture','fixture','[]','fixture','fixture')")
                    database.executemany("INSERT INTO scan_runs(keyword_set_id,name,status,started_at,source_operation) VALUES(1,?,'completed','fixture','fixture')",[(f'scan {n}',) for n in range(105)])
                    database.executemany("INSERT INTO errors(operation,category,message,first_seen,last_seen,resolved,ignored) VALUES('download','fixture',?,'fixture','fixture',0,0)",[(f'cause {n}',) for n in range(105)])
                    database.execute("INSERT INTO errors(operation,category,message,first_seen,last_seen,resolved) VALUES('download','fixture','resolved cause','fixture','fixture',1)")
                    database.commit(); database.close()
                    app.refresh_history(reset_page=True)
                    self.assertEqual(len(app.history_tree.get_children()),100)
                    app.next_history_page()
                    self.assertEqual(len(app.history_tree.get_children()),5)
                    app.history_tree.selection_set(app.history_tree.get_children()[0])
                    app.load_selected_history_detail()
                    self.assertIn('Keyword set: fixture',app.history_detail_text.get('1.0','end'))
                    app.refresh_errors(reset_page=True)
                    self.assertEqual(len(app.errors_tree.get_children()),100)
                    app.next_error_page()
                    self.assertEqual(len(app.errors_tree.get_children()),5)
                    app.error_status_filter_var.set('Resolved')
                    app.refresh_errors(reset_page=True)
                    self.assertEqual(len(app.errors_tree.get_children()),1)
                    app.errors_tree.selection_set(app.errors_tree.get_children()[0])
                    app.load_selected_error_detail()
                    self.assertIn('resolved cause',app.error_detail_text.get('1.0','end'))
                    self.assertEqual(app.error_detail_text.cget('state'),'disabled')
                finally:
                    for job in app.tk.splitlist(app.tk.call('after','info')):
                        app.after_cancel(job)
                    app.destroy()
                    # Tk cycles must be finalized on the creating thread before
                    # subsequent tests start HTTP worker threads.
                    del app
                    gc.collect()


if __name__=='__main__':
    unittest.main()
