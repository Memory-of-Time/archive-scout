from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import Mock, patch

from archive_scout.cdx.client import HttpClient, MalformedCDXResponse
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import MediaConfig, NetworkConfig, ProjectConfig, ReportConfig, ResearchConfig, project_config_from_payload
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, get_or_create_keyword_set, start_scan_run, upsert_captures, upsert_document, queue_media_discovery_candidates
from archive_scout.downloads.downloader import download_archive
from archive_scout.downloads.rate_limit import FixedRateLimiter
from archive_scout.media.discovery import discover_media
from archive_scout.media.embedded_pages import index_all_embedded_pages
from archive_scout.media.indexer import media_query_signature
from archive_scout.network.transports import CdxResponseTimeout, IncompleteRepresentationError, ResilientTransport, TransportExhaustedError, _read_limited
from archive_scout.operations import run_project
from archive_scout.research.search import search_research


class ProjectFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = ProjectConfig(self.root, ['example.org/*'], ['needle'], from_date='2008', to_date='2008',
            cdx_delay=0, download_delay=0, workers=2, cdx_collapses=[],
            network=NetworkConfig(trust_environment=False),
            report=ReportConfig(outputs=['matched_urls'])).normalized()
        self.db = open_database(self.root)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def seed(self):
        body = b'<html><title>Needle</title><p>needle archived evidence</p></html>'
        url = 'http://example.org/page'
        target = get_or_create_target(self.db, self.config.targets[0])
        upsert_captures(self.db, [('20080101000000',url,'text/html','200','',str(len(body)))],
                        target, cdx_query_signature(self.config))
        capture = self.db.execute('SELECT id FROM captures').fetchone()[0]
        path = self.root / 'page.html'; path.write_bytes(body)
        digest = hashlib.sha256(body).hexdigest()
        document = upsert_document(self.db, capture, path, 'Needle', 'needle archived evidence', [], digest, digest, len(body))
        self.db.commit()
        return capture, document


class ResearchOptInTests(ProjectFixture):
    def test_new_saved_and_example_projects_default_off(self):
        self.assertFalse(ResearchConfig().auto_build)
        self.assertFalse(project_config_from_payload({}, self.root/'project.json').research.auto_build)
        example = json.loads((Path(__file__).resolve().parents[2]/'examples/project.example.json').read_text())
        self.assertFalse(project_config_from_payload(example, self.root/'project.json').research.auto_build)
        enabled = replace(self.config, research=ResearchConfig(auto_build=True))
        self.assertTrue(project_config_from_payload(enabled.to_payload(), self.root/'project.json').research.auto_build)

    def test_normal_scan_skips_research_and_manual_build_and_search_work(self):
        self.seed()
        events = []
        with patch('archive_scout.operations.build_research_index', side_effect=AssertionError('automatic index was not requested')):
            result = run_project(self.config, 'rescan', callback=events.append)
        self.assertEqual(result.status, 'complete')
        self.assertTrue(any('indexing is off' in e.message for e in events))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM research_vectors').fetchone()[0],0)
        self.assertGreater(self.db.execute('SELECT COUNT(*) FROM document_matches').fetchone()[0],0)
        paths = run_project(self.config, 'research_index')
        self.assertTrue(paths['research_index'].is_file())
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM research_vectors').fetchone()[0],1)
        self.assertTrue(search_research(self.config,self.db,'needle'))

    def test_explicit_auto_build_remains_connected(self):
        self.seed()
        config = replace(self.config, research=ResearchConfig(auto_build=True))
        result = run_project(config, 'rescan')
        self.assertIn('research_index',result)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM research_vectors').fetchone()[0],1)


class DiscoveryTests(ProjectFixture):
    def test_repeat_candidates_do_not_write_or_reset_completed_queue(self):
        values=[('http://example.org/a.jpg',None,'embedded','image')]
        with self.db:
            self.assertEqual(queue_media_discovery_candidates(self.db,'sig',values),1)
            self.db.execute("UPDATE media_discovery_queue SET state='indexed',lookup_attempts=3,result_count=4")
        before=self.db.total_changes
        with self.db:
            self.assertEqual(queue_media_discovery_candidates(self.db,'sig',values*10),0)
        self.assertEqual(self.db.total_changes,before)
        row=self.db.execute('SELECT state,lookup_attempts,result_count FROM media_discovery_queue').fetchone()
        self.assertEqual(tuple(row),('indexed',3,4))

    def test_missing_hint_and_document_are_still_enriched(self):
        _, document=self.seed()
        with self.db:
            queue_media_discovery_candidates(self.db,'sig',[('http://example.org/a.jpg',None,'','')])
            queue_media_discovery_candidates(self.db,'sig',[('http://example.org/a.jpg',document,'embedded','image')])
        row=self.db.execute('SELECT source_document_id,source_type,kind_hint FROM media_discovery_queue').fetchone()
        self.assertEqual(tuple(row),(document,'embedded','image'))

    def test_duplicate_inputs_preserve_relative_base_and_hint_upgrade(self):
        media=MediaConfig().normalized()
        raw='<a href="opaque">link</a><img src="opaque"><video src="film%2Emp4"></video><img src="photo.jpg">'*20
        first=discover_media(raw,'http://example.org/one/page',media,['opaque'])
        self.assertEqual({(x.url,x.kind_hint) for x in first}, {
            ('http://example.org/one/opaque','image'),('http://example.org/one/film%2Emp4','video'),
            ('http://example.org/one/photo.jpg','image')})
        second=discover_media(raw,'http://example.org/two/page',media)
        self.assertTrue(all('/two/' in x.url for x in second))


class EmbeddedConcurrencyTests(ProjectFixture):
    def test_all_strategy_overlaps_exact_urls_and_keeps_smaller_pages(self):
        config=replace(self.config,media=MediaConfig(enabled=True,snapshot_strategy='all'),page_size=100000).normalized()
        signature=media_query_signature(config)
        with self.db:
            queue_media_discovery_candidates(self.db,signature,
                [(f'http://example.org/{i}.jpg',None,'embedded','image') for i in range(12)])
        lock=threading.Lock(); active=peak=0; limits=[]
        def fetch(cfg,client,url,window,*args,**kwargs):
            nonlocal active,peak
            with lock:
                active+=1;peak=max(peak,active);limits.append(window.page_size)
            time.sleep(.025)
            with lock:active-=1
            return [('20080101000000',url,'image/jpeg','200','','4')],True,None
        with patch('archive_scout.media.indexer._request_media_resume',side_effect=fetch):
            index_all_embedded_pages(config,self.db,Mock(),threading.Event(),None,signature)
        self.assertGreater(peak,1)
        self.assertLessEqual(peak,10)
        self.assertEqual(set(limits),{2000})
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0],12)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM media_discovery_queue WHERE state='indexed'").fetchone()[0],12)

    def test_saved_cursor_survives_failed_page_and_successful_resume(self):
        config=replace(self.config,media=MediaConfig(enabled=True,snapshot_strategy='all')).normalized()
        signature=media_query_signature(config)
        with self.db:
            queue_media_discovery_candidates(self.db,signature,[('http://example.org/a.jpg',None,'embedded','image')])
        qid=self.db.execute('SELECT id FROM media_discovery_queue').fetchone()[0]
        key=f'media_embed_cursor_v1:{signature}:{qid}'
        with self.db:self.db.execute('INSERT INTO project_meta VALUES(?,?)',(key,json.dumps({'resume_key':'KEPT','page_size':100000})))
        def failure(cfg,client,url,window,*args,**kwargs):
            self.assertEqual((window.resume_key,window.page_size),('KEPT',2000))
            raise CdxResponseTimeout('fixture active request deadline')
        with patch('archive_scout.media.indexer._request_media_resume',side_effect=failure):
            index_all_embedded_pages(config,self.db,Mock(),threading.Event(),None,signature)
        self.assertEqual(json.loads(self.db.execute('SELECT value FROM project_meta WHERE key=?',(key,)).fetchone()[0])['resume_key'],'KEPT')
        with patch('archive_scout.media.indexer._request_media_resume',return_value=([('20080101000000','http://example.org/a.jpg','image/jpeg','200','','4')],True,None)):
            index_all_embedded_pages(config,self.db,Mock(),threading.Event(),None,signature)
        self.assertIsNone(self.db.execute('SELECT value FROM project_meta WHERE key=?',(key,)).fetchone())
        self.assertEqual(self.db.execute('SELECT state FROM media_discovery_queue').fetchone()[0],'indexed')


class DeadlineTests(unittest.TestCase):
    def test_failed_urllib_response_is_closed_without_draining(self):
        from archive_scout.network.transports import Urllib3Backend
        response=Mock()
        Urllib3Backend._discard(response)
        response.close.assert_called_once()
        response.release_conn.assert_called_once()
        response.drain_conn.assert_not_called()

    def test_partial_deadline_body_is_never_returned(self):
        def chunks():
            yield b'first'
            yield b'second'
        with patch('archive_scout.network.transports.time.monotonic',side_effect=[0,11]):
            with self.assertRaises(CdxResponseTimeout):
                _read_limited(chunks(),100,threading.Event(),deadline=10)

    def test_incomplete_body_does_not_repeat_across_cdx_endpoints_or_formats(self):
        for method in ('get_cdx_any','get_cdx_rows_any','get_cdx_json_rows_any'):
            with self.subTest(method=method):
                transport=Mock()
                transport.request.side_effect=TransportExhaustedError('fixture',[('httpx',IncompleteRepresentationError('incomplete chunked read'))])
                client=HttpClient(FixedRateLimiter(0),1,1,'fixture',threading.Event(),transport=transport)
                try:
                    with self.assertRaises(MalformedCDXResponse):
                        getattr(client,method)(['http://example.org/cdx/search/cdx','http://example.org/web/timemap/json'],[('url','example.org/*')])
                    self.assertEqual(transport.request.call_count,1)
                finally:client.close()

    def test_active_budget_stops_trickling_cdx_on_available_backends(self):
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_GET(self):
                self.send_response(200);self.send_header('Content-Length','120');self.end_headers()
                try:
                    for _ in range(120):
                        self.wfile.write(b'x');self.wfile.flush();time.sleep(.03)
                except OSError:pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        server_thread=threading.Thread(target=server.serve_forever);server_thread.start()
        try:
            backends=['httpx','urllib3']+(['curl'] if shutil.which('curl') else [])
            for backend in backends:
                with self.subTest(backend=backend):
                    transport=ResilientTransport(pool_size=1,connect_timeout=1,read_timeout=1,mode=backend,trust_env=False)
                    start=time.monotonic()
                    try:
                        with self.assertRaises(TransportExhaustedError) as caught:
                            transport.request(f'http://127.0.0.1:{server.server_port}/cdx/search/cdx',{},1000,threading.Event())
                        self.assertTrue(caught.exception.timed_out)
                        self.assertFalse(caught.exception.connection_failed)
                        self.assertLess(time.monotonic()-start,2.8)
                    finally:transport.close()
        finally:
            server.shutdown();server_thread.join();server.server_close()


class AcquisitionTailTests(ProjectFixture):
    def test_completed_bodies_commit_before_slow_sibling_and_expose_owner_timings(self):
        target=get_or_create_target(self.db,self.config.targets[0])
        rows=[('20080101000000',f'http://example.org/{"slow" if i==0 else i}',
               'text/html','200','','100') for i in range(8)]
        with self.db:
            upsert_captures(self.db,rows,target,cdx_query_signature(self.config))
            key=get_or_create_keyword_set(self.db,'test',['needle'])
            run=start_scan_run(self.db,key,'test',1,'download')
        slow=self.db.execute("SELECT id FROM captures WHERE original_url LIKE '%/slow'").fetchone()[0]
        early=[]
        def fetch(url,*args,**kwargs):
            if url.endswith('/slow'):time.sleep(.25)
            return {'data':b'<html><p>needle evidence</p></html>','headers':{'content-type':'text/html'},'status':200,'final_url':url}
        def callback(event):
            if event.stage=='download' and event.detail and event.detail.get('new_saves')==1:
                early.append(self.db.execute('SELECT state FROM captures WHERE id=?',(slow,)).fetchone()[0])
        with patch.object(HttpClient,'get',side_effect=fetch):
            download_archive(self.config,self.db,run,threading.Event(),callback,allow_partial_inventory=True)
        self.assertEqual(early,['downloading'])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM captures WHERE state='downloaded'").fetchone()[0],8)
        health=json.loads((self.root/'diagnostics/text-acquisition/latest.json').read_text())
        self.assertEqual(set(health['acquisition']['owner_stages']),{'outcome','refill','callback','completed_wait'})


class ResearchTabTests(unittest.TestCase):
    def test_tab_exposes_optional_control_and_manual_build(self):
        import gc
        import os
        import subprocess
        import sys
        import weakref
        if os.environ.get('ARCHIVE_SCOUT_RESEARCH_TAB_TEST_CHILD') != '1':
            name='tests.unit.test_v126_pipeline_audit.ResearchTabTests.'+self._testMethodName
            result=subprocess.run([sys.executable,'-m','unittest',name,'-v'],
                cwd=Path(__file__).resolve().parents[2],
                env=dict(os.environ,ARCHIVE_SCOUT_RESEARCH_TAB_TEST_CHILD='1'),
                text=True,capture_output=True,timeout=60)
            self.assertEqual(result.returncode,0,result.stdout+result.stderr)
            if 'skipped=1' in result.stderr:
                self.skipTest('display is not available in the GUI child process')
            return
        import tkinter as tk
        from tkinter import ttk
        from archive_scout.ui.main_window import ArchiveScoutApp
        try:root=tk.Tk()
        except tk.TclError:self.skipTest('display is not available')
        app=None
        controls=[]
        references=[weakref.ref(root)]
        try:
            root.withdraw()
            app=Mock()
            app.notebook=ttk.Notebook(root)
            app.research_auto_var=tk.BooleanVar(root,value=False)
            app.research_backend_var=tk.StringVar(root,value='local-hash')
            app.research_limit_var=tk.StringVar(root,value='100')
            app.research_query_var=tk.StringVar(root)
            references.extend(weakref.ref(value) for value in (app,app.research_auto_var,
                app.research_backend_var,app.research_limit_var,app.research_query_var))
            ArchiveScoutApp.create_research_tab(app)
            def descendants(widget):
                for child in widget.winfo_children():
                    yield child
                    yield from descendants(child)
            controls=[widget for widget in descendants(app.notebook) if isinstance(widget,ttk.Checkbutton)]
            self.assertEqual(len(controls),1)
            self.assertIn('optional',controls[0].cget('text'))
            self.assertFalse(app.research_auto_var.get())
            controls[0].invoke()
            self.assertTrue(app.research_auto_var.get())
            app.research_index_button.invoke()
            app.start_research_index_ui.assert_called_once()
        finally:
            root.destroy()
            controls.clear()
            del app,root
            gc.collect()
        self.assertTrue(all(reference() is None for reference in references),
                        'Research-tab fixture retained Tk objects after main-thread cleanup')


class ResearchTabIsolationTests(unittest.TestCase):
    def test_gui_launcher_preserves_child_failures(self):
        import os
        import subprocess
        case=ResearchTabTests('test_tab_exposes_optional_control_and_manual_build')
        with patch.dict(os.environ,{},clear=False):
            os.environ.pop('ARCHIVE_SCOUT_RESEARCH_TAB_TEST_CHILD',None)
            with patch('subprocess.run',return_value=subprocess.CompletedProcess([],134,'','Tcl failure')) as child:
                with self.assertRaisesRegex(AssertionError,'Tcl failure'):
                    case.test_tab_exposes_optional_control_and_manual_build()
        self.assertEqual(child.call_count,1)
        self.assertEqual(child.call_args.kwargs['env']['ARCHIVE_SCOUT_RESEARCH_TAB_TEST_CHILD'],'1')
