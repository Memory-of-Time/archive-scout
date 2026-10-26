"""Regressions for audited evidence, scope, retry and independent domain contracts."""
import hashlib
import json
import os
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from archive_scout.config import ProjectConfig, ResearchConfig, ReportConfig, MediaConfig, project_config_from_payload
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_captures, upsert_document, record_error, upsert_media_capture, list_site_issues
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.content import parse_page
from archive_scout.domains import iter_domains, commit_domains, backfill_domains, publish_domains, rules
from archive_scout.scanning.keywords import compile_keywords
from archive_scout.scanning.scoring import analyze_content
from archive_scout.scanning.full_text import search_documents
from archive_scout.media.recovery import record_media_failure, recover_active_media
from archive_scout.media.indexer import media_query_signature
from archive_scout.media.downloader import validate_media_representation
from archive_scout.downloads.validation import classify_exception
from archive_scout.events import Stopped
from archive_scout.utils import hash_text, normalize_search


class ProjectFixture(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.cfg=ProjectConfig(self.root,['example.org/*'],['needle'],from_date='2008',to_date='2008',cdx_collapses=[],extract_domains=True).normalized()
        self.db=open_database(self.root)
    def tearDown(self):
        self.db.close();self.temp.cleanup()
    def seed(self,body='needle',url='http://example.org/page',timestamp='20080101000000',cfg=None):
        cfg=cfg or self.cfg
        target=get_or_create_target(self.db,cfg.targets[0])
        sig=cdx_query_signature(cfg)
        upsert_captures(self.db,[(timestamp,url,'text/html','200','',str(len(body.encode())))],target,sig)
        cid=self.db.execute('SELECT id FROM captures WHERE original_url=? AND timestamp=?',(url,timestamp)).fetchone()[0]
        path=self.root/'captures'/f'{cid}.txt';path.parent.mkdir(exist_ok=True);path.write_bytes(body.encode())
        title,visible,links=parse_page(body,url)
        digest=hashlib.sha256(body.encode()).hexdigest()
        did=upsert_document(self.db,cid,path,title,visible,links,digest,hash_text(normalize_search(visible)),path.stat().st_size)
        self.db.commit()
        return cid,did,path


class DomainRules(unittest.TestCase):
    def test_full_hostnames_deduplicate_without_parent_collapse(self):
        found=set(iter_domains('www.Example.com cdn.example.com example.com https://CDN.EXAMPLE.COM:80/x cdn.example.com.'))
        self.assertEqual(found,{'www.example.com','cdn.example.com','example.com'})
    def test_multilevel_private_wildcard_exception(self):
        self.assertEqual(set(iter_domains('cdn.example.co.uk user.github.io co.uk github.io a.ck www.ck sub.www.ck')),
                         {'cdn.example.co.uk','user.github.io','www.ck','sub.www.ck'})
    def test_escaped_unicode_and_email(self):
        found=set(iter_domains(r'https:\/\/cdn.example.com/a https://xn--bcher-kva.de bücher.de user.name@sub.example.org cdn%2Eexample%2Ecom'))
        self.assertEqual(found,{'cdn.example.com','xn--bcher-kva.de','sub.example.org'})
    def test_no_partial_suffix_match_or_ip(self):
        self.assertEqual(set(iter_domains('example.com.invalid-example 127.0.0.1 localhost co.uk bad_name.com')),set())
    def test_retired_and_filename_ambiguity(self):
        self.assertEqual(set(iter_domains('archive.yu version.py')),{'archive.yu','version.py'})
    def test_replay_wrapper(self):
        self.assertEqual(set(iter_domains('https://web.archive.org/web/20080101000000/http://cdn.example.org/x')),{'cdn.example.org'})
    def test_late_body_domain(self):
        self.assertIn('last.example.com',set(iter_domains('x '*600000+'last.example.com')))
    def test_long_unbroken_tokens_and_credentials(self):
        source='x'*1000000+' https://user.name:password@cdn.example.com/ user.name@other.example.org'
        self.assertEqual(set(iter_domains(source)),{'cdn.example.com','other.example.org'})
    def test_visible_domain_split_by_markup(self):
        from archive_scout.domains import domain_text
        raw='<p>cdn.exa<b>mple</b>.com</p>'
        _,visible,links=parse_page(raw,'http://source.example.org')
        self.assertIn('cdn.example.com',set(iter_domains(domain_text(raw,visible,links))))
    def test_pinned_data_and_recognizer_identity(self):
        self.assertGreater(len(rules()[0]),1400)
        self.assertTrue(rules()[-1].startswith('domains-v1-full-host:'))
    def test_generated_pattern_roundtrip(self):
        from archive_scout.cdx.indexer import validate_cdx_scope
        from archive_scout.cdx.client import MalformedCDXResponse
        for host in ['cdn.example.com','deep.cdn.example.com']:
            validate_cdx_scope([('20080101000000','http://'+host+'/a','text/html','200','','10')],'*.cdn.example.com/*','20080101000000','20081231235959',['statuscode:200'])
        with self.assertRaises(MalformedCDXResponse):
            validate_cdx_scope([('20080101000000','http://example.com/a','text/html','200','','10')],'*.cdn.example.com/*','20080101000000','20081231235959',['statuscode:200'])


class DomainPersistence(ProjectFixture):
    def test_backfill_all_bodies_not_only_keyword_matches(self):
        self.seed('no matching keyword cdn.example.com')
        self.seed('excluded other.example.org',url='http://example.org/other')
        backfill_domains(self.db,self.cfg,threading.Event())
        path,gaps=publish_domains(self.db,self.cfg)
        self.assertEqual(gaps,0)
        self.assertEqual(path.read_text().splitlines(),['*.cdn.example.com/*','*.other.example.org/*'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM document_matches').fetchone()[0],0)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM documents').fetchone()[0],2)
    def test_atomic_rollback_and_idempotence(self):
        cid,_,path=self.seed('cdn.example.com');digest=hashlib.sha256(path.read_bytes()).hexdigest()
        with self.assertRaises(RuntimeError),self.db:
            commit_domains(self.db,cid,digest,'cdn.example.com')
            raise RuntimeError('injected transaction failure')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM domain_sources').fetchone()[0],0)
        for _ in range(2):
            with self.db:commit_domains(self.db,cid,digest,'cdn.example.com cdn.example.com')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM domain_hosts').fetchone()[0],1)
    def test_unchanged_resume_does_not_reread_bodies(self):
        self.seed('cdn.example.com');backfill_domains(self.db,self.cfg,threading.Event())
        with patch('pathlib.Path.read_bytes',side_effect=AssertionError('unnecessary reread')):
            backfill_domains(self.db,self.cfg,threading.Event())
    def test_missing_changed_input_unresolved(self):
        _,_,path=self.seed('cdn.example.com');path.write_text('changed')
        backfill_domains(self.db,self.cfg,threading.Event())
        self.assertEqual(publish_domains(self.db,self.cfg)[1],1)
        self.assertEqual(self.db.execute('SELECT status FROM domain_sources').fetchone()[0],'error')
    def test_declared_charset_backfill_uses_payload_evidence(self):
        cid,did,path=self.seed('http://例え.jp')
        data='http://例え.jp'.encode('shift_jis');path.write_bytes(data)
        digest=hashlib.sha256(data).hexdigest()
        self.db.execute('UPDATE documents SET content_hash=?,size_bytes=? WHERE id=?',(digest,len(data),did))
        self.db.execute("INSERT INTO capture_payload_evidence(capture_id,content_hash,bytes_saved,response_content_type,final_url,response_status,updated_at) VALUES(?,?,?,'text/html; charset=shift_jis','http://example.org/page',200,'now')",(cid,digest,len(data)));self.db.commit()
        backfill_domains(self.db,self.cfg,threading.Event())
        self.assertEqual(publish_domains(self.db,self.cfg)[0].read_text(),'*.xn--r8jz45g.jp/*\n')
    def test_selection_and_date_scope(self):
        self.seed('yes.example.com')
        other=replace(self.cfg,targets=['other.org/*'])
        self.seed('no.example.com',url='http://other.org/a',cfg=other)
        self.seed('old.example.com',url='http://example.org/old',timestamp='20070101000000')
        backfill_domains(self.db,self.cfg,threading.Event())
        path,gaps=publish_domains(self.db,self.cfg)
        self.assertEqual(path.read_text(),'*.yes.example.com/*\n')
        self.assertEqual(gaps,0)
    def test_config_roundtrip(self):
        self.assertTrue(project_config_from_payload(self.cfg.to_payload(),self.root/'project.json').extract_domains)
        self.assertFalse(project_config_from_payload({'targets':[],'keywords':[]},self.root/'project.json').extract_domains)
    def test_public_rescan_has_independent_domain_output_and_lean_storage(self):
        from archive_scout.operations import run_project
        self.seed('needle cdn.example.com')
        cfg=replace(self.cfg,report=ReportConfig(outputs=['matched_urls'],retain_scan_details=False))
        paths=run_project(cfg,'rescan')
        self.assertEqual(paths['domains'].read_text(),'*.cdn.example.com/*\n')
        row=self.db.execute('SELECT score,hits_json,snippets_json FROM document_matches').fetchone()
        self.assertGreater(row[0],0);self.assertEqual(json.loads(row[1]),{});self.assertEqual(json.loads(row[2]),[])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM reviews').fetchone()[0],0)

    def test_excluded_keyword_does_not_exclude_domains(self):
        from archive_scout.operations import run_project
        self.seed('needle banned cdn.example.com')
        paths=run_project(replace(self.cfg,keyword_sets=[],keywords=['needle','exclude: banned']),'rescan')
        self.assertEqual(paths['domains'].read_text(),'*.cdn.example.com/*\n')
        self.assertEqual(self.db.execute('SELECT score FROM document_matches').fetchone()[0],0)

    def test_report_failure_preserves_previous_domain_report(self):
        self.seed('cdn.example.com');backfill_domains(self.db,self.cfg,threading.Event())
        path,_=publish_domains(self.db,self.cfg)
        with patch('archive_scout.utils.os.replace',side_effect=PermissionError('injected')):
            with self.assertRaises(PermissionError):publish_domains(self.db,self.cfg)
        self.assertEqual(path.read_text(),'*.cdn.example.com/*\n')

    def test_stop_backfill_leaves_no_false_coverage(self):
        self.seed('cdn.example.com');stop=threading.Event();stop.set()
        with self.assertRaises(Stopped):backfill_domains(self.db,self.cfg,stop)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM domain_sources').fetchone()[0],0)


class EvidenceCorrectness(ProjectFixture):
    def test_required_labels_are_not_identity(self):
        result=analyze_content('','','Alpha','Alpha',[],compile_keywords(['required: Alpha | label=shared','required: Beta | label=shared']))
        self.assertTrue(result['required_missing']);self.assertEqual(result['score'],0)
    def test_old_multi_rule_scores_are_preserved_in_new_generation(self):
        from archive_scout.operations import prepare_scan_jobs
        from archive_scout.database.repositories import get_or_create_keyword_set,start_scan_run,save_match
        _,did,_=self.seed('Alpha')
        cfg=replace(self.cfg,keywords=['required: Alpha | label=same','required: Beta | label=same'],keyword_sets=[]).normalized()
        kid=get_or_create_keyword_set(self.db,cfg.keyword_set_name,cfg.keywords)
        old=start_scan_run(self.db,kid,'old',1,'all',{})
        save_match(self.db,old,did,{'score':2})
        self.db.execute("UPDATE scan_runs SET status='interrupted' WHERE id=?",(old,));self.db.commit()
        jobs=prepare_scan_jobs(self.db,cfg,'resume')
        self.assertNotEqual(jobs[0].scan_run_id,old)
        self.assertEqual(self.db.execute('SELECT score FROM document_matches WHERE scan_run_id=?',(old,)).fetchone()[0],2)
    def test_no_body_position_does_not_receive_proximity_bonus(self):
        result=analyze_content('http://example.org/Alpha-Bravo','','short','short',[],compile_keywords(['Alpha','Bravo']))
        self.assertEqual(result['proximity']['pairs'],0)
    def test_integrity_reference_tracking_does_not_label_retained_text_orphan(self):
        from archive_scout.projects.integrity import check_project_integrity
        self.seed('needle')
        path=check_project_integrity(self.root,self.db)
        self.assertNotIn('ORPHAN_FILE',path.read_text())
    def test_sensitive_proximity(self):
        text='Alpha Bravo '+' filler'*60
        a=analyze_content('','',text,text,[],compile_keywords(['Alpha','Bravo']))
        b=analyze_content('','',text,text,[],compile_keywords(['Alpha | case','Bravo | case']))
        self.assertEqual(a['score'],b['score']);self.assertEqual(b['proximity']['pairs'],1)
    def test_domain_filter_is_host_bounded(self):
        for url in ['http://example.org/a','http://sub.example.org/a','http://unrelated.org/?x=example.org','http://example.org.evil.net/a']:
            self.seed('needle',url=url)
        urls={r['original_url'] for r in search_documents(self.db,'needle',domain='example.org')}
        self.assertEqual(urls,{'http://example.org/a','http://sub.example.org/a'})
    def test_readonly_hash_and_question_paths(self):
        from archive_scout.cli import _readonly_database
        # '?' is a legal POSIX filename but not a legal Windows filename.
        name='project#part%20' if os.name=='nt' else 'project#part?x'
        cfg=replace(self.cfg,output_dir=self.root/name)
        db=open_database(cfg.output_dir);db.close()
        readonly=_readonly_database(cfg)
        self.assertIsNotNone(readonly.execute('SELECT version FROM schema_info').fetchone());readonly.close()
        self.assertFalse((self.root/'project').exists())
    def test_import_versions_preserve_same_mtime_history(self):
        from archive_scout.projects.importers import import_text_folder
        folder=self.root/'input';folder.mkdir();path=folder/'a.html'
        path.write_text('first');os.utime(path,(1000000000,1000000000))
        import_text_folder(self.root,folder,self.db,threading.Event())
        path.write_text('second');os.utime(path,(1000000000,1000000000))
        import_text_folder(self.root,folder,self.db,threading.Event())
        self.assertEqual({r[0] for r in self.db.execute('SELECT body_text FROM documents')},{'first','second'})
    def test_oversized_import_is_unfinished_not_truncated(self):
        from archive_scout.projects.importers import import_text_folder
        folder=self.root/'input';folder.mkdir();(folder/'large.txt').write_text('needle'*100)
        with self.assertRaisesRegex(ValueError,'not truncated'):
            import_text_folder(self.root,folder,self.db,threading.Event(),max_bytes=32)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM documents').fetchone()[0],0)
    def test_merge_rejects_changed_bytes(self):
        from archive_scout.projects.merge import merge_projects
        _,_,path=self.seed('original');path.write_text('tampered')
        destination=self.root/'destination';other=open_database(destination)
        try:
            with self.assertRaisesRegex(RuntimeError,'recorded hash'):
                merge_projects(destination,self.root,other)
            self.assertEqual(other.execute('SELECT COUNT(*) FROM documents').fetchone()[0],0)
        finally:other.close()
    def test_missing_analysis_preserves_previous_generation(self):
        from archive_scout.analysis.workflow import run_analysis
        self.seed('needle')
        self.db.execute("INSERT INTO forum_threads(canonical_key,canonical_url,title,profile,created_at) VALUES('prior','http://example.org/','prior','auto','now')")
        self.db.commit()
        self.db.execute("UPDATE documents SET path='missing.txt'");self.db.commit()
        with self.assertRaises(FileNotFoundError):run_analysis(self.cfg,self.db,threading.Event())
        self.assertEqual(self.db.execute('SELECT canonical_key FROM forum_threads').fetchone()[0],'prior')
        self.assertEqual(self.db.execute('SELECT status FROM analysis_runs ORDER BY id DESC LIMIT 1').fetchone()[0],'failed')
    def test_analysis_shared_utf16_decode(self):
        from archive_scout.analysis.workflow import _read_source
        p=self.root/'u.txt';p.write_bytes('needle'.encode('utf-16'))
        self.assertIn('needle',_read_source(str(p)))
    def test_forum_disabled_rejected_before_clearing(self):
        from archive_scout.analysis.workflow import run_analysis
        cfg=replace(self.cfg,analysis=replace(self.cfg.analysis,reconstruct_threads=False))
        with self.assertRaises(ValueError):run_analysis(cfg,self.db,threading.Event(),forum_only=True)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM analysis_runs').fetchone()[0],0)
    def test_external_analysis_retries_are_not_complete(self):
        from archive_scout.analysis.workflow import run_analysis
        self.seed('<html>needle <embed src="http://cdn.example.com/movie.swf"></html>')
        cfg=replace(self.cfg,analysis=replace(self.cfg.analysis,search_external_assets=True,external_domains=['cdn.example.com']))
        def lookup(*args):
            self.db.execute("UPDATE legacy_assets SET archive_status='retry'");self.db.commit()
            return 0
        with patch('archive_scout.analysis.workflow._lookup_external_assets',side_effect=lookup):
            paths=run_analysis(cfg,self.db,threading.Event())
        self.assertEqual(paths.status,'incomplete')
        self.assertEqual(self.db.execute('SELECT status FROM analysis_runs ORDER BY id DESC LIMIT 1').fetchone()[0],'incomplete')
    def test_research_settings_and_parsed_changes_invalidate(self):
        from archive_scout.research.index import build_research_index
        _,did,_=self.seed('needle http://cdn.example.com')
        a=replace(self.cfg,research=ResearchConfig(vector_dimensions=64,entity_extraction=False,duplicate_clustering=False))
        build_research_index(a,self.db)
        b=replace(a,research=replace(a.research,vector_dimensions=128,entity_extraction=True))
        self.assertEqual(build_research_index(b,self.db).indexed,1)
        self.assertEqual(self.db.execute('SELECT dimensions FROM research_vectors').fetchone()[0],128)
        self.assertGreater(self.db.execute('SELECT COUNT(*) FROM research_document_entities').fetchone()[0],0)
        self.assertEqual(build_research_index(b,self.db).unchanged,1)
        self.db.execute('UPDATE documents SET title=? WHERE id=?',('changed title',did));self.db.commit()
        self.assertEqual(build_research_index(b,self.db).indexed,1)
    def test_auto_research_omitted_is_off(self):
        self.assertFalse(project_config_from_payload({},self.root/'project.json').research.auto_build)

    def test_failed_generation_promotion_keeps_old_derived_rows(self):
        from archive_scout.database.staging import derived_generation
        self.db.execute("INSERT INTO forum_threads(canonical_key,title,created_at) VALUES('old','old','now')");self.db.commit()
        with self.assertRaises(RuntimeError):
            with derived_generation(self.db,['forum_threads','forum_posts']):
                self.db.execute("INSERT INTO forum_threads(canonical_key,title,created_at) VALUES('new','new','now')")
                self.db.commit()
                raise RuntimeError('injected interruption after local commit')
        self.assertEqual(self.db.execute('SELECT canonical_key FROM forum_threads').fetchone()[0],'old')
        self.assertEqual(self.db.execute('PRAGMA foreign_keys').fetchone()[0],1)

    def test_unchanged_research_skips_encoder_and_entity_reader(self):
        from archive_scout.research.index import build_research_index
        self.seed('needle')
        cfg=replace(self.cfg,research=ResearchConfig(duplicate_clustering=False))
        build_research_index(cfg,self.db)
        with patch('archive_scout.research.index.encode_text',side_effect=AssertionError('unnecessary encoding')),patch('archive_scout.research.index.extract_entities',side_effect=AssertionError('unnecessary entities')):
            self.assertEqual(build_research_index(cfg,self.db).unchanged,1)

    def test_unchanged_duplicates_reuse_fingerprints(self):
        from archive_scout.analysis.duplicates import cluster_duplicates
        self.seed('needle shared');self.seed('needle shared',url='http://example.org/b')
        self.assertEqual(cluster_duplicates(self.db).exact_groups,1)
        with patch('archive_scout.analysis.duplicates.simhash64',side_effect=AssertionError('unnecessary full body hash')):
            self.assertEqual(cluster_duplicates(self.db).exact_groups,1)

    def test_repair_queues_missing_downloaded_media(self):
        from archive_scout.projects.repair import repair_project
        upsert_media_capture(self.db,{'timestamp':'20080101000000','original':'http://example.org/a.jpg','mimetype':'image/jpeg','statuscode':'200','digest':'','length':'40'},None,'fixture','image','.jpg')
        self.db.execute("UPDATE media_captures SET state='downloaded',path='missing.jpg',bytes_saved=40,content_hash='x'");self.db.commit()
        repair_project(self.root,self.db)
        self.assertEqual(self.db.execute('SELECT state FROM media_captures').fetchone()[0],'pending')

    def test_integrity_stop_is_cancellable(self):
        from archive_scout.projects.integrity import check_project_integrity
        self.seed('needle');stop=threading.Event();stop.set()
        with self.assertRaises(Stopped):check_project_integrity(self.root,self.db,stop_event=stop)

    def test_schema10_migration_requires_backup(self):
        self.db.execute('UPDATE schema_info SET version=10');self.db.commit();self.db.close()
        with patch('archive_scout.projects.backups.create_project_backup',side_effect=OSError('cannot backup')):
            with self.assertRaises(OSError):open_database(self.root)
        import sqlite3
        self.db=sqlite3.connect(self.root/'archive_scout.sqlite3');self.db.row_factory=sqlite3.Row
        self.assertEqual(self.db.execute('SELECT version FROM schema_info').fetchone()[0],10)
    def test_no_migration_flag_cannot_bypass_safety_backup(self):
        self.db.execute('UPDATE schema_info SET version=10');self.db.commit()
        with self.assertRaisesRegex(RuntimeError,'backed-up'):
            open_database(self.root,migrate=False)
        self.assertEqual(self.db.execute('SELECT version FROM schema_info').fetchone()[0],10)
    def test_issue_filter_precedes_limit(self):
        for n in range(1001):
            self.db.execute("INSERT INTO site_issues(host,stage,category,message,resolved,first_seen,last_seen) VALUES(?,'x','connection','x',?,'now',?)",(f'h{n}',int(n>0),str(n).zfill(5)))
        self.assertEqual(len(list_site_issues(self.db,unresolved_only=False,resolved=False,limit=1000)),1)


class RetryCorrectness(ProjectFixture):
    def media(self):
        cfg=replace(self.cfg,media=MediaConfig(enabled=True,discover_embedded=False,snapshot_strategy='all'))
        upsert_media_capture(self.db,{'timestamp':'20080101000000','original':'http://example.org/a.jpg','mimetype':'image/jpeg','statuscode':'200','digest':'','length':'40'},None,media_query_signature(cfg),'image','.jpg')
        cid=self.db.execute('SELECT id FROM media_captures').fetchone()[0]
        self.db.execute("UPDATE media_captures SET state='error'");self.db.commit()
        return cfg,cid
    def test_old_transient_does_not_revive_new_nonretryable(self):
        cfg,cid=self.media()
        with self.db:
            record_error(self.db,'media_download','connection','old',media_capture_id=cid,retryable=True)
            record_media_failure(self.db,cid,'connection',None,True,cfg)
            self.db.execute('UPDATE media_retry_schedule SET next_eligible_at=0')
        calls=[]
        def download(*args,**kwargs):
            calls.append(kwargs['media_capture_ids'])
            with self.db:
                record_error(self.db,'media_download','invalid_media_representation','new',media_capture_id=cid,retryable=False)
                record_media_failure(self.db,cid,'invalid_media_representation',None,False,cfg)
        with patch('archive_scout.coverage.require_media_coverage'),patch('archive_scout.media.downloader.download_media',side_effect=download):
            recover_active_media(cfg,self.db,threading.Event())
        self.assertEqual(calls,[[cid]])
        self.assertEqual(self.db.execute('SELECT failure_count,retryable FROM media_retry_decisions').fetchone()[0],2)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM errors WHERE resolved=0').fetchone()[0],2)
    def test_nonretry_decision_survives_reopen(self):
        cfg,cid=self.media()
        with self.db:
            record_error(self.db,'media_download','connection','old',media_capture_id=cid,retryable=True)
            record_media_failure(self.db,cid,'invalid_media_representation',200,False,cfg)
        self.db.close();self.db=open_database(self.root)
        with patch('archive_scout.coverage.require_media_coverage'),patch('archive_scout.media.downloader.download_media') as download:
            recover_active_media(cfg,self.db,threading.Event())
        download.assert_not_called()
    def test_html_rejection_retains_category_status_evidence(self):
        try:
            validate_media_representation({'status':200,'bytes':40,'preview':b'<html>error</html>','headers':{'content-type':'text/html'}},path_hint='a.jpg')
        except RuntimeError as exc:
            self.assertEqual(classify_exception(exc),('invalid_wayback_replay',200,False))
            self.assertEqual(exc.representation_diagnostic['response_status'],200)
        else:self.fail('HTML accepted as media')
    def test_backoff_preserved_across_category_changes(self):
        cfg,cid=self.media()
        with self.db:
            record_media_failure(self.db,cid,'connection',None,True,cfg)
            record_media_failure(self.db,cid,'incomplete_representation',200,True,cfg)
        self.assertEqual(self.db.execute('SELECT failure_count FROM media_retry_schedule').fetchone()[0],2)
    def test_phase_health_retains_counts_across_retry_batches(self):
        from archive_scout.downloads.diagnostics import AcquisitionDiagnostics
        d=AcquisitionDiagnostics(self.root,None,kind='media')
        d.begin_batch(None);d.summary(8,force=True)
        d.begin_batch(None);d.summary(2,force=True)
        value=json.loads((self.root/'diagnostics/media-acquisition/latest.json').read_text())
        self.assertEqual(value['new_saves'],10);self.assertEqual(value['batch_new_saves'],2)
        self.assertIsNone(value['avg_backend_elapsed_seconds'])


if __name__=='__main__':unittest.main()
