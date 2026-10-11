"""v1.2.4 regression gates: semantic CDX identity, resume, manual dashboard."""
from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.config import ProjectConfig
from archive_scout.cdx.parameters import cdx_query_signature, cdx_query_signatures
from archive_scout.cdx.indexer import _adopt_compatible_range_coverage, uncovered_index_ranges
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_capture
from archive_scout.ui.dashboard_refresh import DashboardRefreshController
from archive_scout.ui.main_window import ArchiveScoutApp


class IndexingIdentityTests(unittest.TestCase):
    def config(self, root: Path, *, start='2001', end='2002', collapse=True, scope='range', pages=100000):
        return ProjectConfig(root, targets=['example.org/*'], keywords=['needle'],
                             from_date=start, to_date=end, page_size=pages,
                             cdx_collapses=['urlkey'] if collapse else [],
                             text_collapse_scope=scope).normalized()

    def test_page_size_does_not_change_inventory_identity(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            first=self.config(root,pages=100000)
            other=self.config(root,pages=5000)
            self.assertEqual(cdx_query_signature(first),cdx_query_signature(other))
            self.assertNotEqual(cdx_query_signature(first),cdx_query_signatures(first)[1])

    def test_collapsed_range_changes_signature_but_uncollapsed_does_not(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            self.assertNotEqual(cdx_query_signature(self.config(root,end='2002')),
                                cdx_query_signature(self.config(root,end='2003')))
            self.assertEqual(cdx_query_signature(self.config(root,end='2002',collapse=False)),
                             cdx_query_signature(self.config(root,end='2003',collapse=False)))

    def test_completed_range_and_page_checkpoints_are_reused(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); db=open_database(root)
            try:
                config=self.config(root)
                target=get_or_create_target(db,config.targets[0])
                new=cdx_query_signature(config)
                old=next(sig for sig in cdx_query_signatures(config) if sig!=new)
                a,b=config.from_date, config.to_date
                db.execute('''INSERT INTO index_coverage(target_id,query_signature,range_start,range_end,
                             plan_json,strategy,layout_signature,complete,seen,updated_at)
                             VALUES(?,?,?,?,?,?,?,?,?,?)''',
                           (target,old,a,b,'{"test":"finished"}','paged','layout-existing',1,3,'fixture'))
                db.execute('''INSERT INTO index_pages(query_signature,target_id,window_start,window_end,
                             layout_signature,page,row_count,status,updated_at)
                             VALUES(?,?,?,?,?,?,?,?,?)''',
                           (old,target,a,b,'layout-existing',0,3,'complete','fixture'))
                upsert_capture(db, {'timestamp':'20010101000000','original':'http://example.org/a',
                                    'mimetype':'text/html','statuscode':'200','digest':'sha','length':'15'}, target, old)
                db.commit()
                with db:
                    _adopt_compatible_range_coverage(db,target,config,new)
                self.assertEqual(uncovered_index_ranges(db,target,new,a,b),[])
                self.assertEqual(db.execute('SELECT query_signature FROM captures').fetchone()[0],new)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM index_pages WHERE query_signature=?', (new,)).fetchone()[0],1)
                with db:
                    _adopt_compatible_range_coverage(db,target,config,new)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM index_pages WHERE query_signature=?', (new,)).fetchone()[0],1)
            finally:
                db.close()

    def test_partial_plan_and_existing_capture_ids_remain_intact(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); db=open_database(root)
            try:
                config=self.config(root)
                target=get_or_create_target(db,config.targets[0]); new=cdx_query_signature(config)
                old=next(sig for sig in cdx_query_signatures(config) if sig!=new)
                a,b=config.from_date,config.to_date
                db.execute('''INSERT INTO index_coverage(target_id,query_signature,range_start,range_end,
                             plan_json,strategy,layout_signature,complete,seen,updated_at)
                             VALUES(?,?,?,?,?,?,?,?,?,?)''',
                           (target,old,a,b,'{"pending":["resumekey"]}','resume','layout',0,100,'fixture'))
                row={'timestamp':'20010101000000','original':'http://example.org/a',
                     'mimetype':'text/html','statuscode':'200','digest':'sha','length':'15'}
                upsert_capture(db,row,target,old)
                upsert_capture(db,row,target,new)
                before=list(db.execute('SELECT id,query_signature FROM captures ORDER BY id'))
                with db:
                    _adopt_compatible_range_coverage(db,target,config,new)
                after=list(db.execute('SELECT id,query_signature FROM captures ORDER BY id'))
                self.assertEqual([tuple(x) for x in before],[tuple(x) for x in after])
                plan=db.execute('SELECT plan_json,complete,seen FROM index_coverage WHERE query_signature=?',(new,)).fetchone()
                self.assertEqual(tuple(plan),('{"pending":["resumekey"]}',0,100))
            finally:
                db.close()


class DashboardManualTests(unittest.TestCase):
    def test_explicit_refresh_has_generous_deadline_without_polling(self):
        class Var:
            def __init__(self,value): self.v=value
            def get(self): return self.v
            def set(self,value): self.v=value
        class Events:
            def __init__(self): self.calls=[]
            def put(self,value): self.calls.append(value)
        class ThreadStub:
            def __init__(self,target,**kwargs): self.target=target
            def start(self): self.target()
        class FakeApp:
            def __init__(self):
                self.project_restore_identity=None
                self.output_var=Var('/tmp/archive-scout-v124-test')
                self.dashboard_project_var=Var('')
                self.dashboard_last_refresh_var=Var('')
                self.dashboard_refresh=DashboardRefreshController(mode='manual')
                self.events=Events()
            def project_identity(self): return 'test'
        app=FakeApp()
        with patch('archive_scout.ui.main_window.threading.Thread',ThreadStub), \
             patch('archive_scout.ui.main_window.read_dashboard_counts',return_value={'captures':10}) as reader:
            ArchiveScoutApp.refresh_dashboard(app,manual=True)
            self.assertEqual(reader.call_args.kwargs['max_query_seconds'],20.0)
            self.assertEqual(app.events.calls[0][0],'dashboard')
            self.assertIn('Refreshing',app.dashboard_last_refresh_var.get())


if __name__=='__main__': unittest.main()
