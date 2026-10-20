"""Official-release regressions for operation handoffs and independent reports."""
from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.config import ProjectConfig, MediaConfig, ReportConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_keyword_set, start_scan_run
from archive_scout.events import Stopped
from archive_scout.media.indexer import media_query_signature
from archive_scout.media.reports import generate_media_reports
from archive_scout.operations import run_project
from archive_scout.reports.snapshot import export_project_snapshot


class SnapshotReportTests(unittest.TestCase):
    def test_empty_project_writes_honest_snapshot_and_does_not_change_database(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = open_database(root)
            db.close()
            cfg = ProjectConfig(output_dir=root, targets=[], keywords=[]).normalized()
            result = export_project_snapshot(cfg)
            manifest = json.loads(result['manifest'].read_text())
            self.assertFalse(manifest['complete'])
            self.assertEqual(manifest['counts']['indexed_captures'], 0)
            self.assertTrue(result['snapshot_folder'].is_dir())
            self.assertTrue((result['snapshot_folder'] / 'reports' / 'summary.txt').exists())
            self.assertFalse((root / 'reports' / 'summary.txt').exists())
            db = open_database(root)
            try:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM operation_runs').fetchone()[0], 0)
            finally:
                db.close()

    def test_media_only_snapshot_obeys_selected_outputs_and_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = open_database(root)
            cfg = ProjectConfig(output_dir=root, targets=[], keywords=[], media=MediaConfig(enabled=True)).normalized()
            sig = media_query_signature(cfg)
            db.execute("""INSERT INTO media_captures(original_url,timestamp,query_signature,media_kind,extension,
                        created_at,updated_at) VALUES(?,?,?,?,?,?,?)""",
                       ('http://example.org/a.jpg','20070101120000',sig,'image','.jpg','now','now'))
            db.commit()
            db.close()
            cfg.report = ReportConfig(outputs=['media_indexed'], fields={'media_indexed':['original_url','state']}).normalized()
            result = export_project_snapshot(cfg)
            reports = result['snapshot_folder'] / 'reports'
            self.assertEqual((reports / 'media_indexed.txt').read_text().strip(), 'pending\thttp://example.org/a.jpg')
            self.assertFalse((reports / 'media_errors.txt').exists())
            self.assertFalse((root / 'reports' / 'media_indexed.txt').exists())
            manifest = json.loads(result['manifest'].read_text())
            self.assertEqual(manifest['counts']['media_captures'], 1)
            self.assertFalse(manifest['complete'])

    def test_read_only_export_does_not_block_other_committed_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = open_database(root)
            db.close()
            cfg = ProjectConfig(output_dir=root, targets=[], keywords=[]).normalized()
            from archive_scout.reports import snapshot
            original = snapshot._collect_report_files
            observed = []
            def concurrently_commit(*args, **kwargs):
                other = open_database(root)
                try:
                    with other:
                        other.execute("INSERT INTO project_meta(key,value) VALUES('external_writer','committed')")
                    observed.append(True)
                finally:
                    other.close()
                return original(*args, **kwargs)
            with patch.object(snapshot, '_collect_report_files', side_effect=concurrently_commit):
                result = export_project_snapshot(cfg)
            self.assertTrue(observed)
            self.assertTrue(result['manifest'].is_file())
            db = open_database(root)
            try:
                self.assertEqual(db.execute("SELECT value FROM project_meta WHERE key='external_writer'").fetchone()[0], 'committed')
            finally:
                db.close()

    def test_snapshot_scans_use_isolated_temp_tables_on_read_only_connection(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            db = open_database(root)
            keyword_id = get_or_create_keyword_set(db, 'test', ['needle'])
            scan_id = start_scan_run(db, keyword_id, 'finished', 1, 'rescan', {})
            db.execute("UPDATE scan_runs SET status='complete' WHERE id=?", (scan_id,))
            db.commit(); db.close()
            cfg = ProjectConfig(output_dir=root, targets=[], keywords=['needle']).normalized()
            paths = export_project_snapshot(cfg, scan_run_id=scan_id)
            snapshot = paths['snapshot_folder']
            self.assertTrue((snapshot/'reports'/'scan-00001-test'/'interesting_links.txt').exists())
            self.assertEqual(json.loads(paths['manifest'].read_text())['scan']['status'], 'complete')
            self.assertFalse((root/'reports'/'latest_scan_run.txt').exists())

    def test_export_failure_preserves_last_reports_and_cleans_partial_folder(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            db=open_database(root); db.close()
            latest=root/'reports'/'summary.txt'
            latest.parent.mkdir(parents=True,exist_ok=True)
            latest.write_text('previous complete report')
            cfg=ProjectConfig(output_dir=root, targets=[], keywords=[]).normalized()
            with patch('archive_scout.reports.snapshot.generate_index_reports',side_effect=PermissionError('denied')):
                with self.assertRaisesRegex(PermissionError, 'denied'):
                    export_project_snapshot(cfg)
            self.assertEqual(latest.read_text(),'previous complete report')
            self.assertFalse(list((root/'reports'/'manual').iterdir()))

    def test_second_export_same_project_is_rejected_while_active(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            db=open_database(root); db.close()
            cfg=ProjectConfig(output_dir=root, targets=[], keywords=[]).normalized()
            from archive_scout.reports import snapshot
            lock=snapshot._lock_for(root)
            with lock:
                with self.assertRaisesRegex(RuntimeError,'already'):
                    export_project_snapshot(cfg)


class CliOutcomeRegressions(unittest.TestCase):
    def test_incomplete_return_is_json_and_incomplete_exit_code(self):
        import argparse
        import io
        from contextlib import redirect_stdout
        from archive_scout.cli import _run_command, EXIT_INCOMPLETE
        from archive_scout.operations import OperationPaths
        cfg = ProjectConfig(output_dir=Path('/tmp/unused'), targets=[], keywords=[]).normalized()
        payload = OperationPaths({'summary': Path('/tmp/report.txt')}, status='incomplete', message='pending CDX')
        buf = io.StringIO()
        with patch('archive_scout.cli._load', return_value=cfg), \
             patch('archive_scout.cli.run_project', return_value=payload), \
             redirect_stdout(buf):
            code = _run_command(argparse.Namespace(project=Path('/unused'), mode='index', format='json'))
        self.assertEqual(code, EXIT_INCOMPLETE)
        data = json.loads(buf.getvalue())
        self.assertEqual(data['status'], 'incomplete')
        self.assertEqual(data['message'], 'pending CDX')


class OperationReportStatusTests(unittest.TestCase):
    def test_index_deferred_is_not_cli_or_gui_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg=ProjectConfig(output_dir=Path(tmp),targets=['example.org/*'],keywords=[]).normalized()
            with patch('archive_scout.operations.index_archive',return_value=True):
                result=run_project(cfg,'index')
            self.assertEqual(result.status,'incomplete')
            self.assertEqual(result['project'].name, 'project.json')

    def test_media_download_pending_keeps_incomplete_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            cfg=ProjectConfig(output_dir=root,targets=["example.org/*"],keywords=[],media=MediaConfig(enabled=True)).normalized()
            db=open_database(root)
            db.execute("""INSERT INTO media_captures(original_url,timestamp,query_signature,media_kind,
                          created_at,updated_at) VALUES(?,?,?,?,?,?)""",
                       ('http://example.org/a.jpg','20080101100000',media_query_signature(cfg),'image','now','now'))
            db.commit();db.close()
            with patch('archive_scout.operations.index_media'), patch('archive_scout.operations.download_media'):
                result=run_project(cfg,'media_download')
            self.assertEqual(result.status,'incomplete')

    def test_report_failure_does_not_revoke_completed_scan(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            cfg=ProjectConfig(output_dir=root,targets=['example.org/*'],keywords=['word']).normalized()
            with patch('archive_scout.operations.index_archive',side_effect=_fixture_index_complete), \
                 patch('archive_scout.operations.download_archive'), \
                 patch('archive_scout.operations.recover_transient_text_errors'), \
                 patch('archive_scout.operations.generate_job_reports',side_effect=PermissionError('disk full')):
                with self.assertRaises(PermissionError):
                    run_project(cfg,'all')
            db=open_database(root)
            try:
                self.assertEqual(db.execute('SELECT status FROM scan_runs ORDER BY id DESC LIMIT 1').fetchone()[0],'complete')
                self.assertEqual(db.execute('SELECT status FROM operation_runs ORDER BY id DESC LIMIT 1').fetchone()[0],'failed')
            finally:
                db.close()

    def test_resume_retains_original_external_media_policy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            cfg=ProjectConfig(output_dir=root,targets=['example.org/*'],keywords=['word']).normalized()
            with patch('archive_scout.operations.index_archive',side_effect=_fixture_index_complete), \
                 patch('archive_scout.operations.download_archive'), \
                 patch('archive_scout.operations.recover_transient_text_errors'), \
                 patch('archive_scout.operations.index_external_embedded_media',side_effect=Stopped):
                with self.assertRaises(Stopped):
                    run_project(cfg,'external_media_after_scan')
            with patch('archive_scout.operations.index_archive',side_effect=_fixture_index_complete), \
                 patch('archive_scout.operations.download_archive'), \
                 patch('archive_scout.operations.recover_transient_text_errors'), \
                 patch('archive_scout.operations.index_external_embedded_media',return_value='external-signature') as external, \
                 patch('archive_scout.operations.index_media') as direct, \
                 patch('archive_scout.operations.download_media'):
                result=run_project(cfg,'resume')
            external.assert_called_once()
            direct.assert_not_called()
            self.assertEqual(result.status,'complete')  # no retained documents, no pending embedded work


if __name__ == '__main__':
    unittest.main()


def _fixture_index_complete(config, database, *_unused):
    from archive_scout.cdx.parameters import cdx_query_signature
    from archive_scout.cdx.indexer import save_coverage_state
    from archive_scout.database.repositories import get_or_create_target
    scoped = config.normalized()
    for target in scoped.targets:
        target_config = scoped.for_target(target)
        target_id = get_or_create_target(database, target)
        save_coverage_state(database, target_id, cdx_query_signature(target_config),
                            target_config.from_date, target_config.to_date,
                            None, True, 0, None)
    database.commit()
