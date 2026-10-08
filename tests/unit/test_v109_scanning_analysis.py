from __future__ import annotations

import hashlib
import json
import multiprocessing
import random
import sqlite3
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from archive_scout.analysis.diffs import _summary, compare_snapshots
from archive_scout.analysis.duplicates import _DiskMetricTree, cluster_duplicates
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_keyword_set, start_scan_run, upsert_document
from archive_scout.downloads.downloader import _scan_pending_captures
from archive_scout.eta import OperationForecast
from archive_scout.events import ProgressEvent, Stopped
from archive_scout.projects.backups import create_project_backup, list_project_backups
from archive_scout.scanning.executor import ScanByteBudget, ScanExecutor
from archive_scout.scanning.jobs import ScanJob
from archive_scout.ui.eta import OperationEtaTracker


class ScanAnalysisTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = open_database(self.root)
        self.cfg = ProjectConfig(self.root, ['example.com/*'], ['needle'], scan_workers=2).normalized()
        kid = get_or_create_keyword_set(self.db, 'Rules', ['needle'])
        run = start_scan_run(self.db, kid, 'Rules', 1, 'download')
        self.job = ScanJob.create(run, 'Rules', ['needle'])

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def capture(self, text, index, *, url=None):
        path = self.root / f'{index}.txt'
        path.write_text(text, encoding='utf-8')
        cur = self.db.execute("""INSERT INTO captures(original_url,timestamp,query_signature,mimetype,statuscode,
            state,local_path,payload_availability,bytes_saved,resource_class,created_at,updated_at)
            VALUES(?,? ,?,'text/plain','200','downloaded_unscanned',?,'retained_unscanned',?,'text','now','now')""",
            (url or f'http://example.com/{index}', f'200501{index % 28 + 1:02d}000000', cdx_query_signature(self.cfg), str(path), path.stat().st_size))
        return cur.lastrowid, path

    def document(self, text, index, **kwargs):
        cid, path = self.capture(text, index, **kwargs)
        return upsert_document(self.db, cid, path, '', text, [], hashlib.sha256(text.encode()).hexdigest(),
                               hashlib.sha256(text.encode()).hexdigest(), len(text.encode()))

    def test_spawn_workers_match_thread_results_and_do_not_leave_children(self):
        for index in range(300):
            self.capture(f'needle archive record {index} café', index)
        self.db.commit()
        children = {child.pid for child in multiprocessing.active_children()}
        for engine in ('thread', 'process'):
            with self.subTest(engine=engine):
                self.db.execute("UPDATE captures SET state='downloaded_unscanned'")
                self.db.commit()
                result = _scan_pending_captures(replace(self.cfg, scan_backend=engine), self.db, [self.job], threading.Event(), None)
                self.assertEqual((result['scanned'], result['errors']), (300, 0))
                rows = [tuple(row) for row in self.db.execute('SELECT document_id,score,hits_json,fields_json,snippets_json FROM document_matches ORDER BY document_id')]
                if engine == 'thread':
                    expected = rows
                else:
                    self.assertEqual(rows, expected)
        self.assertEqual({child.pid for child in multiprocessing.active_children()}, children)
        self.assertEqual(self.db.execute('PRAGMA quick_check').fetchone()[0], 'ok')

    def test_regex_stop_terminates_worker_and_preserves_resume_source(self):
        cid, path = self.capture('a' * 30000 + '!', 1)
        self.db.commit()
        job = ScanJob.create(self.job.scan_run_id, 'Rules', ['regex: (a+)+$'])
        stop = threading.Event()
        timer = threading.Timer(1.5, stop.set)
        timer.start()
        started = time.monotonic()
        try:
            with self.assertRaises(Stopped):
                _scan_pending_captures(self.cfg, self.db, [job], stop, None)
        finally:
            timer.cancel()
        self.assertLess(time.monotonic() - started, 6)
        self.assertTrue(path.exists())
        self.assertEqual(self.db.execute('SELECT state FROM captures WHERE id=?', (cid,)).fetchone()[0], 'downloaded_unscanned')
        result = _scan_pending_captures(self.cfg, self.db, [self.job], threading.Event(), None)
        self.assertEqual((result['scanned'], result['errors']), (1, 0))

    def test_large_payload_is_complete_with_exclusive_memory_admission(self):
        text = 'ordinary prose ' * 230000 + 'needle'
        cid, path = self.capture(text, 1)
        self.capture('needle second', 2)
        self.db.commit()
        budget = ScanByteBudget(32)
        size = budget.estimate({'local_path': str(path)})
        self.assertGreater(size, budget.limit)
        self.assertTrue(budget.accepts(size))
        budget.reserve(size)
        self.assertFalse(budget.accepts(1))
        budget.release(size)
        result = _scan_pending_captures(replace(self.cfg, scan_memory_mb=32), self.db, [self.job], threading.Event(), None)
        self.assertEqual((result['scanned'], result['matched'], result['errors']), (2, 2, 0))
        self.assertEqual(path.read_text(), text)

    def test_exact_hamming_tree_matches_oracle_past_old_bucket_limit(self):
        randomizer = random.Random(109)
        values = [randomizer.getrandbits(64) for _ in range(2001)]
        # Four differing 16-bit bands defeated the former candidate heuristic.
        values += [0, (1 << 0) | (1 << 16) | (1 << 32) | (1 << 48)]
        tree = _DiskMetricTree(self.db)
        for index, value in enumerate(values, 1):
            tree.insert(index, value)
        for radius in (0, 4, 8, 32):
            for query in [values[0], values[-1], randomizer.getrandbits(64)]:
                expected = {i for i, value in enumerate(values, 1) if (query ^ value).bit_count() <= radius}
                self.assertEqual(set(tree.matches(query, radius)), expected)

    def test_duplicate_cancellation_preserves_previous_published_groups(self):
        for index in range(110):
            self.document('same archive content', index)
        self.db.commit()
        cluster_duplicates(self.db)
        expected = [tuple(row) for row in self.db.execute('SELECT * FROM duplicate_members ORDER BY document_id')]
        stop = threading.Event()
        with self.assertRaises(Stopped):
            cluster_duplicates(self.db, stop_event=stop, callback=lambda event: stop.set())
        self.assertEqual([tuple(row) for row in self.db.execute('SELECT * FROM duplicate_members ORDER BY document_id')], expected)

    def test_duplicate_group_pruning_matches_complete_graph_with_late_bridges(self):
        randomizer = random.Random(10909)
        values = [0, 1, 0, 63, 62, 63, 7]
        values += [randomizer.getrandbits(64) for _ in range(75)]
        # These distant fingerprints are exact duplicates by saved identity.
        texts = [f'unique saved document {index}' for index in range(len(values))]
        texts[8] = texts[7]
        ids = [self.document(text, index) for index, text in enumerate(texts)]
        self.db.commit()
        for radius in (1, 3, 6, 32):
            with self.subTest(radius=radius):
                parent = list(range(len(values)))
                def find(index):
                    while parent[index] != index:
                        parent[index] = parent[parent[index]]
                        index = parent[index]
                    return index
                for left in range(len(values)):
                    for right in range(left):
                        if texts[left] == texts[right] or (values[left] ^ values[right]).bit_count() <= radius:
                            parent[find(left)] = find(right)
                components = {}
                for index, document in enumerate(ids):
                    components.setdefault(find(index), set()).add(document)
                expected = {frozenset(group) for group in components.values() if len(group) > 1}
                with mock.patch('archive_scout.analysis.duplicates.simhash64', side_effect=values):
                    cluster_duplicates(self.db, 1 - radius / 64)
                actual = {}
                for row in self.db.execute('SELECT group_id,document_id FROM duplicate_members'):
                    actual.setdefault(row[0], set()).add(row[1])
                self.assertEqual({frozenset(group) for group in actual.values()}, expected)

    def test_difference_retains_complete_counts_and_tiny_changes(self):
        earlier = '\n'.join(f'old line {index}' for index in range(350))
        later = '\n'.join(f'new line {index}' for index in range(350))
        result = _summary(earlier, later)
        self.assertEqual((result['added_count'], result['removed_count']), (350, 350))
        self.assertEqual(len(result['added_lines']), 200)
        large = 'same text ' * 200000
        result = _summary(large + 'a', large + 'b')
        self.assertTrue(result['changed'])
        self.assertEqual(result['similarity_method'], 'anchored_character_blocks_64_v1')
        self.assertTrue(_summary('x'*64 + 'y'*64, 'y'*64 + 'x'*64)['changed'])

    def test_cancelled_difference_keeps_previous_published_pairs(self):
        self.document('old text', 1, url='http://example.com/one')
        self.document('new text', 2, url='http://example.com/one')
        self.db.commit()
        compare_snapshots(self.db)
        expected = [tuple(row) for row in self.db.execute('SELECT * FROM snapshot_diffs')]
        stop = threading.Event(); stop.set()
        with self.assertRaises(Stopped):
            compare_snapshots(self.db, stop_event=stop)
        self.assertEqual([tuple(row) for row in self.db.execute('SELECT * FROM snapshot_diffs')], expected)

    def test_backup_cancellation_never_prunes_last_valid_snapshot(self):
        self.db.commit()
        old = create_project_backup(self.root)
        stop = threading.Event(); stop.set()
        with self.assertRaises(Stopped):
            create_project_backup(self.root, keep=1, stop_event=stop)
        self.assertEqual(list_project_backups(self.root), [old])
        self.assertEqual(list((self.root / 'backups').glob('*.tmp')), [])

    def test_eta_future_unknown_is_explicit_and_history_is_bounded(self):
        forecast = OperationForecast(self.db, self.cfg, 'download')
        plan = forecast.observe(ProgressEvent('download', '', 0, 100), now=0)
        self.assertEqual(plan['future_phases'][:2], ['scan', 'report'])
        self.assertIsNone(plan['future_seconds'])
        tracker = OperationEtaTracker(True)
        tracker.observe(ProgressEvent('download', '', 0, 100, {'eta_plan': plan}), now=0)
        tracker.observe(ProgressEvent('download', '', 10, 100, {'eta_plan': plan}), now=10)
        self.assertIn('later phases: scan, report', tracker.label(10))
        for index in range(100):
            forecast.observe(ProgressEvent('scan', '', index, 100), now=index)
        self.assertLessEqual(len(forecast.samples), 32)
        forecast.persist()
        saved = json.loads(self.db.execute("SELECT value FROM project_meta WHERE key='operation_eta_rates_v1'").fetchone()[0])
        self.assertLessEqual(len(saved), 32)
        self.assertTrue(any(key.startswith('scan:') for key in saved))

    def test_eta_phase_completion_and_unknown_work_keep_later_phases_visible(self):
        tracker = OperationEtaTracker(True)
        plan = {'future_phases': ['integrity_files', 'integrity_report'], 'future_seconds': None}
        tracker.observe(ProgressEvent('integrity', '', 100, 100, {'eta_plan': plan}), now=1)
        self.assertIn('phase complete', tracker.label(1))
        self.assertIn('later phases: integrity files, integrity report', tracker.label(1))
        plan = {'future_phases': ['integrity_report'], 'future_seconds': None}
        tracker.observe(ProgressEvent('integrity_files', '', detail={'eta_plan': plan}), now=2)
        self.assertIn('Estimating', tracker.label(2))
        self.assertIn('later phases: integrity report', tracker.label(2))
        tracker.finish()
        self.assertEqual(tracker.label(3), 'Estimated time remaining: Complete')

    def test_eta_known_future_uses_measurement_and_adds_recovery_once(self):
        tracker = OperationEtaTracker(True)
        plan = {'future_phases': ['scan'], 'future_seconds': 20}
        tracker.observe(ProgressEvent('download', '', 0, 100, {'eta_plan': plan}), now=0)
        tracker.observe(ProgressEvent('download', '', 10, 100, {'eta_plan': plan}), now=10)
        tracker.observe(ProgressEvent('download_retry', '', detail={'eta_plan': plan}), now=15)
        self.assertEqual(tracker.seconds_remaining(10), 90)
        self.assertIn('operation estimate', tracker.label(10))
        tracker.observe(ProgressEvent('network_waiting', '', detail={'waiting_seconds': 10}), now=10)
        self.assertEqual(tracker.seconds_remaining(15), 95)


if __name__ == '__main__':
    unittest.main()
