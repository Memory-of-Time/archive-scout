import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, record_error, upsert_capture
from archive_scout.downloads.rate_limit import ReplayOutage
from archive_scout.downloads.schedule import clear_retry, record_failure
from archive_scout.operations import _retryable_text_batches, recover_transient_text_errors


class ReplayAdmissionTests(unittest.TestCase):
    def test_probe_admission_requires_distinct_failures_and_reopens_after_a_save(self):
        now = 0
        outage = ReplayOutage(3, 3, clock=lambda: now)
        now = 12
        self.assertFalse(outage.failed(1, 'connection'))
        self.assertFalse(outage.failed(1, 'connect_timeout'))
        self.assertFalse(outage.failed(2, 'payload_digest_mismatch'))
        self.assertFalse(outage.failed(3, 'read_timeout'))
        self.assertEqual(outage.capacity(20), 20)
        self.assertTrue(outage.failed(4, 'connection'))
        self.assertEqual(outage.capacity(20), 0)
        now = 15
        self.assertEqual(outage.capacity(20), 1)
        outage.admitted()
        self.assertEqual(outage.capacity(20), 0)
        self.assertTrue(outage.saved())
        self.assertEqual(outage.capacity(20), 20)
        now = 16
        for cid in range(6):
            self.assertFalse(outage.failed(cid, 'connection'))
        self.assertEqual(outage.capacity(20), 20)


class RetrySweepTests(unittest.TestCase):
    def setUp(self):
        root = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.db = self.enterContext(closing(open_database(root)))
        self.cfg = ProjectConfig(root, ['example.test/*'], [], from_date='2001', to_date='2003',
                                 cdx_collapses=[], max_attempts=1,
                                 network=NetworkConfig(retry_base_seconds=1, retry_max_seconds=1)).normalized()
        self.target_id = get_or_create_target(self.db, self.cfg.targets[0])
        self.signature = cdx_query_signature(self.cfg)

    def failure(self, n, category='read_timeout', *, scheduled=True, deadline=0):
        with self.db:
            original = f'http://example.test/{n}'
            upsert_capture(self.db, {'timestamp': '20010101000000', 'original': original,
                       'mimetype': 'text/html', 'statuscode': '200', 'length': 1}, self.target_id, self.signature)
            cid = int(self.db.execute('SELECT id FROM captures WHERE original_url=?', (original,)).fetchone()[0])
            self.db.execute("UPDATE captures SET state='error' WHERE id=?", (cid,))
            record_error(self.db, 'download', category, 'fixture', capture_id=cid, retryable=True)
            if scheduled:
                record_failure(self.db, cid, category, None, True, 1, 1)
                self.db.execute('UPDATE text_retry_schedule SET next_eligible_at=? WHERE capture_id=?', (deadline, cid))
        return cid

    def test_retry_sweep_revalidates_saved_keys_and_reopens_for_new_errors(self):
        first = self.failure(0)
        due_later = self.failure(1, deadline=4200000000)
        obsolete = self.failure(2)
        legacy = self.failure(3, scheduled=False)
        sweep = _retryable_text_batches(self.cfg, self.db, batch_size=1, automatic=True)
        self.assertEqual(next(sweep), [first])
        with self.db:
            self.db.execute('UPDATE text_retry_schedule SET next_eligible_at=0 WHERE capture_id=?', (due_later,))
            self.db.execute('UPDATE errors SET resolved=1 WHERE capture_id=?', (obsolete,))
        new = self.failure(4)
        self.assertEqual(list(sweep), [[due_later], [legacy]])
        self.assertEqual(list(_retryable_text_batches(self.cfg, self.db, automatic=True)),
                         [[first, due_later, legacy, new]])

    def test_read_timeout_recovery_probes_one_capture_then_drains_all_due_work(self):
        ids = [self.failure(n) for n in range(4)]
        calls = []

        class ImmediateStop(threading.Event):
            def wait(self, timeout=None):
                return self.is_set()

        def retry(*_args, **kwargs):
            selected = kwargs['capture_ids']
            calls.append(selected)
            if len(calls) <= 2:
                return
            with self.db:
                for cid in selected:
                    self.db.execute("UPDATE captures SET state='downloaded' WHERE id=?", (cid,))
                    self.db.execute('UPDATE errors SET resolved=1 WHERE capture_id=?', (cid,))
                    clear_retry(self.db, cid)

        with patch('archive_scout.operations.download_archive', side_effect=retry):
            self.assertEqual(recover_transient_text_errors(self.cfg, self.db, 0, [], ImmediateStop(), None,
                             continuous=True, acquire_only=True), 0)
        self.assertEqual(calls, [ids, [ids[0]], [ids[0]], ids[1:]])
        self.assertEqual(self.db.execute('SELECT count(*) FROM text_retry_schedule').fetchone()[0], 0)
