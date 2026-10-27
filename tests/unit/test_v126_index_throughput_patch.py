"""v1.2.6: adjacent SURT collisions, exact checkpoints, and index write timings."""
from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import CDXRows, HttpClient, MalformedCDXResponse
from archive_scout.cdx.indexer import index_archive, validate_cdx_scope
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database


GOOD = ('20080101000000', 'http://www.liveleak.com/view?id=123', 'text/html', '200', '', '123')
COLLISION = ('20080101000000', 'http://www.liveleak+quran.com/view?id=not-the-domain', 'text/html', '200', '', '20')
FOREIGN = ('20080101000000', 'http://www.entirely-unrelated.org/other', 'text/html', '200', '', '9')
TARGET = '*.liveleak.com/*'
START, END = '20070101000000', '20111231235959'


def config(root):
    return ProjectConfig(root, targets=[TARGET], keywords=[], from_date='2007', to_date='2011',
                         text_collapse_scope='range', cdx_collapses=[], cdx_delay=0,
                         network=NetworkConfig(index_strategy='resume', trust_environment=False)).normalized()


class ScopeTests(unittest.TestCase):
    def test_opt_in_removes_only_adjacent_cdx_host_prefix_spill(self):
        page = [GOOD, COLLISION, GOOD]
        excluded = validate_cdx_scope(page, TARGET, START, END, ['statuscode:200'],
                                      discard_adjacent_surt_hosts=True)
        self.assertEqual(excluded, 1)
        self.assertEqual(page, [GOOD, GOOD])

    def test_entire_page_of_foreign_hosts_is_not_silently_checkpointed(self):
        with self.assertRaisesRegex(MalformedCDXResponse, 'only adjacent out-of-scope'):
            validate_cdx_scope([COLLISION], TARGET, START, END, ['statuscode:200'],
                               discard_adjacent_surt_hosts=True)

    def test_unmodified_public_validation_stays_strict(self):
        with self.assertRaisesRegex(MalformedCDXResponse, 'unrelated host'):
            validate_cdx_scope([GOOD, COLLISION], TARGET, START, END, ['statuscode:200'])

    def test_arbitrary_unrelated_host_cannot_be_silently_filtered(self):
        with self.assertRaisesRegex(MalformedCDXResponse, 'unrelated host'):
            validate_cdx_scope([GOOD, FOREIGN], TARGET, START, END, ['statuscode:200'],
                               discard_adjacent_surt_hosts=True)

    def test_same_host_with_wrong_suffix_is_not_adjacent(self):
        counterfeit = ('20080101000000','http://liveleak.com.evil.invalid/x','text/html','200','','1')
        with self.assertRaises(MalformedCDXResponse):
            validate_cdx_scope([GOOD, counterfeit], TARGET, START, END, [],
                               discard_adjacent_surt_hosts=True)

    def test_timestamp_status_and_path_corruption_still_fail(self):
        for broken in [
            ('19980101000000', COLLISION[1], 'text/html', '200', '', '10'),
            ('20080101000000', COLLISION[1], 'text/html', '404', '', '10'),
            ('20080101000000', 'http://www.liveleak.com/wrong', 'text/html', '200', '', '10'),
        ]:
            with self.subTest(broken=broken):
                target = TARGET if broken[1] != 'http://www.liveleak.com/wrong' else '*.liveleak.com/view'
                with self.assertRaises(MalformedCDXResponse):
                    validate_cdx_scope([GOOD, broken], target, START, END, ['statuscode:200'],
                                       'prefix' if target != TARGET else '',
                                       discard_adjacent_surt_hosts=True)

    def test_large_in_scope_page_is_preserved_in_place(self):
        rows = [(GOOD[0], f'http://www.liveleak.com/view?id={n}', *GOOD[2:]) for n in range(10000)]
        old_id = id(rows)
        self.assertEqual(validate_cdx_scope(rows, TARGET, START, END, [],
                                            discard_adjacent_surt_hosts=True), 0)
        self.assertEqual(id(rows), old_id)
        self.assertEqual(len(rows), 10000)


class AtomicIndexTests(unittest.TestCase):
    def test_mixed_host_cursor_progresses_and_exactly_one_target_is_saved(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(Path(temp))
            db = open_database(cfg.output_dir)
            events = []
            calls = []

            def fetch(_client, _endpoints, params, **kwargs):
                calls.append(dict(params))
                if len(calls) == 1:
                    return CDXRows([GOOD, COLLISION], 'NEXT-CURSOR')
                return CDXRows([])
            try:
                with patch.object(HttpClient, 'get_cdx_any', fetch):
                    index_archive(cfg, db, threading.Event(), events.append)
                self.assertEqual(len(calls), 2)
                self.assertEqual(calls[1]['resumeKey'], 'NEXT-CURSOR')
                saved = db.execute('SELECT original_url FROM captures').fetchall()
                self.assertEqual([r[0] for r in saved], [GOOD[1]])
                coverage = db.execute('SELECT seen,complete FROM index_coverage').fetchone()
                self.assertEqual(tuple(coverage), (2,1))
                self.assertTrue(any('adjacent out-of-scope host' in e.message for e in events))
                page = next(e for e in events if e.detail.get('out_of_scope_excluded') == 1)
                self.assertEqual(page.detail['received'],2)
                self.assertEqual(page.detail['stored'],1)
                self.assertEqual(page.detail['out_of_scope_excluded'],1)
                for key in ('database_seconds','upsert_seconds','checkpoint_seconds','commit_seconds'):
                    self.assertGreaterEqual(page.detail[key],0)
                audit = db.execute("SELECT COUNT(*) FROM network_events WHERE message LIKE '%adjacent out-of-scope host%'").fetchone()[0]
                self.assertGreaterEqual(audit,1)
            finally:
                db.close()

    def test_unknown_foreign_host_keeps_original_cursor_incomplete(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(Path(temp))
            db = open_database(cfg.output_dir)
            stop = threading.Event()
            seen = []
            def fetch(_client, _endpoints, params, **kwargs):
                seen.append(dict(params))
                if len(seen) == 1:
                    return CDXRows([GOOD], 'SAFE')
                stop.set()
                return CDXRows([GOOD, FOREIGN], 'UNSAFE')
            try:
                with patch.object(HttpClient,'get_cdx_any',fetch):
                    from archive_scout.events import Stopped
                    with self.assertRaises(Stopped):
                        index_archive(cfg,db,stop)
                state = db.execute('SELECT plan_json,complete,seen FROM index_coverage').fetchone()
                from archive_scout.cdx.indexer import decode_plan
                cursor = decode_plan(state['plan_json'], []).pending[0].resume_key
                self.assertEqual(cursor,'SAFE')
                self.assertEqual(tuple(state)[1:],(0,1))
                self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0],1)
            finally:
                db.close()


if __name__ == '__main__':
    unittest.main()
