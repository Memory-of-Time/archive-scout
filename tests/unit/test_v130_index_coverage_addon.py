"""Mixed-host CDX coverage and committed traversal regression tests."""
from __future__ import annotations

import tempfile
import threading
import unittest
import sqlite3
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import CDXRows, HttpClient, MalformedCDXResponse, TransientRequestError
from archive_scout.cdx.indexer import decode_plan, index_archive, validate_cdx_scope
from archive_scout.cdx.parallel import PageFetchResult
from archive_scout.config import MediaConfig, NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import upsert_captures
from archive_scout.events import Stopped
from archive_scout.media.indexer import index_direct_media, media_index_state_signature, media_query_signature


TARGET = '*.ogrish.com/*'
START, END = '20000101000000', '20091231235959'


def row(number=0, host='www.ogrish.com', year=2005, media=False):
    return (f'{year}0101000000', f'http://{host}/{number}' + ('.jpg' if media else ''),
            'image/jpeg' if media else 'text/html', '200', '', '10')


def config(root, **kwargs):
    return ProjectConfig(Path(root), targets=[TARGET], keywords=['needle'],
        from_date=START, to_date=END, cdx_collapses=[], cdx_delay=0,
        network=NetworkConfig(index_strategy='resume', trust_environment=False),
        **kwargs).normalized()


class ScopeAnomalyTests(unittest.TestCase):
    def test_mixed_empty_label_hosts_are_excluded_without_rewriting_originals(self):
        for host in ('ogrish..com', 'www.ogrish..com', 'ogrish...com'):
            with self.subTest(host=host):
                good = row()
                page = [good, row(1, host=host), row(2)]
                identity = id(page)
                count = validate_cdx_scope(page, TARGET, START, END, ['statuscode:200'],
                                           discard_adjacent_surt_hosts=True)
                self.assertEqual(count, 1)
                self.assertEqual(id(page), identity)
                self.assertEqual(page, [good, row(2)])

    def test_original_already_inside_selected_host_boundary_is_preserved(self):
        original = row(1, host='www..ogrish.com')
        page = [row(), original]
        self.assertEqual(validate_cdx_scope(page, TARGET, START, END, [],
                         discard_adjacent_surt_hosts=True), 0)
        self.assertEqual(page[1], original)

    def test_unrelated_and_suffix_spoof_hosts_still_reject_the_page(self):
        for host in ('other..com', 'ogrish..com.evil.invalid', 'evil.ogrish.net'):
            with self.subTest(host=host), self.assertRaises(MalformedCDXResponse):
                validate_cdx_scope([row(), row(1, host=host)], TARGET, START, END, [],
                                   discard_adjacent_surt_hosts=True)

    def test_strict_callers_do_not_accept_empty_label_hosts(self):
        with self.assertRaises(MalformedCDXResponse):
            validate_cdx_scope([row(), row(1, host='ogrish..com')], TARGET, START, END, [])

    def test_anomaly_only_response_never_proves_completion(self):
        with self.assertRaisesRegex(MalformedCDXResponse, 'only adjacent out-of-scope'):
            validate_cdx_scope([row(1, host='ogrish..com')], TARGET, START, END, [],
                               discard_adjacent_surt_hosts=True)

    def test_bad_dates_and_status_are_not_hidden_by_host_exclusion(self):
        for bad in (row(1, host='ogrish..com', year=1999),
                    (*row(1, host='ogrish..com')[:3], '404', '', '10')):
            with self.subTest(bad=bad), self.assertRaises(MalformedCDXResponse):
                validate_cdx_scope([row(), bad], TARGET, START, END, ['statuscode:200'],
                                   discard_adjacent_surt_hosts=True)


class CoverageTests(unittest.TestCase):
    def test_failed_database_write_retains_last_committed_cursor_and_retries_whole_page(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(temp, text_collapse_scope='range')
            db = open_database(cfg.output_dir)
            calls = []

            def response(_client, _endpoints, params, **kwargs):
                calls.append(dict(params))
                if len(calls) == 1:
                    return CDXRows([row()], 'COMMITTED')
                return CDXRows([row(1), row(2, 'ogrish..com')])

            def write(database, rows, *args):
                changed = upsert_captures(database, rows, *args)
                if len(calls) == 2:
                    raise sqlite3.OperationalError('injected write failure before checkpoint')
                return changed

            try:
                with patch.object(HttpClient, 'get_cdx_any', response), \
                     patch('archive_scout.cdx.indexer.upsert_captures', write):
                    with self.assertRaisesRegex(sqlite3.OperationalError, 'injected write failure'):
                        index_archive(cfg, db, threading.Event())
                state = db.execute('SELECT plan_json,complete,seen FROM index_coverage').fetchone()
                self.assertEqual((state['complete'], state['seen']), (0, 1))
                self.assertEqual(decode_plan(state['plan_json'], []).pending[0].resume_key, 'COMMITTED')
                self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 1)
                with patch.object(HttpClient, 'get_cdx_any', response):
                    self.assertFalse(index_archive(cfg, db, threading.Event()))
                self.assertEqual([p.get('resumeKey') for p in calls], [None, 'COMMITTED', 'COMMITTED'])
                self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 2)
            finally:
                db.close()

    def test_large_first_page_commits_then_reopen_continues_at_exact_cursor(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(temp, text_collapse_scope='range')
            db = open_database(cfg.output_dir)
            calls, events = [], []
            stop = threading.Event()

            def first(_client, _endpoints, params, **kwargs):
                calls.append(dict(params))
                stop.set()
                return CDXRows([row(i) for i in range(10000)] + [row(20000, 'ogrish..com')],
                               'EXACT+CURSOR%2F')

            try:
                with patch.object(HttpClient, 'get_cdx_any', first):
                    with self.assertRaises(Stopped):
                        index_archive(cfg, db, stop, events.append)
                state = db.execute('SELECT plan_json,seen,complete FROM index_coverage').fetchone()
                self.assertEqual((state['seen'], state['complete']), (10001, 0))
                self.assertEqual(decode_plan(state['plan_json'], []).pending[0].resume_key, 'EXACT+CURSOR%2F')
                db.close()
                db = open_database(cfg.output_dir)

                def finish(_client, _endpoints, params, **kwargs):
                    calls.append(dict(params))
                    return CDXRows([row(10000), row(20001, 'ogrish..com'), row(10001)])

                with patch.object(HttpClient, 'get_cdx_any', finish):
                    self.assertFalse(index_archive(cfg, db, threading.Event(), events.append))
                self.assertEqual([p.get('resumeKey') for p in calls], [None, 'EXACT+CURSOR%2F'])
                self.assertTrue(all((p['from'], p['to']) == (START, END) for p in calls))
                self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 10002)
                self.assertEqual(tuple(db.execute('SELECT seen,complete FROM index_coverage').fetchone()), (10004, 1))
                self.assertEqual(sum(e.detail.get('out_of_scope_excluded', 0) for e in events), 2)
                self.assertEqual(db.execute("SELECT COUNT(*) FROM network_events WHERE message LIKE '%discarded 1 adjacent%'").fetchone()[0], 2)
                with patch.object(HttpClient, 'get_cdx_any', side_effect=AssertionError('completed range repeated')):
                    self.assertFalse(index_archive(cfg, db, threading.Event()))
            finally:
                db.close()

    def test_yearly_resume_never_refetches_completed_years_or_skips_later_years(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(temp, text_collapse_scope='year')
            db = open_database(cfg.output_dir)
            stop = threading.Event()
            calls = []

            def first(_client, _endpoints, params, **kwargs):
                p = dict(params); calls.append(p)
                year = int(p['from'][:4])
                if year == 2005:
                    stop.set()
                    return CDXRows([row(1, year=year), row(2, 'ogrish..com', year)], 'YEAR-2005')
                return CDXRows([row(year=year)])

            try:
                with patch.object(HttpClient, 'get_cdx_any', first):
                    with self.assertRaises(Stopped):
                        index_archive(cfg, db, stop)
                self.assertEqual([p['from'][:4] for p in calls], [str(y) for y in range(2000, 2006)])
                db.close(); db = open_database(cfg.output_dir)
                resumed = []

                def finish(_client, _endpoints, params, **kwargs):
                    p = dict(params); resumed.append(p)
                    return CDXRows([row(3, year=int(p['from'][:4]))])

                with patch.object(HttpClient, 'get_cdx_any', finish):
                    self.assertFalse(index_archive(cfg, db, threading.Event()))
                self.assertEqual([p['from'][:4] for p in resumed], [str(y) for y in range(2005, 2010)])
                self.assertEqual(resumed[0]['resumeKey'], 'YEAR-2005')
                self.assertEqual(db.execute('SELECT SUM(complete),COUNT(*) FROM index_state').fetchone()[:], (10, 10))
                self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 11)
            finally:
                db.close()

    def test_repeated_oversize_at_minimum_page_never_rewinds_saved_cursor(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(temp, text_collapse_scope='range', page_size=100, max_attempts=8)
            db = open_database(cfg.output_dir)
            calls = []

            def response(_client, _endpoints, params, **kwargs):
                calls.append(dict(params))
                if len(calls) == 1:
                    return CDXRows([row()], 'KEEP-ME')
                if len(calls) < 7:
                    raise TransientRequestError('response exceeds safe in-memory budget', splittable=True)
                return CDXRows([row(1)])

            try:
                with patch.object(HttpClient, 'get_cdx_any', response), \
                     patch('archive_scout.cdx.indexer.transient_backoff', return_value=0):
                    self.assertFalse(index_archive(cfg, db, threading.Event()))
                self.assertEqual([p.get('resumeKey') for p in calls], [None] + ['KEEP-ME'] * 6)
                self.assertTrue(all((p['from'], p['to']) == (START, END) for p in calls))
                self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 2)
            finally:
                db.close()

    def test_media_mixed_pages_retain_selected_media_and_continuation(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(temp, text_collapse_scope='range', media=MediaConfig(
                enabled=True, include_extensions=['jpg'], discover_embedded=False))
            db = open_database(cfg.output_dir)
            calls = []

            def response(_client, _endpoints, params, **kwargs):
                calls.append(dict(params))
                n = len(calls)
                return CDXRows([row(n, media=True), row(n, 'ogrish..com', media=True)],
                               'MEDIA-SAVED' if n == 1 else None)

            try:
                with patch.object(HttpClient, 'get_cdx_any', response):
                    client = HttpClient.__new__(HttpClient)
                    self.assertFalse(index_direct_media(cfg, db, client, threading.Event(), None,
                        media_query_signature(cfg), media_index_state_signature(cfg)))
                self.assertEqual([p.get('resumeKey') for p in calls], [None, 'MEDIA-SAVED'])
                self.assertEqual(db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0], 2)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 0)
                self.assertEqual(db.execute('SELECT seen,complete FROM media_index_state').fetchone()[:], (4, 1))
            finally:
                db.close()

    def test_numbered_pages_commit_good_rows_and_all_page_ledgers(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg = config(temp, text_collapse_scope='range')
            cfg = replace(cfg, network=replace(cfg.network, index_strategy='paged'))
            db = open_database(cfg.output_dir)
            requested = []

            def pages(_client, _endpoints, page_numbers, *_args, **kwargs):
                for page in page_numbers:
                    requested.append(page)
                    yield PageFetchResult(page, [row(page), row(20 + page, 'ogrish..com')], .01)

            try:
                with patch('archive_scout.cdx.indexer._request_paged_count', return_value=2), \
                     patch('archive_scout.cdx.indexer.iter_cdx_pages', pages):
                    self.assertFalse(index_archive(cfg, db, threading.Event()))
                self.assertEqual(sorted(requested), [0, 1])
                self.assertEqual(db.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 2)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM index_pages WHERE status=\'complete\'').fetchone()[0], 2)
                self.assertEqual(db.execute('SELECT complete FROM index_coverage').fetchone()[0], 1)
            finally:
                db.close()


if __name__ == '__main__':
    unittest.main()
