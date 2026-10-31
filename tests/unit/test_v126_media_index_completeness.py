"""Missing-membership regressions through production CDX parsing and durable indexing."""
from __future__ import annotations

import json
import re
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import HttpClient, MalformedCDXResponse
from archive_scout.cdx.indexer import index_archive, save_coverage_state, save_state, decode_plan
from archive_scout.cdx.parameters import build_cdx_params, cdx_query_signature
from archive_scout.config import ProjectConfig, MediaConfig, NetworkConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_captures, get_or_create_media_target
from archive_scout.downloads.rate_limit import FixedRateLimiter
from archive_scout.events import Stopped
from archive_scout.media.extensions import allowed_media_url
from archive_scout.media.indexer import (build_media_params, extension_filter_regex, _accept_media_rows,
    index_media, _reuse_completed_main_index, _save_media_state, media_query_signature, media_index_state_signature)
from archive_scout.network.transports import TransportResponse

HEADER = ['urlkey', 'timestamp', 'original', 'mimetype', 'statuscode', 'digest', 'length']
STAMP = '20080101000000'


def row(url='http://example.org/a.jpg', mime='image/jpeg'):
    return (STAMP, url, mime, '200', 'D', '12')


def json_body(rows, cursor=None):
    body = [HEADER] + [['org,example)/' + str(n), *item] for n, item in enumerate(rows)]
    if cursor:
        body += [[], [cursor]]
    return json.dumps(body).encode()


def text_body(rows, cursor=None):
    data = b''.join(f'org,example)/{n} {t} {m} {s} {d} {length} {u}\n'.encode()
                    for n, (t, u, m, s, d, length) in enumerate(rows))
    return data + (f'\n{cursor}\n'.encode() if cursor else b'')


class Transport:
    def __init__(self, bodies):
        self.bodies = iter(bodies)
        self.urls = []

    def request(self, url, headers, max_bytes, stop_event):
        self.urls.append(url)
        body = next(self.bodies)
        return TransportResponse(200, {}, url, body, 'fixture', .001)

    def close(self):
        pass


def client(transport):
    return HttpClient(FixedRateLimiter(0), 1, 1, 'test', threading.Event(), transport=transport)


class MissingMediaMembership(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.cfg = ProjectConfig(Path(self.temp.name), targets=['example.org/*'], keywords=['needle'],
            from_date='2008', to_date='2008', cdx_collapses=[], cdx_delay=0,
            network=NetworkConfig(index_strategy='resume', trust_environment=False),
            media=MediaConfig(enabled=True, discover_embedded=False, snapshot_strategy='all',
                              include_extensions=['jpg', 'wmv'])).normalized()
        self.db = open_database(self.cfg.output_dir)

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def run_media(self, transport, stop=None):
        c = client(transport)
        stop = stop or threading.Event()
        c.stop_event = stop
        with patch('archive_scout.media.indexer.HttpClient', return_value=c):
            return index_media(self.cfg, self.db, stop)

    def test_short_clean_prefix_recovers_missing_media_and_continues(self):
        rows = [row(), row('http://example.org/b.jpg')]
        transport = Transport([text_body(rows[:1]), json_body(rows, 'NEXT'), text_body([]), json_body([]), b'1', json_body(rows)])
        self.run_media(transport)
        self.assertEqual({r[0] for r in self.db.execute('SELECT original_url FROM media_captures')}, {r[1] for r in rows})
        self.assertEqual(len(transport.urls), 6)
        self.assertTrue(all('resumeKey=NEXT' in u for u in transport.urls[2:4]))

    def test_false_empty_media_response_is_not_empty_inventory(self):
        transport = Transport([b'', json_body([row()]), b'1', json_body([row()])])
        self.run_media(transport)
        self.assertEqual(self.db.execute('SELECT original_url FROM media_captures').fetchone()[0], row()[1])

    def test_identical_scan_limited_responses_do_not_hide_later_media(self):
        first, hidden = row(), row('http://example.org/after-scan-ceiling.jpg')
        transport = Transport([text_body([first]), json_body([first]), b'2', json_body([first]), json_body([hidden])])
        self.run_media(transport)
        self.assertEqual({r[0] for r in self.db.execute('SELECT original_url FROM media_captures')}, {first[1], hidden[1]})
        self.assertEqual(len(transport.urls), 5)

    def test_numbered_media_pages_reconcile_merged_only_url(self):
        cfg = replace(self.cfg, network=replace(self.cfg.network, index_strategy='paged'))
        from archive_scout.cdx.client import CDXRows
        calls = []
        def fetch(c, endpoints, params, **kwargs):
            p = dict(params); calls.append(p)
            if p.get('showNumPages') == 'true': return 1
            if 'page' in p: return CDXRows([row()])
            return CDXRows([row(), row('http://example.org/merged-only.jpg')])
        with patch.object(HttpClient, 'get_cdx_any', fetch): index_media(cfg, self.db, threading.Event())
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0], 2)
        self.assertTrue(any('page' in p for p in calls))
        self.assertTrue(any('page' not in p and 'showNumPages' not in p for p in calls))

    def test_disagreement_keeps_original_media_cursor(self):
        transport = Transport([text_body([row()], 'SAFE'), text_body([row('http://example.org/b.jpg')]),
                               json_body([row('http://example.org/c.jpg')])])
        c = client(transport); stop = threading.Event(); c.stop_event = stop
        original = c.get_cdx_json_rows_any
        def confirm(*args, **kwargs):
            result = original(*args, **kwargs); stop.set(); return result
        with patch.object(c, 'get_cdx_json_rows_any', side_effect=confirm), \
             patch('archive_scout.media.indexer.HttpClient', return_value=c):
            with self.assertRaises(Stopped): index_media(self.cfg, self.db, stop)
        state = self.db.execute('SELECT complete,resume_key FROM media_index_state').fetchone()
        self.assertEqual(state['complete'], 0)
        self.assertEqual(decode_plan(state['resume_key'], []).pending[0].resume_key, 'SAFE')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0], 1)

    def test_live_media_cursors_do_not_add_confirmation_requests(self):
        transport = Transport([text_body([row()], 'NEXT')]); c = client(transport)
        params = build_media_params(self.cfg, self.cfg.targets[0], self.cfg.from_date, self.cfg.to_date, extensions=['.jpg'])
        result = c.get_cdx_rows_any(('https://fixture.invalid/cdx',), params, verify_terminal=True)
        self.assertEqual(result.resume_key, 'NEXT'); self.assertEqual(len(transport.urls), 1); c.close()

    def test_text_client_default_does_not_enable_media_verification(self):
        transport = Transport([text_body([row()])]); c = client(transport)
        result = c.get_cdx_rows_any(('https://fixture.invalid/cdx',),
            build_cdx_params(self.cfg, self.cfg.targets[0], self.cfg.from_date, self.cfg.to_date))
        self.assertEqual(result.rows, [row()]); self.assertFalse(result.needs_paged_check)
        self.assertEqual(len(transport.urls), 1); c.close()

    def test_normal_text_indexing_keeps_its_existing_request_path(self):
        transport = Transport([text_body([row()])]); c = client(transport)
        with patch('archive_scout.cdx.indexer._client_for_config', return_value=c):
            index_archive(self.cfg, self.db, threading.Event())
        self.assertEqual(len(transport.urls), 1)
        self.assertEqual(self.db.execute('SELECT complete FROM index_coverage').fetchone()[0], 1)

    def test_encoded_extensions_and_extensionless_mime_survive_local_selection(self):
        urls = ['http://example.org/a.jpg', 'http://example.org/a.%6a%70%67',
                'http://example.org/a%2E%4A%50%47', 'http://example.org/image',
                'http://example.org/a.jpg&ref=thumb', 'http://example.org/a.jpg%3Fref=x']
        for url in urls:
            with self.subTest(url=url):
                self.assertTrue(allowed_media_url(url, self.cfg.media, 'image/jpeg')[0])
                self.assertEqual(len(_accept_media_rows([row(url)], self.cfg.media)), 1)
        self.assertIsNone(re.fullmatch(extension_filter_regex(['.jpg']), urls[1]))
        self.assertIsNone(re.fullmatch(extension_filter_regex(['.jpg']), urls[3]))

    def test_local_selection_keeps_include_exclude_policy(self):
        for images, videos in ((True, False), (False, True), (True, True)):
            media = replace(self.cfg.media, include_images=images, include_videos=videos)
            for mime in ('image/jpeg', 'video/x-ms-wmv', 'text/html'):
                for url in ('http://example.org/a.jpg', 'http://example.org/a%2e%77%6d%76', 'http://example.org/opaque'):
                    self.assertEqual(bool(_accept_media_rows([row(url, mime)], media)),
                                     allowed_media_url(url, media, mime)[0])
        self.assertFalse(allowed_media_url('http://example.org/a.png', self.cfg.media, 'image/png')[0])

    def test_old_media_completion_reindexes_without_resetting_saved_capture(self):
        from archive_scout.database.repositories import upsert_media_captures
        from archive_scout.cdx.client import CDXRows
        sig = media_query_signature(self.cfg); mt = get_or_create_media_target(self.db, self.cfg.targets[0])
        with self.db:
            _save_media_state(self.db, mt, 2008, media_index_state_signature(self.cfg, revision=4), None, True, 1, None)
            upsert_media_captures(self.db, [(row(), 'image', '.jpg')], mt, sig)
            self.db.execute("UPDATE media_captures SET state='downloaded',path='retained.jpg'")
        before = self.db.execute('SELECT id,state,path FROM media_captures').fetchone()[:]
        with patch.object(HttpClient, 'get_cdx_any', return_value=CDXRows([row('http://example.org/hidden', 'image/jpeg')])) as fetch:
            index_media(self.cfg, self.db, threading.Event())
        self.assertGreater(fetch.call_count, 0)
        self.assertEqual(self.db.execute('SELECT id,state,path FROM media_captures WHERE original_url=?', (row()[1],)).fetchone()[:], before)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0], 2)
        with patch.object(HttpClient, 'get_cdx_any', side_effect=AssertionError('proven media repeated')):
            index_media(self.cfg, self.db, threading.Event())

    def test_media_cannot_trust_text_completion_or_change_text_checkpoint(self):
        from archive_scout.cdx.client import CDXRows
        target = get_or_create_target(self.db, self.cfg.targets[0]); sig = cdx_query_signature(self.cfg)
        with self.db:
            save_coverage_state(self.db, target, sig, self.cfg.from_date, self.cfg.to_date, None, True, 0, None)
        before = dict(self.db.execute('SELECT * FROM index_coverage').fetchone())
        with patch.object(HttpClient, 'get_cdx_any', return_value=CDXRows([row('http://example.org/opaque')])) as fetch:
            index_media(self.cfg, self.db, threading.Event())
        self.assertGreater(fetch.call_count, 0)
        self.assertEqual(self.db.execute('SELECT original_url FROM media_captures').fetchone()[0], 'http://example.org/opaque')
        self.assertEqual(dict(self.db.execute('SELECT * FROM index_coverage').fetchone()), before)

    def test_advanced_paging_rejected_only_by_media_builder(self):
        for parameter in ('offset=100', 'page=2', 'pageSize=1', 'showNumPages=true', 'showPagedIndex=true'):
            with self.subTest(parameter=parameter):
                cfg = replace(self.cfg, cdx_extra_params=[parameter]).normalized()
                self.assertIn(tuple(parameter.split('=', 1)), build_cdx_params(cfg, cfg.targets[0], cfg.from_date, cfg.to_date))
                with self.assertRaisesRegex(ValueError, 'controlled by media indexing'):
                    build_media_params(cfg, cfg.targets[0], cfg.from_date, cfg.to_date, extensions=['.jpg'])

    def test_unsupported_media_verification_keeps_scope_unverified(self):
        from archive_scout.cdx.client import CDXRows
        from archive_scout.cdx.indexer import PendingWindow
        from archive_scout.media.indexer import _request_media_resume
        current = PendingWindow(self.cfg.from_date, self.cfg.to_date, pagination_supported=False)
        result = CDXRows([row()], terminal_verified=True, needs_paged_check=True)
        with patch('archive_scout.media.indexer.request_cdx_rows', return_value=result):
            rows, finished, cursor = _request_media_resume(self.cfg, object(), self.cfg.targets[0], current, ['.jpg'])
        self.assertEqual(rows, [row()]); self.assertFalse(finished)
        self.assertTrue(current.verify_paged_next)

    def test_stop_retains_media_verification_phase(self):
        from archive_scout.cdx.indexer import encode_plan
        transport = Transport([text_body([row()]), json_body([row()])])
        c = client(transport); stop = threading.Event(); c.stop_event = stop
        original = c.get_cdx_json_rows_any
        def confirm(*args, **kwargs):
            result = original(*args, **kwargs); stop.set(); return result
        with patch.object(c, 'get_cdx_json_rows_any', side_effect=confirm), \
             patch('archive_scout.media.indexer.HttpClient', return_value=c):
            with self.assertRaises(Stopped): index_media(self.cfg, self.db, stop)
        state = self.db.execute('SELECT complete,resume_key FROM media_index_state').fetchone()
        self.assertEqual(state[0], 0)
        plan = decode_plan(state[1], [])
        self.assertTrue(plan.pending[0].merged_verified)
        self.assertEqual(plan.pending[0].strategy, 'paged')
        self.assertTrue(decode_plan(encode_plan(plan), []).pending[0].merged_verified)

    def test_failed_insert_cannot_verify_media_pass_or_lose_cursor(self):
        from archive_scout.cdx.client import CDXRows
        from archive_scout.cdx.indexer import encode_plan, IndexPlan, PendingWindow
        mt = get_or_create_media_target(self.db, self.cfg.targets[0]); state_sig = media_index_state_signature(self.cfg)
        plan = encode_plan(IndexPlan([PendingWindow(self.cfg.from_date, self.cfg.to_date, 'SAFE')], 0, 1))
        with self.db: _save_media_state(self.db, mt, 2008, state_sig, plan, False, 1, None)
        result = CDXRows([row()], terminal_verified=True, needs_paged_check=True)
        with patch('archive_scout.media.indexer.request_cdx_rows', return_value=result), \
             patch('archive_scout.media.indexer.upsert_media_captures', side_effect=RuntimeError('injected SQLite write failure')):
            with self.assertRaisesRegex(RuntimeError, 'injected SQLite'):
                index_media(self.cfg, self.db, threading.Event())
        state = self.db.execute('SELECT complete,resume_key FROM media_index_state WHERE query_signature=?', (state_sig,)).fetchone()
        self.assertEqual(state[0], 0)
        current = decode_plan(state[1], []).pending[0]
        self.assertEqual(current.resume_key, 'SAFE'); self.assertFalse(current.merged_verified)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0], 0)

    def test_media_page_count_never_interprets_capture_timestamp_as_page_count(self):
        from archive_scout.cdx.indexer import PendingWindow
        from archive_scout.media.indexer import _request_media_paged_batch
        from unittest.mock import Mock
        cfg = replace(self.cfg, network=replace(self.cfg.network, index_strategy='paged'))
        for payload in (True, False, json.loads(json_body([row()]))):
            with self.subTest(payload=payload):
                c = Mock(); c.get_cdx_any.return_value = payload
                current = PendingWindow(cfg.from_date, cfg.to_date, strategy='paged')
                with self.assertRaisesRegex(MalformedCDXResponse, 'no page layout verified'):
                    _request_media_paged_batch(cfg, c, cfg.targets[0], current, ['.jpg'], threading.Event())
                self.assertEqual(current.page_count, -1)
                self.assertEqual(c.get_cdx_any.call_count, 1)

    def test_empty_numbered_media_pages_do_not_hide_later_matches(self):
        transport = Transport([b'', json_body([]), b'3', json_body([]), json_body([]), json_body([row('http://example.org/last-page.jpg')])])
        self.run_media(transport)
        self.assertEqual(self.db.execute('SELECT original_url FROM media_captures').fetchone()[0], 'http://example.org/last-page.jpg')
        self.assertEqual(len(transport.urls), 6)

    def test_unsupported_paging_retains_discovered_media_and_rechecks_only_verification(self):
        from archive_scout.cdx.client import CDXRows, PermanentRequestError
        result = CDXRows([row()], terminal_verified=True, needs_paged_check=True)
        with patch('archive_scout.media.indexer.request_cdx_rows', return_value=result), \
             patch('archive_scout.media.indexer._request_media_paged_batch', side_effect=PermanentRequestError('HTTP 400', status=400)) as paging:
            index_media(self.cfg, self.db, threading.Event())
        self.assertEqual(paging.call_count, 1)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0], 1)
        self.assertEqual(self.db.execute('SELECT complete FROM media_index_state').fetchone()[0], 0)
        self.assertEqual(self.db.execute('SELECT category FROM errors ORDER BY id DESC').fetchone()[0], 'coverage_verification_unavailable')
        with patch('archive_scout.media.indexer.request_cdx_rows', side_effect=AssertionError('merged rows were already durable')), \
             patch('archive_scout.media.indexer._request_media_paged_batch', side_effect=PermanentRequestError('HTTP 400', status=400)) as paging:
            index_media(self.cfg, self.db, threading.Event())
        self.assertEqual(paging.call_count, 1)
