from __future__ import annotations

import hashlib
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from archive_scout.cdx.client import HttpClient, CDXRows, MalformedCDXResponse, TransientRequestError
from archive_scout.cdx.indexer import PendingWindow, IndexPlan, encode_plan, decode_plan
from archive_scout.config import ProjectConfig, MediaConfig, NetworkConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_media_target, upsert_media_captures
from archive_scout.events import Stopped
from archive_scout.media.downloader import download_media
from archive_scout.media.indexer import (_request_media_paged_batch, _save_media_state, index_media,
    media_query_signature, media_index_state_signature, MediaPaginationUnavailable, _media_page_layout)
from archive_scout.network.transports import TransportResponse
from archive_scout.operations import run_project
from .test_v126_media_index_completeness import text_body, json_body


class FastStop(threading.Event):
    def wait(self, timeout=None):
        return self.is_set()


class Scenario:
    def __init__(self, count_mode='invalid', bad_exact=0):
        self.calls = []
        self.count_mode = count_mode
        self.bad_exact = bad_exact
        self.exact_attempts = {}
        self.urls = ['http://www.phoenixtv.com/a.jpg', 'http://www.phoenixtv.com/b.jpg']
        self.rows = [('20020101000000', u, 'image/jpeg', '200', '', '4') for u in self.urls]

    def request(self, url, headers, max_bytes, stop_event):
        q = parse_qs(urlsplit(url).query)
        self.calls.append((url, q))
        status = 200
        if q.get('matchType') == ['exact']:
            original = q['url'][0]
            attempt = self.exact_attempts.get(original, 0) + 1
            self.exact_attempts[original] = attempt
            if original == self.urls[-1] and attempt <= self.bad_exact:
                return TransportResponse(504, {}, url, b'gateway timeout', 'fixture', .001)
            stamp = '20031201000000' if q.get('limit') == ['-1'] else '20011001000000'
            body_rows = [(stamp, original, 'image/jpeg', '200', '', '4')]
            body = text_body(body_rows) if q.get('output') == ['txt'] else json_body(body_rows)
        elif q.get('showNumPages') == ['true']:
            if self.count_mode == 'transient':
                return TransportResponse(504, {}, url, b'gateway timeout', 'fixture', .001)
            native = '/cdx/search/cdx' in url
            body = b'1' if self.count_mode == 'native' and native else json_body(self.rows)
        elif 'page' in q:
            body = json_body(self.rows)
        elif q.get('resumeKey') == ['NEXT']:
            body = text_body([]) if q.get('output') == ['txt'] else json_body([])
        else:
            body = text_body(self.rows, 'NEXT') if q.get('output') == ['txt'] else json_body(self.rows, 'NEXT')
        return TransportResponse(status, {}, url, body, 'fixture', .001)

    def close(self):
        pass


class MediaRevertIndexingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.cfg = ProjectConfig(self.root, targets=['*.phoenixtv.com/*'], keywords=['needle'],
            from_date='20010911', to_date='20031231', text_collapse_scope='range',
            cdx_collapses=[], cdx_delay=0, download_delay=0, max_attempts=3, workers=2,
            network=NetworkConfig(index_strategy='resume', trust_environment=False, cdx_workers=2,
                                  retry_base_seconds=1, retry_max_seconds=1),
            media=MediaConfig(enabled=True, discover_embedded=False, snapshot_strategy='earliest',
                              include_extensions=['jpg'])).normalized()
        self.db = open_database(self.root)
        self.events, self.saved = [], []
        self.sig = media_query_signature(self.cfg)
        self.target = get_or_create_media_target(self.db, self.cfg.targets[0])
        self.db.commit()

    def tearDown(self):
        self.db.close()
        self.temp.cleanup()

    def client_patch(self, scenario):
        original = HttpClient.__init__
        def initialize(instance, *args, **kwargs):
            kwargs['transport'] = scenario
            original(instance, *args, **kwargs)
        return patch.object(HttpClient, '__init__', initialize)

    def fetch(self, row, cfg, client, destination):
        self.saved.append((row['original_url'], row['timestamp']))
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b'test')
        return dict(id=row['id'], path=destination, bytes=4, hash=hashlib.sha256(b'test').hexdigest(), status=200, final_url=row['original_url'])

    def seed(self, rows):
        with self.db:
            upsert_media_captures(self.db, [(r, 'image', '.jpg') for r in rows], self.target, self.sig)


    def test_native_paging_fallback_has_bound_endpoint_and_distinct_ledger(self):
        scenario = Scenario(count_mode='native')
        with self.client_patch(scenario):
            index_media(self.cfg, self.db, FastStop(), self.events.append)
        self.assertEqual(self.db.execute('SELECT complete FROM media_index_state').fetchone()[0], 1)
        pages = [(url, q) for url, q in scenario.calls if 'page' in q]
        self.assertTrue(pages)
        self.assertTrue(all('/cdx/search/cdx' in url for url, _ in pages))
        layout = self.db.execute('SELECT layout_signature FROM media_index_pages').fetchone()[0]
        self.assertIn(':', layout)
        self.assertFalse(scenario.exact_attempts)

    def test_transient_paging_is_not_relabelled_as_unsupported(self):
        scenario = Scenario(count_mode='transient')
        with self.client_patch(scenario), patch('archive_scout.media.indexer._wait_seconds', return_value=0):
            index_media(self.cfg, self.db, FastStop(), self.events.append)
        counts = [url for url, q in scenario.calls if 'showNumPages' in q]
        self.assertEqual(len(counts), 3)
        self.assertTrue(all('/web/timemap/' in url for url in counts))
        self.assertTrue(any('504' in e.message for e in self.events))
        plan = decode_plan(self.db.execute('SELECT resume_key FROM media_index_state').fetchone()[0], [])
        self.assertTrue(plan.pending[0].pagination_supported)
        self.assertTrue(plan.pending[0].merged_verified)




    def test_index_only_does_not_make_exact_selection_or_replay_requests(self):
        scenario = Scenario()
        with self.client_patch(scenario), patch('archive_scout.media.downloader.fetch_media') as fetch:
            run_project(self.cfg, 'media_index', FastStop(), self.events.append)
        fetch.assert_not_called()
        self.assertFalse(scenario.exact_attempts)

    def test_all_snapshots_keep_partial_inventory_without_selection_lookups(self):
        self.cfg = replace(self.cfg, media=replace(self.cfg.media, snapshot_strategy='all'))
        self.sig = media_query_signature(self.cfg)
        scenario = Scenario()
        self.seed(scenario.rows)
        with self.client_patch(scenario), patch('archive_scout.media.downloader.fetch_media', side_effect=self.fetch):
            download_media(self.cfg, self.db, FastStop(), self.events.append, allow_partial_inventory=True)
        self.assertEqual(len(self.saved), 2)
        self.assertFalse(scenario.calls)




    def test_page_endpoint_survives_checkpoint_roundtrip(self):
        from archive_scout.constants import CDX_URL
        window = PendingWindow(self.cfg.from_date, self.cfg.to_date, strategy='paged', page_endpoint=CDX_URL)
        restored = decode_plan(encode_plan(IndexPlan([window], 0, 1)), []).pending[0]
        self.assertEqual(restored.page_endpoint, CDX_URL)
        self.assertEqual(_media_page_layout(self.cfg, restored), _media_page_layout(self.cfg, window))




    def test_complete_inventory_skips_exact_lookups(self):
        scenario = Scenario()
        self.seed(scenario.rows)
        with self.db:
            _save_media_state(self.db, self.target, 0, media_index_state_signature(self.cfg), None, True, 2, None)
        with self.client_patch(scenario), patch('archive_scout.media.downloader.fetch_media', side_effect=self.fetch):
            download_media(self.cfg, self.db, FastStop(), self.events.append)
        self.assertEqual(len(self.saved), 2)
        self.assertEqual(scenario.calls, [])



    def test_endpoint_fallback_does_not_reuse_primary_page_ledger(self):
        scenario = Scenario(count_mode='native')
        current = PendingWindow(self.cfg.from_date, self.cfg.to_date, strategy='paged')
        completed = {0}
        c = HttpClient.__new__(HttpClient)
        c.get_cdx_any = lambda endpoints, params, **kwargs: ([['timestamp','original'],['20020101000000','x']] if '/timemap/' in endpoints[0] else 1)
        c.get_cdx_json_any = c.get_cdx_any
        from archive_scout.cdx.parallel import PageFetchResult
        def pages(_client, endpoints, requested, *args, **kwargs):
            self.assertEqual(requested, [0])
            self.assertIn('/cdx/', endpoints[0])
            yield PageFetchResult(page=0, rows=scenario.rows, elapsed=.001)
        with patch('archive_scout.media.indexer.iter_cdx_pages', side_effect=pages):
            result = _request_media_paged_batch(self.cfg, c, self.cfg.targets[0], current, ['.jpg'], FastStop(), completed_pages=completed)
        self.assertEqual(completed, set())
        self.assertEqual(len(result.successful), 1)



    def test_capture_text_cannot_become_a_trillion_page_count(self):
        from archive_scout.media.indexer import _media_page_count
        scenario = Scenario()
        class CaptureText:
            def request(self, url, *args):
                return TransportResponse(200, {}, url, text_body(scenario.rows), 'fixture', .001)
            def close(self):
                pass
        from archive_scout.downloads.rate_limit import FixedRateLimiter
        c = HttpClient(FixedRateLimiter(0), 1, 1, 'test', FastStop(), transport=CaptureText())
        with self.assertRaises(TransientRequestError):
            _media_page_count(c, ('https://fixture.invalid/cdx',), [('output','json'),('showNumPages','true')])
        c.close()

    def test_media_all_uses_original_bulk_selection_without_exact_checks(self):
        scenario = Scenario(count_mode='native')
        with self.client_patch(scenario), patch('archive_scout.media.downloader.fetch_media', side_effect=self.fetch):
            paths = run_project(self.cfg, 'media_all', FastStop(), self.events.append)
        self.assertEqual(paths.status, 'complete')
        self.assertEqual(set(self.saved), {(url, '20020101000000') for url in scenario.urls})
        self.assertEqual(scenario.exact_attempts, {})
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_discovery_queue').fetchone()[0], 0)

    def test_incomplete_index_and_old_private_proofs_do_not_add_lookup_requests(self):
        scenario = Scenario()
        self.seed(scenario.rows)
        with self.db:
            for url in scenario.urls:
                self.db.execute("INSERT INTO media_discovery_queue(query_signature,original_url,source_type,state,created_at,updated_at) VALUES(?,?,'cdx_selection_proof','indexed','now','now')", ('direct-media-selection-v1:' + self.sig + ':20020101000000', url))
        before = [tuple(row) for row in self.db.execute('SELECT id,query_signature,original_url,state FROM media_discovery_queue ORDER BY id')]
        with self.client_patch(scenario), patch('archive_scout.media.downloader.fetch_media', side_effect=self.fetch):
            download_media(self.cfg, self.db, FastStop(), self.events.append, allow_partial_inventory=True)
        self.assertEqual(self.saved, [])
        self.assertEqual(scenario.calls, [])
        self.assertEqual(before, [tuple(row) for row in self.db.execute('SELECT id,query_signature,original_url,state FROM media_discovery_queue ORDER BY id')])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM media_captures WHERE state='pending'").fetchone()[0], 2)

    def test_media_reports_match_old_flow_without_selection_export(self):
        from archive_scout.media.reports import generate_media_reports
        self.seed(Scenario().rows)
        paths = generate_media_reports(self.cfg, self.db)
        self.assertNotIn('media_selection_unfinished', paths)
