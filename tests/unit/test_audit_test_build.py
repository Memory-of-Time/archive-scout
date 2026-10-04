from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
import time
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from archive_scout.cdx.client import HttpClient, MalformedCDXResponse, RateLimitDeferred, parse_json_response
from archive_scout.cdx.parameters import (
    _legacy_cdx_query_signature, adopt_compatible_index_state, cdx_query_signature, preferred_index_strategy,
)
from archive_scout.config import MediaConfig, ProjectConfig, load_project_config, save_project_config
from archive_scout.content import classify_replay_content
from archive_scout.database.connection import _pid_is_alive, open_database
from archive_scout.database.repositories import (
    get_or_create_media_target, get_or_create_target, start_operation_run, upsert_media_captures,
)
from archive_scout.downloads.downloader import _acquire_archive, download_archive, prepare_acquisition_rows
from archive_scout.downloads.rate_limit import (
    FixedRateLimiter,
    SharedFixedRateLimiter,
    SharedHostGate,
    WAYBACK_INDEX_RATE_KEY,
    WAYBACK_REPLAY_RATE_KEY,
    reset_shared_traffic_state_for_tests,
)
from archive_scout.network.transports import TransportResponse
from archive_scout.media import indexer as media_indexer
from archive_scout.media.indexer import (
    MEDIA_DISCOVERY_REVISION,
    _discover_embedded_queue,
    _adopt_compatible_media_state,
    _cached_embedded_inventory,
    _reuse_completed_main_index,
    build_media_params,
    media_index_state_signature,
    media_query_signature,
)
from archive_scout.scanning import hitlist
from archive_scout.utils import utc_now


class AuditTestBuildTests(unittest.TestCase):
    def test_index_and_replay_use_independent_pacing_clocks(self):
        reset_shared_traffic_state_for_tests()
        index_a = SharedFixedRateLimiter(2.5, key=WAYBACK_INDEX_RATE_KEY)
        index_b = SharedFixedRateLimiter(2.5, key=WAYBACK_INDEX_RATE_KEY)
        replay = SharedFixedRateLimiter(0.125, key=WAYBACK_REPLAY_RATE_KEY)
        index_a.next_request = 123.0
        self.assertEqual(index_b.next_request, 123.0)
        self.assertNotEqual(replay.next_request, 123.0)
        replay.next_request = 77.0
        self.assertEqual(index_a.next_request, 123.0)

    def test_v100_default_uses_conservative_24_cdx_starts_per_minute(self):
        config = ProjectConfig(output_dir=Path('.'), targets=['example.com/*'], keywords=[]).normalized()
        self.assertEqual(config.cdx_delay, 2.5)
        self.assertEqual(config.workers, 10)
        self.assertEqual(config.download_delay, 0.125)


    def test_media_selection_policy_round_trips_project_json(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                output_dir=root, targets=['example.com/*'], keywords=[],
                media=MediaConfig(
                    enabled=True, cdx_filters=['statuscode:200', 'mimetype:image/.*'],
                    cdx_collapses=['digest'], cdx_extra_params=['filter=~original:thumb'],
                ),
            ).normalized()
            path = save_project_config(config)
            loaded = load_project_config(path)
            self.assertEqual(loaded.media.cdx_filters, ['statuscode:200', 'mimetype:image/.*'])
            self.assertEqual(loaded.media.cdx_collapses, ['digest'])
            self.assertEqual(loaded.media.cdx_extra_params, ['filter=~original:thumb'])

    def test_http_metrics_count_transport_admission_not_executor_submission(self):
        class FakeTransport:
            def request(self, url, headers, max_bytes, stop_event):
                return TransportResponse(200, {}, url, b'[]', 'fake', 0.01)
            def close(self):
                return None

        client = HttpClient(
            FixedRateLimiter(0), 1, 5, 'test', threading.Event(),
            host_gate=SharedHostGate(), transport=FakeTransport(),
        )
        try:
            response = client.get('https://web.archive.org/test', 1024)
            self.assertEqual(response['status'], 200)
            metrics = client.metrics_snapshot()
            self.assertEqual(metrics['request_starts'], 1)
            self.assertEqual(metrics['request_completions'], 1)
            self.assertGreaterEqual(metrics['network_seconds'], 0)
        finally:
            client.close()

    def test_blank_json_is_incomplete_but_explicit_empty_json_is_valid(self):
        for payload in (b'', b'  \r\n\t'):
            with self.assertRaises(MalformedCDXResponse):
                parse_json_response(payload, 'test')
        self.assertEqual(parse_json_response(b'[]', 'test'), [])

    def test_plain_page_saying_not_in_archive_is_not_a_wayback_error_shell(self):
        normal = '<html><body>The author wrote that the item was not in archive storage.</body></html>'
        self.assertIsNone(classify_replay_content(normal, 'https://web.archive.org/web/20010101000000id_/http://example.com/'))
        error = '<html><body>Wayback Machine has not archived that URL.</body></html>'
        self.assertEqual(
            classify_replay_content(error, 'https://web.archive.org/web/20010101000000id_/http://example.com/'),
            'missing_capture',
        )

    def test_media_cdx_policy_is_independent_from_text_filters(self):
        config = ProjectConfig(
            output_dir=Path('.'),
            targets=['example.com/*'],
            keywords=[],
            cdx_filters=['statuscode:200', 'mimetype:text/html'],
            cdx_collapses=['digest'],
            media=MediaConfig(
                enabled=True,
                cdx_filters=['statuscode:200', 'mimetype:image/.*'],
                cdx_extra_params=['filter=~original:thumb'],
                snapshot_strategy='earliest',
            ),
        ).normalized()
        params = build_media_params(config, 'example.com/*', '20010101000000', '20011231235959')
        self.assertIn(('filter', 'mimetype:image/.*'), params)
        self.assertNotIn(('filter', 'mimetype:text/html'), params)
        self.assertIn(('filter', '~original:thumb'), params)
        self.assertNotIn(('collapse', 'urlkey'), params)
        self.assertNotIn(('collapse', 'digest'), params)

    def test_transport_page_size_no_longer_changes_inventory_identity(self):
        config = ProjectConfig(
            output_dir=Path('.'), targets=['example.com/*'], keywords=[],
            from_date='2001', to_date='2001', page_size=100000,
        ).normalized()
        self.assertEqual(cdx_query_signature(config, 1000), cdx_query_signature(config, 150000))
        self.assertEqual(media_query_signature(config, 1000), media_query_signature(config, 150000))

    def test_auto_strategy_starts_unknown_queries_with_resume(self):
        broad = ProjectConfig(output_dir=Path('.'), targets=['example.com/*'], keywords=[]).normalized()
        exact = ProjectConfig(output_dir=Path('.'), targets=['https://example.com/page.html'], keywords=[]).normalized()
        self.assertEqual(preferred_index_strategy(broad, 'example.com/*'), 'resume')
        self.assertEqual(preferred_index_strategy(exact, 'https://example.com/page.html'), 'resume')

    def test_full_scan_acquires_before_local_scan(self):
        config = ProjectConfig(output_dir=Path('.'), targets=['example.com/*'], keywords=['needle']).normalized()
        order: list[str] = []
        fake_job = SimpleNamespace(patterns=['needle'])
        database = sqlite3.connect(':memory:')
        try:
            with mock.patch('archive_scout.downloads.downloader._acquire_archive', side_effect=lambda *a, **k: order.append('acquire') or {}), \
                 mock.patch('archive_scout.downloads.downloader._scan_pending_captures', side_effect=lambda *a, **k: order.append('scan') or {}):
                download_archive(config, database, 1, threading.Event(), None, scan_jobs=[fake_job])
        finally:
            database.close()
        self.assertEqual(order, ['acquire', 'scan'])

    def test_unified_selector_does_not_materialize_project_sized_temp_queue(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = open_database(root)
            try:
                now = utc_now()
                signature_config = ProjectConfig(root, ['example.com/*'], [], from_date='2001', to_date='2001', text_collapse_scope='year').normalized()
                sig = cdx_query_signature(signature_config)
                with db:
                    db.executemany(
                        """INSERT INTO captures(original_url,timestamp,query_signature,mimetype,statuscode,length,state,created_at,updated_at)
                           VALUES(?,?,?,?,?,?, 'pending',?,?)""",
                        [
                            (f'http://example.com/{i}.html', f'200101010000{i:02d}', sig, 'text/html', '200', 100+i, now, now)
                            for i in range(20)
                        ],
                    )
                total, rows, _stats = prepare_acquisition_rows(db, signature_config, patterns=None)
                self.assertEqual(total, 20)
                self.assertEqual(len(list(rows)), 20)
                temp_names = {row[0] for row in db.execute("SELECT name FROM sqlite_temp_master WHERE type='table'")}
                self.assertNotIn('archive_scout_download_queue', temp_names)
            finally:
                db.close()

    def test_download_only_media_discovery_checkpoint_skips_unchanged_capture(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            capture = root / 'captures' / '2001' / '01' / 'page.txt'
            capture.parent.mkdir(parents=True)
            capture.write_text('<html><img src="http://example.com/a.jpg"></html>', encoding='utf-8')
            config = ProjectConfig(
                output_dir=root,
                targets=['example.com/*'],
                keywords=[],
                media=MediaConfig(enabled=True, discover_embedded=True),
            ).normalized()
            db = open_database(root)
            try:
                now = utc_now()
                with db:
                    cur = db.execute(
                        """INSERT INTO captures(original_url,timestamp,query_signature,mimetype,statuscode,length,state,local_path,created_at,updated_at)
                           VALUES(?,?,?,?,?,?,'downloaded_unscanned',?,?,?)""",
                        ('http://example.com/page.html', '20010101000000', 'q', 'text/html', '200', capture.stat().st_size, str(capture), now, now),
                    )
                    capture_id = int(cur.lastrowid)
                signature = 'media-test'
                calls = {'count': 0}

                def fake_discovery(output_dir, max_file_bytes, media, targets, external_only, row):
                    calls['count'] += 1
                    stat = Path(row['path']).stat()
                    return int(row['id']), None, '', stat.st_size, stat.st_mtime_ns, []

                with mock.patch('archive_scout.media.indexer._discover_document_media', side_effect=fake_discovery):
                    _discover_embedded_queue(config, db, threading.Event(), None, signature, external_only=False)
                    _discover_embedded_queue(config, db, threading.Event(), None, signature, external_only=False)
                self.assertEqual(calls['count'], 1)
                row = db.execute(
                    'SELECT extraction_version,size_bytes FROM media_discovery_captures WHERE query_signature=? AND capture_id=?',
                    (signature, capture_id),
                ).fetchone()
                self.assertEqual(int(row['extraction_version']), MEDIA_DISCOVERY_REVISION)
                self.assertEqual(int(row['size_bytes']), capture.stat().st_size)
            finally:
                db.close()

    def test_legacy_identity_adoption_preserves_pending_rows_and_page_checkpoints(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(root, ['example.com/*'], [], from_date='2001', to_date='2001', text_collapse_scope='year').normalized()
            db = open_database(root)
            try:
                target_id = get_or_create_target(db, config.targets[0])
                old_sig = _legacy_cdx_query_signature(config, 100000)
                new_sig = cdx_query_signature(config)
                with db:
                    db.execute(
                        """INSERT INTO captures(original_url,timestamp,target_id,query_signature,mimetype,statuscode,length,state,created_at,updated_at)
                           VALUES('http://example.com/p.html','20010101000000',?,?, 'text/html','200',100,'pending',?,?)""",
                        (target_id, old_sig, utc_now(), utc_now()),
                    )
                    db.execute(
                        "INSERT INTO index_state(target_id,year,query_signature,complete,seen,updated_at) VALUES(?,2001,?,0,1,?)",
                        (target_id, old_sig, utc_now()),
                    )
                    db.execute(
                        """INSERT INTO index_pages(query_signature,target_id,window_start,window_end,page,row_count,status,updated_at)
                           VALUES(?,?,?, ?,0,1,'complete',?)""",
                        (old_sig, target_id, config.from_date, config.to_date, utc_now()),
                    )
                total, rows, _ = prepare_acquisition_rows(db, config)
                self.assertEqual(total, 1)
                self.assertEqual(len(list(rows)), 1)
                self.assertEqual(db.execute(
                    'SELECT COUNT(*) FROM index_pages WHERE query_signature=?', (new_sig,)
                ).fetchone()[0], 1)
            finally:
                db.close()

    def test_identity_collision_preserves_legacy_document_chain(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(root, ['example.com/*'], [], from_date='2001', to_date='2001', text_collapse_scope='year').normalized()
            db = open_database(root)
            try:
                target_id = get_or_create_target(db, config.targets[0])
                old_sig = _legacy_cdx_query_signature(config, 100000)
                new_sig = cdx_query_signature(config)
                now = utc_now()
                with db:
                    old_id = db.execute(
                        """INSERT INTO captures(original_url,timestamp,target_id,query_signature,mimetype,statuscode,length,state,local_path,created_at,updated_at)
                           VALUES(?,?,?,?,?,?,100,'downloaded',?,?,?)""",
                        ('http://example.com/p.html','20010101000000',target_id,old_sig,'text/html','200',str(root/'legacy.txt'),now,now),
                    ).lastrowid
                    current_id = db.execute(
                        """INSERT INTO captures(original_url,timestamp,target_id,query_signature,mimetype,statuscode,length,state,created_at,updated_at)
                           VALUES(?,?,?,?,?,?,100,'pending',?,?)""",
                        ('http://example.com/p.html','20010101000000',target_id,new_sig,'text/html','200',now,now),
                    ).lastrowid
                    doc_id = db.execute(
                        """INSERT INTO documents(capture_id,path,title,body_chars,size_bytes,created_at,updated_at)
                           VALUES(?,?,?,?,?,?,?)""",
                        (old_id,str(root/'legacy.txt'),'legacy',1,1,now,now),
                    ).lastrowid
                    db.execute('UPDATE captures SET document_id=? WHERE id=?', (doc_id, old_id))
                    db.execute(
                        "INSERT INTO index_state(target_id,year,query_signature,complete,seen,updated_at) VALUES(?,2001,?,0,1,?)",
                        (target_id, old_sig, now),
                    )
                with db:
                    adopt_compatible_index_state(db, target_id, 2001, config, new_sig)
                self.assertIsNotNone(db.execute('SELECT 1 FROM captures WHERE id=?', (old_id,)).fetchone())
                self.assertIsNotNone(db.execute('SELECT 1 FROM documents WHERE id=? AND capture_id=?', (doc_id, old_id)).fetchone())
                current = db.execute('SELECT state,local_path FROM captures WHERE id=?', (current_id,)).fetchone()
                self.assertEqual(current['state'], 'downloaded_unscanned')
                self.assertEqual(current['local_path'], str(root/'legacy.txt'))
            finally:
                db.close()

    def test_media_identity_adoption_copies_completed_page_checkpoint(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(root, ['example.com/*'], [], from_date='2001', to_date='2001', text_collapse_scope='year', media=MediaConfig(enabled=True)).normalized()
            db = open_database(root)
            try:
                target_id = get_or_create_media_target(db, config.targets[0])
                signature = media_query_signature(config)
                current_state = media_index_state_signature(config)
                candidates = media_indexer.media_signature_candidates(config)
                old_media_sig, old_state = next((a,b) for a,b in candidates if b != current_state)
                now = utc_now()
                with db:
                    db.execute(
                        """INSERT INTO media_index_state(target_id,extension,year,query_signature,complete,seen,updated_at)
                           VALUES(?, ?,2001,?,0,1,?)""",
                        (target_id, media_indexer.ALL_EXTENSIONS_STATE, old_state, now),
                    )
                    db.execute(
                        """INSERT INTO media_index_pages(query_signature,target_id,extension,window_start,window_end,page,row_count,status,updated_at)
                           VALUES(?,?,?,?,?,0,1,'complete',?)""",
                        (old_state,target_id,media_indexer.ALL_EXTENSIONS_STATE,config.from_date,config.to_date,now),
                    )
                with db:
                    _adopt_compatible_media_state(db,target_id,2001,config,signature,current_state)
                self.assertEqual(db.execute(
                    'SELECT COUNT(*) FROM media_index_pages WHERE query_signature=?', (current_state,)
                ).fetchone()[0], 1)
            finally:
                db.close()

    def test_selector_plan_uses_ordered_index_without_temp_sort(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(root, ['example.com/*'], [], from_date='2001', to_date='2001', text_collapse_scope='year').normalized()
            db = open_database(root)
            try:
                sig = cdx_query_signature(config)
                plan = [row[3] for row in db.execute(
                    """EXPLAIN QUERY PLAN SELECT c.* FROM captures c
                       WHERE c.query_signature=? AND c.download_attempts<? AND c.state=?
                         AND (c.length,c.id)>(?,?) ORDER BY c.length,c.id LIMIT 2000""",
                    (sig, config.max_attempts, 'pending', 0, 9223372036854775807),
                )]
                self.assertTrue(any('captures_acquisition_order_idx' in row for row in plan), plan)
                self.assertFalse(any('TEMP B-TREE' in row for row in plan), plan)
            finally:
                db.close()

    def test_hitlist_skips_redundant_dom_but_preserves_ambiguous_markup_fallback(self):
        fixtures = [
            ('all-source', '<html><body>alpha beta</body></html>', 'text/html', ['alpha','beta'], 0),
            ('fragment', '<div>alpha <b>beta</b></div>', 'application/octet-stream', ['alpha beta'], 1),
            ('late', 'x '*2500 + '<html><body>alpha <b>beta</b></body></html>', 'application/octet-stream', ['alpha beta'], 1),
        ]
        for label, raw, mime, terms, expected_dom in fixtures:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                cfg = ProjectConfig(root, ['example.com/*'], [], from_date='2001', to_date='2001', text_collapse_scope='year').normalized()
                db = open_database(root)
                try:
                    path = root / 'page.html.txt'
                    path.write_text(raw, encoding='utf-8')
                    with db:
                        db.execute(
                            """INSERT INTO captures(original_url,timestamp,query_signature,mimetype,statuscode,length,state,local_path,created_at,updated_at)
                               VALUES('http://example.com/p.html','20010101000000',?,?, '200',?,'downloaded_unscanned',?,?,?)""",
                            (cdx_query_signature(cfg), mime, len(raw), str(path), utc_now(), utc_now()),
                        )
                    with mock.patch.object(hitlist, 'parse_page', wraps=hitlist.parse_page) as parser:
                        result = hitlist.search_with_hitlist(root, db, terms, threading.Event())
                    self.assertEqual(result['matches'], 1)
                    self.assertEqual(parser.call_count, expected_dom)
                finally:
                    db.close()

    def test_html_only_text_inventory_cannot_mark_image_media_complete(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(
                root, ['example.com/*'], [], from_date='2001', to_date='2001', text_collapse_scope='year',
                cdx_filters=['statuscode:200','mimetype:text/html'],
                media=MediaConfig(enabled=True, include_images=True, include_videos=False, discover_embedded=False),
            ).normalized()
            db = open_database(root)
            try:
                target_id = get_or_create_target(db, cfg.targets[0])
                media_target_id = get_or_create_media_target(db, cfg.targets[0])
                with db:
                    db.execute(
                        'INSERT INTO index_state(target_id,year,query_signature,complete,seen,updated_at) VALUES(?,2001,?,1,1,?)',
                        (target_id, cdx_query_signature(cfg), utc_now()),
                    )
                reused = _reuse_completed_main_index(
                    cfg, db, cfg.targets[0], media_target_id, 2001,
                    media_query_signature(cfg), media_index_state_signature(cfg),
                )
                self.assertFalse(reused[0])
                self.assertIsNone(db.execute('SELECT 1 FROM media_index_state LIMIT 1').fetchone())
            finally:
                db.close()

    def test_completed_compatible_media_inventory_avoids_exact_embedded_lookup(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(
                root, ['example.com/*'], [], from_date='2001', to_date='2001', text_collapse_scope='year', cdx_collapses=[],
                media=MediaConfig(enabled=True, include_images=True, include_videos=False, discover_embedded=True, allow_external_embeds=True),
            ).normalized()
            db = open_database(root)
            try:
                page = root / 'page.html.txt'
                page.write_text('<img src="http://example.com/image.jpg"><img src="http://outside.test/new.jpg">', encoding='utf-8')
                with db:
                    db.execute(
                        """INSERT INTO captures(original_url,timestamp,query_signature,mimetype,statuscode,length,state,local_path,created_at,updated_at)
                           VALUES('http://example.com/index.html','20010101000000',?,'text/html','200',100,'downloaded_unscanned',?,?,?)""",
                        (cdx_query_signature(cfg), str(page), utc_now(), utc_now()),
                    )
                sig = media_query_signature(cfg)
                media_target = get_or_create_media_target(db, cfg.targets[0])
                cdx_row = ('20010101000000','http://example.com/image.jpg','image/jpeg','200','ABC','100')
                with db:
                    upsert_media_captures(db, [(cdx_row,'image','.jpg')], media_target, sig)
                    media_indexer._save_media_state(
                        db, media_target, 2001, media_index_state_signature(cfg), None, True, 1, None
                    )
                calls = []
                def fake_lookup(config, client, row, strategy, endpoints):
                    calls.append(str(row['original_url']))
                    return row, [], None
                with mock.patch.object(media_indexer, '_embedded_lookup', side_effect=fake_lookup):
                    media_indexer.index_embedded_media(cfg, db, object(), threading.Event(), None, sig)
                self.assertNotIn('http://example.com/image.jpg', calls)
                # The external URL is outside the completed direct-media target and
                # therefore still requires a real exact-lookup path.
                self.assertIn('http://outside.test/new.jpg', calls)
            finally:
                db.close()

    def test_compatible_unfiltered_text_inventory_can_still_seed_media(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(
                root, ['example.com/*'], [], from_date='2001', to_date='2001', text_collapse_scope='year', cdx_collapses=[],
                media=MediaConfig(enabled=True, include_images=True, include_videos=False, discover_embedded=False),
            ).normalized()
            db = open_database(root)
            try:
                target_id = get_or_create_target(db, cfg.targets[0])
                media_target_id = get_or_create_media_target(db, cfg.targets[0])
                now = utc_now()
                with db:
                    db.execute(
                        """INSERT INTO captures(original_url,timestamp,target_id,query_signature,mimetype,statuscode,length,state,created_at,updated_at)
                           VALUES('http://example.com/a.jpg','20010101000000',?,?, 'image/jpeg','200',100,'pending',?,?)""",
                        (target_id, cdx_query_signature(cfg), now, now),
                    )
                    db.execute(
                        'INSERT INTO index_state(target_id,year,query_signature,complete,seen,updated_at) VALUES(?,2001,?,1,1,?)',
                        (target_id, cdx_query_signature(cfg), now),
                    )
                reused = _reuse_completed_main_index(
                    cfg, db, cfg.targets[0], media_target_id, 2001,
                    media_query_signature(cfg), media_index_state_signature(cfg),
                )
                self.assertTrue(reused[0])
                self.assertEqual(db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0], 1)
            finally:
                db.close()

    def test_cached_media_inventory_respects_snapshot_strategy(self):
        for strategy, expected in (('earliest', ['20010101000000']), ('latest', ['20011231000000']), ('all', ['20010101000000','20011231000000'])):
            with self.subTest(strategy=strategy), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                cfg = ProjectConfig(
                    root, ['example.com/*'], [], from_date='2001', to_date='2001', text_collapse_scope='year',
                    media=MediaConfig(enabled=True, include_images=True, include_videos=False, discover_embedded=True, snapshot_strategy=strategy),
                ).normalized()
                db = open_database(root)
                try:
                    target_id = get_or_create_media_target(db, cfg.targets[0])
                    sig = media_query_signature(cfg)
                    rows = [
                        (('20010101000000','http://example.com/a.jpg','image/jpeg','200','A','100'),'image','.jpg'),
                        (('20011231000000','http://example.com/a.jpg','image/jpeg','200','B','100'),'image','.jpg'),
                    ]
                    with db:
                        upsert_media_captures(db, rows, target_id, sig)
                        media_indexer._save_media_state(
                            db, target_id, 2001, media_index_state_signature(cfg), None, True, 2, None
                        )
                    cached = _cached_embedded_inventory(db, cfg, sig, 'http://example.com/a.jpg', 'image')
                    self.assertIsNotNone(cached)
                    selected, _ids = cached
                    self.assertEqual([str(row['timestamp']) for row in selected], expected)
                finally:
                    db.close()


    def test_rate_limit_defer_cancels_queued_replay_work_promptly(self):
        class DeferredClient:
            calls = 0
            calls_lock = threading.Lock()
            slow_release = threading.Event()

            def __init__(self, *args, **kwargs):
                pass

            def close(self):
                pass

            @classmethod
            def call_count(cls):
                with cls.calls_lock:
                    return cls.calls

            def download_to_path(self, url, destination, max_bytes, compute_hash=False):
                with type(self).calls_lock:
                    type(self).calls += 1
                    sequence = type(self).calls
                if sequence == 1:
                    time.sleep(0.02)
                    raise RateLimitDeferred('budget exhausted', status=429, waited=0)
                # Keep the already-running sibling occupied until the acquisition
                # function has returned. This tests cancellation deterministically
                # instead of depending on Windows/macOS runner scheduling speed.
                type(self).slow_release.wait(timeout=2.0)
                body = b'plain text'
                Path(destination).write_bytes(body)
                return dict(headers={'content-type':'text/plain'}, preview=body, bytes=len(body), content_hash='', status=200, final_url=url)

            def metrics_snapshot(self):
                calls = type(self).call_count()
                return {
                    'request_starts': calls, 'request_completions': max(0, calls-1),
                    'request_failures': 1, 'network_bytes': 0, 'retry_waits': 0, 'rate_limit_events': 1,
                    'pacing_wait_seconds': 0.0, 'host_gate_wait_seconds': 0.0, 'retry_wait_seconds': 0.0,
                    'rate_limit_wait_seconds': 0.0, 'network_seconds': 0.02,
                }

        DeferredClient.calls = 0
        DeferredClient.slow_release.clear()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(root, ['example.com/*'], [], from_date='2001', to_date='2001', workers=2, download_delay=0).normalized()
            cfg.network.persistent_retries = False
            db = open_database(root)
            try:
                sig = cdx_query_signature(cfg)
                with db:
                    for i in range(6):
                        db.execute(
                            """INSERT INTO captures(original_url,timestamp,query_signature,mimetype,statuscode,length,state,created_at,updated_at)
                               VALUES(?, '20010101000000', ?, 'text/html','200',100,'pending',?,?)""",
                            (f'http://example.com/{i}.html', sig, utc_now(), utc_now()),
                        )
                event = threading.Event()
                started = time.perf_counter()
                with mock.patch('archive_scout.downloads.downloader.HttpClient', DeferredClient):
                    with self.assertRaises(RateLimitDeferred):
                        _acquire_archive(cfg, db, event, lambda e: DeferredClient.slow_release.set() if e.stage == 'network_waiting' else None)
                elapsed = time.perf_counter() - started
                calls_at_return = DeferredClient.call_count()
                DeferredClient.slow_release.set()
                time.sleep(0.2)
                self.assertEqual(DeferredClient.call_count(), calls_at_return)
                self.assertLess(calls_at_return, 6)
                # The slow sibling is blocked for up to two seconds, so returning
                # before that proves cancellation/draining is responsive when the
                # active transport settles, while preserving its completed file. A one-second ceiling tolerates CI jitter.
                self.assertLess(elapsed, 1.0)
                self.assertFalse(event.is_set())
                self.assertEqual(db.execute("SELECT COUNT(*) FROM captures WHERE state IN ('pending','downloaded_unscanned')").fetchone()[0], 6)
                self.assertGreater(db.execute("SELECT COUNT(*) FROM captures WHERE state='downloaded_unscanned'").fetchone()[0], 0)
            finally:
                DeferredClient.slow_release.set()
                db.close()

    def test_normalized_explicit_exact_target_uses_resume_strategy(self):
        cfg = ProjectConfig(
            output_dir=Path('.'), targets=['https://example.com/page.html'], keywords=[], cdx_match_type='exact'
        ).normalized()
        self.assertTrue(cfg.targets[0].endswith('*'))
        self.assertEqual(preferred_index_strategy(cfg, cfg.targets[0]), 'resume')

    def test_failed_transport_time_is_included_in_network_metrics(self):
        class FailingTransport:
            def request(self, url, headers, max_bytes, stop_event):
                time.sleep(0.02)
                raise OSError('offline')
            def close(self):
                pass
        client = HttpClient(
            FixedRateLimiter(0), 1, 5, 'test', threading.Event(),
            host_gate=SharedHostGate(), transport=FailingTransport(),
        )
        try:
            with self.assertRaises(Exception):
                client.get('https://web.archive.org/test', 1024)
            metrics = client.metrics_snapshot()
            self.assertEqual(metrics['request_starts'], 1)
            self.assertEqual(metrics['request_completions'], 0)
            self.assertEqual(metrics['request_failures'], 1)
            self.assertGreaterEqual(metrics['network_seconds'], 0.015)
        finally:
            client.close()


    def test_live_foreign_operation_owner_is_not_marked_interrupted(self):
        # Ownership behavior is independent from OS process creation.  Mock the
        # liveness probe here so this regression test cannot hang CI while still
        # proving that a live foreign owner blocks a second writer.
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = open_database(root)
            foreign_pid = os.getpid() + 100000
            try:
                with db:
                    run_id = start_operation_run(db, 'index', 'audit-test')
                    db.execute('UPDATE operation_runs SET process_id=? WHERE id=?', (foreign_pid, run_id))
                db.close()
                db = None
                with mock.patch(
                    'archive_scout.database.connection._pid_is_alive',
                    side_effect=lambda pid: int(pid) == foreign_pid,
                ):
                    with self.assertRaisesRegex(RuntimeError, 'already active'):
                        open_database(root)
            finally:
                if db is not None:
                    db.close()

    def test_pid_liveness_probe_never_kills_a_real_foreign_process(self):
        # This specifically protects the Windows regression: os.kill(pid, 0)
        # terminates processes on Windows, so the production probe must be a
        # non-destructive Win32 handle query there.
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(3)'])
        try:
            self.assertTrue(_pid_is_alive(child.pid))
            self.assertIsNone(child.poll())
        finally:
            child.terminate()
            try:
                child.wait(timeout=3)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=3)


if __name__ == '__main__':
    unittest.main()
