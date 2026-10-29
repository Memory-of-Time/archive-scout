"""Selected corpus, complete discovery and earliest-media admission regressions."""
import hashlib
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch, Mock

from archive_scout.cdx.client import HttpClient, CDXRows, TransientRequestError
from archive_scout.cdx.indexer import save_coverage_state
from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.config import ProjectConfig, MediaConfig, NetworkConfig, ResearchConfig
from archive_scout.coverage import IndexCoveragePending, require_embedded_coverage
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import (get_or_create_target, get_or_create_media_target,
    upsert_capture, upsert_document, upsert_media_captures)
from archive_scout.downloads.downloader import download_archive, prepare_download_rows
from archive_scout.downloads.session import begin_session, end_session
from archive_scout.media.indexer import (_discover_embedded_queue, media_query_signature,
    media_index_state_signature, _save_media_state, _apply_snapshot_strategy,
    _adopt_compatible_media_state, build_media_params, _embedded_lookup)
from archive_scout.media.downloader import download_media
from archive_scout.operations import run_project, remaining_acquisition_gaps
from archive_scout.scanning.pending import _reconcile_legacy_pending, reconciliation_done
from archive_scout.utils import hash_text


class SelectedScopeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.cfg = ProjectConfig(self.root, ['example.org/*'], ['needle'],
            from_date='2008', to_date='2008', cdx_collapses=[],
            cdx_delay=0, download_delay=0, workers=2,
            research=ResearchConfig(auto_build=False),
            network=NetworkConfig(trust_environment=False),
            media=MediaConfig(enabled=True, discover_embedded=True,
                              allow_external_embeds=True)).normalized()
        self.db = open_database(self.root)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def seed(self, target='example.org/*', timestamp='20080101000000', signature=None, raw=None):
        raw = raw if raw is not None else '<p>needle</p><img src="http://cdn.test/a.jpg">'
        tid = get_or_create_target(self.db, target)
        number = self.db.execute('SELECT COUNT(*) FROM captures').fetchone()[0]
        upsert_capture(self.db, {'original': f'http://{target.split("/")[0]}/{number}',
            'timestamp': timestamp, 'mimetype': 'text/html', 'statuscode': '200',
            'digest': '', 'length': str(len(raw))}, tid,
            signature or cdx_query_signature(self.cfg))
        cid = self.db.execute('SELECT MAX(id) FROM captures').fetchone()[0]
        p = self.root / f'{cid}.html'
        p.write_text(raw, encoding='utf-8')
        did = upsert_document(self.db, cid, p, '', 'needle', [], hash_text(raw), hash_text('needle'), len(raw.encode()))
        self.db.commit()
        return cid, did, p

    def test_rescan_only_scores_selected_site_date_and_signature(self):
        _, expected, _ = self.seed()
        self.seed(target='other.org/*')
        self.seed(timestamp='20070101000000')  # Same uncollapsed range signature.
        self.seed(signature='different-filters')
        result = run_project(self.cfg, 'rescan')
        self.assertEqual(result.status, 'complete')
        ids = [r[0] for r in self.db.execute('SELECT document_id FROM document_matches')]
        self.assertEqual(ids, [expected])

    def test_discovery_and_its_gate_share_the_selected_corpus(self):
        _, did, _ = self.seed()
        _, _, outside = self.seed(target='other.org/*')
        outside.unlink()  # Must neither block nor create discovery work.
        self.seed(timestamp='20070101000000')
        self.seed(signature='other-policy')
        sig = media_query_signature(self.cfg)
        self.assertEqual(_discover_embedded_queue(self.cfg, self.db, threading.Event(), None, sig, external_only=True), 1)
        self.assertEqual([r[0] for r in self.db.execute('SELECT document_id FROM media_discovery_documents')], [did])
        self.db.execute("UPDATE media_discovery_queue SET state='indexed'")
        self.db.commit()
        require_embedded_coverage(self.db, self.cfg, sig)

    def test_target_specific_dates_and_overlap_membership(self):
        cid, did, _ = self.seed(target='primary.org/*', timestamp='20090101000000')
        tid = get_or_create_target(self.db, self.cfg.targets[0])
        self.db.execute('INSERT INTO capture_target_membership VALUES(?,?)', (cid, tid))
        cfg = replace(self.cfg, target_settings={'example.org/*': {'from_date': '2009', 'to_date': '2009'}}).normalized()
        self.db.commit()
        run_project(cfg, 'rescan')
        self.assertEqual([r[0] for r in self.db.execute('SELECT document_id FROM document_matches')], [did])

    def test_oversized_read_is_an_obligation_not_empty_discovery(self):
        self.seed(raw=' ' * 200000 + '<img src="http://cdn.test/end.jpg">')
        cfg = replace(self.cfg, max_file_mb=0.1)
        sig = media_query_signature(cfg)
        _discover_embedded_queue(cfg, self.db, threading.Event(), None, sig, external_only=True)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_discovery_documents').fetchone()[0], 0)
        with self.assertRaises(IndexCoveragePending):
            require_embedded_coverage(self.db, cfg, sig)
        _discover_embedded_queue(self.cfg, self.db, threading.Event(), None, sig, external_only=True)
        self.assertEqual(self.db.execute('SELECT original_url FROM media_discovery_queue').fetchone()[0], 'http://cdn.test/end.jpg')

    def test_changed_raw_bytes_do_not_receive_the_old_discovery_hash(self):
        _, _, p = self.seed()
        p.write_text('<p>changed</p>')
        sig = media_query_signature(self.cfg)
        _discover_embedded_queue(self.cfg, self.db, threading.Event(), None, sig, external_only=True)
        with self.assertRaises(IndexCoveragePending):
            require_embedded_coverage(self.db, self.cfg, sig)

    def test_legacy_discovery_and_old_direct_filter_proofs_require_reindex(self):
        self.seed()
        tid = get_or_create_media_target(self.db, 'example.org/*')
        old_sig = media_query_signature(self.cfg, discovery_version=1)
        old_state = hashlib.sha256(('{"index_revision":4,"media_query_signature":"' + old_sig + '"}').encode()).hexdigest()[:20]
        _save_media_state(self.db, tid, 2008, old_state, None, True, 100000, None)
        self.db.execute("INSERT INTO media_discovery_documents(query_signature,document_id,content_hash,scanned_at) SELECT ?,id,content_hash,'now' FROM documents", (old_sig,))
        sig = media_query_signature(self.cfg)
        _adopt_compatible_media_state(self.db, tid, 2008, self.cfg, sig, media_index_state_signature(self.cfg))
        self.assertIsNone(self.db.execute('SELECT complete FROM media_index_state WHERE query_signature=?', (media_index_state_signature(self.cfg),)).fetchone())
        self.assertEqual(self.db.execute('SELECT complete FROM media_index_state WHERE query_signature=?', (old_state,)).fetchone()[0], 1)
        with self.assertRaises(IndexCoveragePending):
            require_embedded_coverage(self.db, self.cfg, sig)

    def test_acquisition_queue_and_remaining_gaps_obey_date_bounds(self):
        a, _, _ = self.seed()
        b, _, _ = self.seed(timestamp='20070101000000')
        self.db.execute("UPDATE captures SET state='pending'")
        self.db.commit()
        total, rows = prepare_download_rows(self.db, self.cfg, [])
        self.assertEqual(total, 1)
        self.assertEqual([r['id'] for r in rows], [a])
        self.db.execute("UPDATE captures SET state='downloaded' WHERE id=?", (a,))
        self.assertEqual(remaining_acquisition_gaps(self.cfg, self.db), 0)
        self.assertEqual(self.db.execute('SELECT state FROM captures WHERE id=?', (b,)).fetchone()[0], 'pending')

    def test_retry_batches_reuse_transport_and_close_it_once(self):
        a, _, _ = self.seed()
        b, _, _ = self.seed()
        tid = get_or_create_target(self.db, self.cfg.targets[0])
        save_coverage_state(self.db, tid, cdx_query_signature(self.cfg), self.cfg.from_date, self.cfg.to_date, None, True, 2, None)
        self.db.execute("UPDATE captures SET state='error'")
        self.db.commit()
        made = []
        def factory(*args, **kwargs):
            client = HttpClient(*args, **kwargs)
            client.close = Mock(wraps=client.close)
            made.append(client)
            return client
        token = begin_session()
        try:
            with patch('archive_scout.downloads.downloader.HttpClient', side_effect=factory), patch(
                'archive_scout.downloads.downloader.fetch_retain_streamed', side_effect=TransientRequestError('fixture', connection_failed=True)):
                for cid in (a, b):
                    download_archive(self.cfg, self.db, 0, threading.Event(), None,
                                     states=('error',), capture_ids=[cid], acquire_only=True)
            self.assertEqual(len(made), 1)
        finally:
            end_session(token)
        made[0].close.assert_called_once()

    def test_changing_date_scope_does_not_hide_old_url_filter_exclusions(self):
        from archive_scout.scanning.jobs import ScanJob
        a, _, _ = self.seed()
        self.seed(timestamp='20090101000000')
        self.db.execute("UPDATE captures SET state='pending'")
        self.db.commit()
        filtered = replace(self.cfg, download_scope='keyword_urls')
        patterns = ScanJob.create(0, 'Test', ['needle']).patterns
        total, rows = prepare_download_rows(self.db, filtered, patterns)
        self.assertEqual(total, 0)
        self.assertEqual(self.db.execute('SELECT state FROM captures WHERE id=?', (a,)).fetchone()[0], 'skipped')
        other_dates = replace(self.cfg, from_date='2009', to_date='2009').normalized()
        prepare_download_rows(self.db, other_dates, [])
        total, rows = prepare_download_rows(self.db, self.cfg, [])
        self.assertEqual(total, 1)
        self.assertEqual([row['id'] for row in rows], [a])

    def test_cancelled_legacy_reconciliation_never_certifies_coverage(self):
        cid, did, _ = self.seed()
        self.db.execute('DELETE FROM documents WHERE id=?', (did,))
        self.db.commit()
        stop = threading.Event(); stop.set()
        from archive_scout.events import Stopped
        with self.assertRaises(Stopped):
            _reconcile_legacy_pending(self.db, self.cfg, stop)
        self.assertFalse(reconciliation_done(self.db, self.cfg))
        _reconcile_legacy_pending(self.db, self.cfg, threading.Event())
        self.assertTrue(reconciliation_done(self.db, self.cfg))
        self.assertEqual(self.db.execute('SELECT state FROM captures WHERE id=?', (cid,)).fetchone()[0], 'downloaded_unscanned')

    def media_rows(self):
        cfg = replace(self.cfg, media=replace(self.cfg.media, discover_embedded=False))
        sig = media_query_signature(cfg)
        tid = get_or_create_media_target(self.db, cfg.targets[0])
        items = [(('20080102000000', 'http://example.org/a.jpg', 'image/jpeg', '200', 'same', '10'), 'image', '.jpg'),
                 (('20080101000000', 'http://example.org/a.jpg', 'image/jpeg', '200', 'same', '10'), 'image', '.jpg'),
                 (('20080103000000', 'http://example.org/a.jpg', 'image/jpeg', '200', 'new', '11'), 'image', '.jpg'),
                 (('20080101000000', 'http://example.org/b.jpg', 'image/jpeg', '200', 'same', '10'), 'image', '.jpg')]
        upsert_media_captures(self.db, items, tid, sig)
        upsert_media_captures(self.db, items, None, sig, source_type='embedded')
        _save_media_state(self.db, tid, 2008, media_index_state_signature(cfg), None, True, 4, None)
        self.db.commit()
        return cfg, sig

    def test_earliest_deduplicates_routes_and_timestamps_without_dropping_other_urls(self):
        cfg, sig = self.media_rows()
        _apply_snapshot_strategy(self.db, sig, 'earliest')
        kept = [(r['original_url'], r['timestamp']) for r in self.db.execute("SELECT * FROM media_captures WHERE state='pending' ORDER BY original_url")]
        self.assertEqual(kept, [('http://example.org/a.jpg', '20080101000000'), ('http://example.org/b.jpg', '20080101000000')])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0], 4)

    def test_explicit_retry_does_not_redownload_later_duplicate(self):
        cfg, sig = self.media_rows()
        later = self.db.execute("SELECT id FROM media_captures WHERE timestamp='20080103000000'").fetchone()[0]
        self.db.execute("UPDATE media_captures SET state='error' WHERE id=?", (later,))
        self.db.commit()
        with patch('archive_scout.media.downloader.HttpClient') as client:
            download_media(cfg, self.db, threading.Event(), states=('error',), media_capture_ids=[later])
            client.assert_not_called()
        self.assertEqual(self.db.execute('SELECT state FROM media_captures WHERE id=?', (later,)).fetchone()[0], 'skipped_strategy')

    def test_extra_digest_collapse_cannot_hide_an_earlier_capture(self):
        cfg = replace(self.cfg, cdx_extra_params=['collapse=digest'])
        params = build_media_params(cfg, 'example.org/*', cfg.from_date, cfg.to_date)
        self.assertFalse(any(key == 'collapse' for key, _ in params))
        self.assertNotEqual(media_index_state_signature(cfg), media_index_state_signature(cfg, revision=4))

    def test_earliest_selection_survives_real_transfer_and_resume(self):
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        from archive_scout.downloads.rate_limit import reset_shared_traffic_state_for_tests
        cfg, sig = self.media_rows()
        body = b'\xff\xd8\xff' + b'x' * 100
        seen = []
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                seen.append(self.path)
                self.send_response(200)
                self.send_header('Content-Type', 'image/jpeg')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers(); self.wfile.write(body)
            def log_message(self, *unused):
                pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
        reset_shared_traffic_state_for_tests()
        # Synthetic CDX digests above are intentionally not SHA-1 values.
        self.db.execute("UPDATE media_captures SET digest=NULL,length=?", (len(body),))
        self.db.commit()
        try:
            with patch('archive_scout.media.downloader.media_replay_url',
                       side_effect=lambda row: f'http://127.0.0.1:{server.server_port}/{row["timestamp"]}/{row["original_url"].rsplit("/", 1)[-1]}'):
                download_media(cfg, self.db, threading.Event())
                download_media(cfg, self.db, threading.Event())
            self.assertEqual(sorted(seen), ['/20080101000000/a.jpg', '/20080101000000/b.jpg'])
            downloaded = self.db.execute("SELECT path,content_hash FROM media_captures WHERE state='downloaded'").fetchall()
            self.assertEqual(len(downloaded), 2)
            for row in downloaded:
                self.assertEqual(hashlib.sha256(Path(row['path']).read_bytes()).hexdigest(), row['content_hash'])
        finally:
            server.shutdown(); server.server_close(); thread.join(timeout=3)


if __name__ == '__main__':
    unittest.main()
