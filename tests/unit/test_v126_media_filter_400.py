"""Media CDX filter compatibility and safe checkpoint transition regressions."""
from __future__ import annotations

import unittest
from dataclasses import replace
from urllib.parse import parse_qs, urlsplit

from archive_scout.cdx.client import PermanentRequestError
from archive_scout.cdx.indexer import IndexPlan, PendingWindow, encode_plan
from archive_scout.database.repositories import get_or_create_media_target, upsert_media_captures
from archive_scout.media.indexer import (
    _save_media_state, build_media_params, build_media_num_pages_params,
    build_media_paged_params, media_index_state_signature, media_query_signature,
)
from archive_scout.network.transports import TransportResponse
from archive_scout.utils import utc_now
from . import test_v126_media_index_completeness as media_fixture
from .test_v126_media_index_completeness import client, row, text_body, json_body


class NamedFieldTransport:
    """Reject the reported fieldless predicate at the actual HTTP client seam."""
    def __init__(self, rows):
        self.rows = rows
        self.urls = []

    def request(self, url, headers, max_bytes, stop_event):
        self.urls.append(url)
        query = parse_qs(urlsplit(url).query)
        for value in query.get('filter', []):
            field, separator, regex = value.partition(':')
            if not separator or field.lstrip('!~') not in {'statuscode', 'original', 'mimetype'}:
                return TransportResponse(400, {}, url,
                    ("invalid field spec '" + field + "'").encode(), 'fixture', .001)
        if query.get('showNumPages') == ['true']:
            body = b'2'
        elif 'page' in query:
            body = json_body(self.rows if query['page'] == ['1'] else [])
        else:
            body = text_body(self.rows) if query.get('output') == ['txt'] else json_body(self.rows)
        return TransportResponse(200, {}, url, body, 'fixture', .001)

    def close(self):
        pass


class MediaFilter400Tests(unittest.TestCase):
    setUp = media_fixture.MissingMediaMembership.setUp
    tearDown = media_fixture.MissingMediaMembership.tearDown
    run_media = media_fixture.MissingMediaMembership.run_media

    def test_reported_target_builders_keep_only_explicit_filters(self):
        cfg = replace(self.cfg, cdx_filters=['statuscode:200', '!original:.*thumbnail.*'],
                      cdx_extra_params=['filter=!mimetype:warc/revisit'])
        target = '*.nothingtoxic.com/*'
        args = (cfg, target, '20051001000000', '20121231235959')
        builders = [
            build_media_params(*args, extensions=['.jpg', '.wmv']),
            build_media_num_pages_params(*args, ['.jpg', '.wmv'], 1),
            build_media_paged_params(*args, ['.jpg', '.wmv'], 1, 1),
            build_media_params(*args, resume='SAFE', exact=True),
        ]
        for params in builders:
            with self.subTest(params=params):
                self.assertEqual([v for k, v in params if k == 'filter'],
                    ['statuscode:200', '!original:.*thumbnail.*', '!mimetype:warc/revisit'])
                self.assertEqual(dict(params)['from'], args[2])
                self.assertEqual(dict(params)['to'], args[3])
                self.assertIn('nothingtoxic.com', dict(params)['url'])

    def test_fieldless_filter_reproduces_400_then_local_selection_finishes(self):
        rows = [row('http://example.org/a.jpg', 'application/octet-stream'),
                row('http://example.org/b.%6a%70%67'),
                row('http://example.org/c%2E%4A%50%47'),
                row('http://example.org/image'),
                row('http://example.org/movie', 'video/x-ms-wmv'),
                row('http://example.org/page', 'text/html'),
                row('http://example.org/disallowed.png', 'image/png')]
        transport = NamedFieldTransport(rows)
        c = client(transport)
        params = build_media_params(self.cfg, self.cfg.targets[0], self.cfg.from_date, self.cfg.to_date)
        params.append(('filter', r'(?i).*(image/|\.jpg).*'))
        with self.assertRaises(PermanentRequestError) as caught:
            c.get_cdx_rows_any(('https://fixture.invalid/cdx',), params)
        self.assertEqual(caught.exception.status, 400)
        c.close()
        fixed = NamedFieldTransport(rows)
        self.run_media(fixed)
        saved = {r[0] for r in self.db.execute('SELECT original_url FROM media_captures')}
        self.assertEqual(saved, {r[1] for r in rows[:5]})
        self.assertEqual(len(fixed.urls), 5)
        self.assertTrue(all('filter=%28' not in url for url in fixed.urls))
        self.assertTrue(all(r[0] for r in self.db.execute('SELECT complete FROM media_index_state')))

    def test_revision_six_cursor_and_pages_are_retained_but_not_reused(self):
        target_id = get_or_create_media_target(self.db, self.cfg.targets[0])
        old_sig = media_index_state_signature(self.cfg, revision=6)
        sig = media_query_signature(self.cfg)
        old_plan = encode_plan(IndexPlan([PendingWindow(self.cfg.from_date, self.cfg.to_date, 'OLD-CURSOR')], 0, 1))
        with self.db:
            _save_media_state(self.db, target_id, 2008, old_sig, old_plan, False, 1, None)
            upsert_media_captures(self.db, [(row(), 'image', '.jpg')], target_id, sig)
            self.db.execute("UPDATE media_captures SET state='downloaded',path='retained.jpg'")
            self.db.execute('INSERT INTO media_index_pages VALUES(?,?,?,?,?,?,?,?,?)',
                (target_id, 2008, old_sig, self.cfg.from_date, self.cfg.to_date, 'old-layout', 0, 1, utc_now()))
        before = self.db.execute('SELECT id,state,path FROM media_captures').fetchone()[:]
        fixed = NamedFieldTransport([row(), row('http://example.org/earlier-hidden')])
        self.run_media(fixed)
        self.assertTrue(all('OLD-CURSOR' not in url for url in fixed.urls))
        self.assertEqual(self.db.execute('SELECT resume_key,complete FROM media_index_state WHERE query_signature=?', (old_sig,)).fetchone()[:], (old_plan, 0))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_index_pages WHERE state_signature=?', (old_sig,)).fetchone()[0], 1)
        self.assertEqual(self.db.execute('SELECT id,state,path FROM media_captures WHERE original_url=?', (row()[1],)).fetchone()[:], before)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0], 2)
        self.assertNotEqual(old_sig, media_index_state_signature(self.cfg))
        unused = NamedFieldTransport([])
        self.run_media(unused)
        self.assertEqual(unused.urls, [])

    def test_revision_six_completed_inventory_does_not_skip_new_query(self):
        target_id = get_or_create_media_target(self.db, self.cfg.targets[0])
        old_sig = media_index_state_signature(self.cfg, revision=6)
        with self.db:
            _save_media_state(self.db, target_id, 2008, old_sig, None, True, 0, None)
        fixed = NamedFieldTransport([row('http://example.org/extensionless')])
        self.run_media(fixed)
        self.assertGreater(len(fixed.urls), 0)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_index_state WHERE complete=1').fetchone()[0], 2)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM media_captures').fetchone()[0], 1)
