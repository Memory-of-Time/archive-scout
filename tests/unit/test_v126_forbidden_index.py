"""A refused CDX scope must not abort unrelated indexing or ready acquisition."""
import threading
from collections import Counter
from dataclasses import replace
from unittest.mock import Mock, patch

from tests.unit.test_v126_download_pass import Fixture
from archive_scout.cdx.client import CDXRows, HttpClient, PermanentRequestError
from archive_scout.cdx.indexer import index_archive, decode_plan
from archive_scout.cdx.status import text_index_status
from archive_scout.media.indexer import index_direct_media, media_query_signature, media_index_state_signature
from archive_scout.config import MediaConfig
from archive_scout.operations import run_project
from archive_scout.site_status import host_from_url, site_issue_message
from archive_scout.database.repositories import blocked_site_reasons, record_site_issue


class RefusedScopes(Fixture):
    def test_text_403_keeps_cursor_and_runs_other_target_then_recovers_next_operation(self):
        cfg = replace(self.cfg, targets=['*.blocked.org/*', 'healthy.org/*'])
        calls = []
        def response(client, endpoints, params, **kwargs):
            values = dict(params)
            calls.append(values)
            if 'blocked.org' in values['url']:
                if not values.get('resumeKey'):
                    return CDXRows([('20080101000000', 'http://blocked.org/a', 'text/html', '200', '', '4')], 'SAVED')
                raise PermanentRequestError('HTTP 403', status=403, category='wayback_forbidden')
            return CDXRows([])
        events = []
        with patch.object(HttpClient, 'get_cdx_any', response):
            self.assertTrue(index_archive(cfg, self.db, threading.Event(), events.append))
        status = text_index_status(self.db, cfg)
        self.assertEqual((status['complete_targets'], status['unfinished_targets']), (1, 1))
        state = self.db.execute("SELECT complete,seen,plan_json FROM index_coverage i JOIN targets t "
                                "ON t.id=i.target_id WHERE t.pattern='*.blocked.org/*'").fetchone()
        self.assertEqual((state['complete'], state['seen']), (0, 1))
        self.assertEqual(decode_plan(state['plan_json'], []).pending[0].resume_key, 'SAVED')
        self.assertEqual(len(calls), 3)
        self.assertTrue(any(event.stage == 'site_issue' and event.message.startswith('blocked.org:') for event in events))
        self.assertEqual(blocked_site_reasons(self.db), {})  # plain 403 is not proof of a host exclusion
        resumed = []
        def finish(client, endpoints, params, **kwargs):
            resumed.append(dict(params))
            return CDXRows([])
        with patch.object(HttpClient, 'get_cdx_any', finish):
            self.assertFalse(index_archive(cfg, self.db, threading.Event()))
        self.assertEqual(len(resumed), 1)
        self.assertEqual(resumed[0]['resumeKey'], 'SAVED')
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM captures').fetchone()[0], 1)

    def test_all_403_categories_defer_only_the_failed_scope(self):
        for category in ('wayback_forbidden', 'wayback_excluded', 'robots_blocked', 'http_client_error'):
            with self.subTest(category=category):
                cfg = replace(self.cfg, targets=[f'{category}.org/*', f'good-{category}.org/*'])
                calls = Counter()
                def response(client, endpoints, params, **kwargs):
                    target = dict(params)['url']
                    calls[target] += 1
                    if target.startswith(category):
                        raise PermanentRequestError('HTTP 403', status=403, category=category)
                    return CDXRows([])
                with patch.object(HttpClient, 'get_cdx_any', response):
                    self.assertTrue(index_archive(cfg, self.db, threading.Event()))
                self.assertEqual(set(calls.values()), {1})
                self.assertEqual(text_index_status(self.db, cfg)['unfinished_targets'], 1)
                self.assertEqual(blocked_site_reasons(self.db), {})

    def test_media_403_does_not_abort_other_media_targets(self):
        cfg = replace(self.cfg, targets=['blocked.org/*', 'healthy.org/*'],
                      media=MediaConfig(enabled=True, discover_embedded=False))
        calls = []
        def response(scoped, client, target, window, extensions):
            calls.append(target)
            if target.startswith('blocked'):
                raise PermanentRequestError('HTTP 403', status=403, category='wayback_forbidden')
            return [], True, None
        with patch('archive_scout.media.indexer._request_media_resume', side_effect=response):
            self.assertTrue(index_direct_media(cfg, self.db, Mock(), threading.Event(), None,
                media_query_signature(cfg), media_index_state_signature(cfg)))
        self.assertEqual(calls, ['blocked.org/*', 'healthy.org/*'])
        states = dict(self.db.execute('SELECT t.pattern,s.complete FROM media_index_state s '
                                      'JOIN media_targets t ON t.id=s.target_id'))
        self.assertEqual(states, {'blocked.org/*': 0, 'healthy.org/*': 1})

    def test_all_operation_reaches_ready_downloads_and_reports_after_403(self):
        cfg = replace(self.cfg, targets=['blocked.org/*', 'healthy.org/*'])
        def response(client, endpoints, params, **kwargs):
            if dict(params)['url'].startswith('blocked'):
                raise PermanentRequestError('HTTP 403', status=403, category='wayback_forbidden')
            return CDXRows([('20080101000000', 'http://healthy.org/a', 'text/html', '200', '', '4')])
        def acquire(*args, **kwargs):
            # This test verifies stage admission/continuation. Actual HTTP,
            # hashing and scanning are exercised by the download-pass suite.
            db = args[1]
            with db:
                db.execute("UPDATE captures SET state='skipped'")
        with patch.object(HttpClient, 'get_cdx_any', response), \
             patch('archive_scout.operations.download_archive', side_effect=acquire) as download:
            result = run_project(cfg, 'all')
        self.assertTrue(download.called)
        self.assertEqual(result.status, 'incomplete')
        self.assertIn('1 unfinished', result.message)
        self.assertIn('wayback_forbidden (HTTP 403)', result.message)
        self.assertTrue(any(path.is_file() for path in result.values()))
        self.assertIsNotNone(self.db.execute("SELECT 1 FROM project_meta WHERE key='resume_request'").fetchone())

    def test_all_blocked_scopes_finish_once_without_false_completion(self):
        cfg = replace(self.cfg, targets=['blocked-a.org/*', 'blocked-b.org/*'])
        with patch.object(HttpClient, 'get_cdx_any', side_effect=PermanentRequestError(
                'HTTP 403', status=403, category='wayback_forbidden')) as fetch:
            result = run_project(cfg, 'index')
        self.assertEqual(fetch.call_count, 2)
        self.assertEqual(result.status, 'incomplete')
        self.assertIn('2 unfinished', result.message)
        self.assertEqual(self.db.execute('SELECT SUM(complete) FROM index_coverage '
                                         'WHERE target_id!=?', (self.tid,)).fetchone()[0], 0)

    def test_non403_configuration_failure_still_raises(self):
        cfg = replace(self.cfg, targets=['bad.org/*'])
        with patch.object(HttpClient, 'get_cdx_any', side_effect=PermanentRequestError(
                'HTTP 400 invalid query', status=400, category='http_client_error')):
            with self.assertRaises(PermanentRequestError):
                index_archive(cfg, self.db, threading.Event())

    def test_wildcard_and_bare_target_labels_are_not_unknown(self):
        for target in ('*.www3.to/*', 'www3.to/*', 'https://www3.to/path', '//www3.to/path'):
            with self.subTest(target=target):
                self.assertEqual(host_from_url(target), 'www3.to')
                message = site_issue_message('wayback_forbidden', target, 'CDX indexing', 403)
                self.assertTrue(message.startswith('www3.to:'))
                self.assertIn('does not establish', message)

    def test_index_exclusions_do_not_suppress_replays_but_replay_policy_still_applies(self):
        with self.db:
            record_site_issue(self.db, 'example.org', 'cdx_index', 'wayback_excluded', 'index refused')
            record_site_issue(self.db, 'example.org', 'media_cdx_index', 'robots_blocked', 'media query refused')
        self.assertEqual(blocked_site_reasons(self.db), {})
        with self.db:
            record_site_issue(self.db, 'example.org', 'media_download', 'wayback_excluded', 'replay exclusion')
        self.assertEqual(blocked_site_reasons(self.db), {'example.org': 'wayback_excluded'})
