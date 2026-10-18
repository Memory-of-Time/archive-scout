"""October 18 indexing evidence: regression gates through real compact CDX adapters.

All HTTP traffic is injected; no Internet Archive queries are made.
"""
from __future__ import annotations

import json
import threading
import unittest
from unittest.mock import Mock, patch

import httpx

from archive_scout.cdx.client import HttpClient, TransientRequestError, CDXRows, MalformedCDXResponse
from archive_scout.cdx.indexer import (
    PendingWindow, _request_paged_batch, validate_cdx_scope, PAGED_PIPELINE_PAGES,
)
from archive_scout.config import ProjectConfig, NetworkConfig
from archive_scout.events import Stopped
from archive_scout.cdx.parallel import PageFetchResult
from archive_scout.network.transports import ResilientTransport, TransportExhaustedError
from pathlib import Path


class IndexingOctober18(unittest.TestCase):
    ROW = ('20060101120000', 'http://example.com/a', 'text/html', '200', 'ABC', '8')

    def test_explicit_scope_modes_and_default_ports(self):
        valid = [
            ('example.com', 'domain', 'http://sub.example.com/a'),
            ('example.com', 'host', 'http://example.com/a'),
            ('example.com/a', 'prefix', 'http://example.com/abc'),
            ('example.com/*', '', 'http://example.com:80/a'),
            ('example.com/*', '', 'https://example.com:443/a'),
            ('*.example.com/*', '', 'http://example.com:80/a'),
            ('*.example.com/*', '', 'http://sub.example.com/a'),
        ]
        for target, mode, original in valid:
            with self.subTest(target=target, mode=mode, original=original):
                validate_cdx_scope(
                    [(self.ROW[0], original, *self.ROW[2:])], target,
                    '20050101000000', '20081231235959', ['statuscode:200'], mode,
                )
        for original in ('http://evil-example.com/a','https://unrelated.net/a'):
            with self.assertRaises(MalformedCDXResponse):
                validate_cdx_scope([(self.ROW[0],original,*self.ROW[2:])],
                                   'example.com','20050101000000','20081231235959', [],'domain')
        with self.assertRaises(MalformedCDXResponse):
            validate_cdx_scope([(self.ROW[0],'http://example.com/abc',*self.ROW[2:])],
                               'example.com/other','20050101000000','20081231235959', [],'prefix')

    def _client(self, result):
        client = object.__new__(HttpClient)
        client.get = result
        client._ordered_endpoints = lambda urls: list(urls)
        client._cdx_format_attempts = lambda endpoint, prefer: ['json']
        client._remember_endpoint_success = Mock()
        client._remember_endpoint_failure = Mock()
        client._report_cdx_endpoint_failure = Mock()
        client.retry_callback = None
        return client

    def test_native_compact_http_200_valid_domain_row(self):
        body = json.dumps([
            ['urlkey','timestamp','original','mimetype','statuscode','digest','length'],
            ['com,example,sub)/a','20060101120000','http://sub.example.com/a',
             'text/html','200','ABC','8'],
        ]).encode()
        client = self._client(Mock(return_value={'data':body}))
        response = client.get_cdx_rows_any(
            ['https://web.archive.org/cdx/search/cdx'],
            [('url','example.com'),('matchType','domain'),('output','json')],
            prefer_text=False,
        )
        self.assertEqual(len(response.rows),1)
        validate_cdx_scope(response.rows,'example.com','20050101000000',
                           '20081231235959',['statuscode:200'],'domain')

    def test_same_host_504_short_circuits_aliases_all_three_apis(self):
        urls = ['https://web.archive.org/cdx/search/cdx',
                'https://web.archive.org/web/timemap/json',
                'https://web.archive.org/web/timemap/cdx']
        for method in ('get_cdx_any','get_cdx_rows_any','get_cdx_json_rows_any'):
            with self.subTest(method=method):
                getter = Mock(side_effect=TransientRequestError('gateway 504', status=504))
                client = self._client(getter)
                with self.assertRaises(TransientRequestError) as caught:
                    getattr(client,method)(urls,[('url','*.example.com/*')],prefer_text=False) if method != 'get_cdx_json_rows_any' else getattr(client,method)(urls,[('url','*.example.com/*')])
                self.assertEqual(caught.exception.status,504)
                self.assertEqual(getter.call_count,1)

    def test_cancel_is_not_converted_to_outage_in_three_fetch_apis(self):
        urls=['https://web.archive.org/cdx/search/cdx',
              'https://web.archive.org/web/timemap/json']
        for method in ('get_cdx_any','get_cdx_rows_any','get_cdx_json_rows_any'):
            with self.subTest(method=method):
                getter=Mock(side_effect=Stopped())
                client=self._client(getter)
                with self.assertRaises(Stopped):
                    getattr(client,method)(urls,[('url','example.com')])
                self.assertEqual(getter.call_count,1)

    def test_one_failed_backend_read_does_not_cooldown_transport(self):
        transport=object.__new__(ResilientTransport)
        transport.lock=threading.RLock()
        transport.condition=threading.Condition(transport.lock)
        transport.recovery_probes=set()
        transport.cooldown_until={}
        transport.last_success=None
        transport.order=['httpx']
        transport.callback=None
        transport.backends={'httpx':Mock(request=Mock(side_effect=httpx.ReadTimeout('read timeout')))}
        with self.assertRaises(TransportExhaustedError):
            transport.request('https://example.org/', {}, 1024, threading.Event())
        self.assertEqual(transport.cooldown_until,{})
        self.assertEqual(transport.recovery_probes,set())

    def test_bounded_page_turn_preserves_unstarted_work(self):
        config=ProjectConfig(Path('/tmp/scout-oct18-fixture'),targets=['example.com/*'],keywords=[],
            network=NetworkConfig(index_strategy='paged',cdx_workers=10,page_blocks=9),
            cdx_delay=0).normalized()
        window=PendingWindow('20050101000000','20081231235959',
                             strategy='paged',page_count=1000,page_blocks=9)
        # The pool can remain full while the operation checks Stop and target
        # budgets after a small bounded turn, not after 1,000 page admissions.
        with patch('archive_scout.cdx.indexer._request_paged_count',return_value=1000), \
             patch('archive_scout.cdx.indexer.iter_cdx_pages',return_value=iter(())):
            batch=_request_paged_batch(Mock(),config,'example.com/*',window,
                                       threading.Event())
        self.assertLessEqual(len(batch.requested_pages),PAGED_PIPELINE_PAGES)
        self.assertFalse(batch.finished)
        self.assertLess(window.page,1000)

    def test_expired_page_admission_keeps_every_unstarted_page(self):
        import time
        config=ProjectConfig(Path('/tmp/scout-oct18-fixture'),targets=['example.com/*'], keywords=[],
            network=NetworkConfig(index_strategy='paged',cdx_workers=3,page_blocks=9),
            cdx_delay=0).normalized()
        current=PendingWindow('20050101000000','20081231235959',
                              strategy='paged',page_count=20,page_blocks=9)
        from archive_scout.cdx.parameters import cdx_layout_signature
        current.page_layout=cdx_layout_signature(config,current.start,current.end,'paged',current.page_blocks)
        with patch('archive_scout.cdx.indexer.iter_cdx_pages', return_value=iter(())):
            batch=_request_paged_batch(Mock(),config,'example.com/*',current,
                                       threading.Event(),deadline=time.monotonic()-1)
        self.assertEqual(batch.results,[])
        self.assertEqual(current.page,0)
        self.assertTrue(current.retry_pages)
        self.assertFalse(batch.finished)

    def test_partial_page_admission_retries_unsubmitted_page(self):
        config=ProjectConfig(Path('/tmp/scout-oct18-fixture'),targets=['example.com/*'],keywords=[],
            network=NetworkConfig(index_strategy='paged',cdx_workers=3,page_blocks=9),cdx_delay=0).normalized()
        current=PendingWindow('20050101000000','20081231235959',
                              strategy='paged',page_count=20,page_blocks=9)
        from archive_scout.cdx.parameters import cdx_layout_signature
        current.page_layout=cdx_layout_signature(config,current.start,current.end,'paged',current.page_blocks)
        with patch('archive_scout.cdx.indexer.iter_cdx_pages',return_value=iter([
                PageFetchResult(0,[],0),PageFetchResult(2,[],0)])):
            batch=_request_paged_batch(Mock(),config,'example.com/*',current,threading.Event())
        self.assertEqual(len(batch.successful),2)
        self.assertEqual(current.page,1)
        self.assertIn(1,current.retry_pages)
        self.assertFalse(batch.finished)


if __name__=='__main__':
    unittest.main()
