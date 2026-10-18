"""CDX service outages must not fragment or silently complete saved inventory."""
from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import HttpClient, MalformedCDXResponse, TransientRequestError
from archive_scout.cdx.indexer import (
    PendingWindow, _can_split_cdx_window, _defer_transient_window,
    _query_is_oversized, decode_plan, encode_plan, index_archive,
)
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.downloads.rate_limit import FixedRateLimiter


class OutageRecoveryTests(unittest.TestCase):
    def config(self, root, start='20061031000115', end='20061031000115'):
        return ProjectConfig(
            output_dir=root, targets=['liveleak.com/*'], keywords=['sample'],
            from_date=start, to_date=end, cdx_delay=0, retries=1,
            network=NetworkConfig(index_strategy='resume'),
        ).normalized()

    def test_http_outage_never_justifies_subdividing(self):
        window=PendingWindow('20060101000000', '20061231235959', strategy='resume')
        for status in (408, 429, 500, 502, 503, 504):
            error=TransientRequestError(f'HTTP {status}', status=status, splittable=True)
            self.assertFalse(_can_split_cdx_window(window,error))
        self.assertFalse(_can_split_cdx_window(window,TransientRequestError('all network backends failed', connection_failed=True, splittable=True)))
        self.assertFalse(_can_split_cdx_window(window,TransientRequestError('all CDX endpoints failed: HTTP 503', splittable=True)))

    def test_real_query_pressure_preserves_split_option(self):
        wide=PendingWindow('20060101000000','20061231235959')
        short=PendingWindow('20061031000115','20061031000115')
        oversized=TransientRequestError('CDX response was larger than the safe in-memory budget',splittable=True)
        self.assertTrue(_can_split_cdx_window(wide,oversized))
        self.assertTrue(_query_is_oversized(oversized))
        # Proven size pressure may justify smaller windows where subdivision remains possible.
        self.assertTrue(_can_split_cdx_window(short,oversized))
        timeout=TransientRequestError('read timeout', timed_out=True,splittable=True)
        self.assertTrue(_can_split_cdx_window(wide,timeout))
        self.assertFalse(_can_split_cdx_window(short,timeout))
        self.assertFalse(_can_split_cdx_window(short,MalformedCDXResponse('bad gateway-looking response',splittable=True)))

    def test_existing_one_second_checkpoint_survives_outage_unchanged(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); db=open_database(root); config=self.config(root)
            outage=TransientRequestError('HTTP 503',status=503,splittable=True)
            seen=[]
            def fake_request(*args,**kwargs):
                seen.append('attempt')
                if len(seen)==1:
                    raise outage
                return []
            with patch.object(HttpClient,'get_cdx_any',side_effect=fake_request),patch('archive_scout.cdx.indexer.transient_backoff',return_value=0):
                index_archive(config,db,threading.Event())
            self.assertEqual(seen,['attempt','attempt'])
            self.assertEqual(db.execute('SELECT seen FROM index_coverage').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT complete FROM index_coverage').fetchone()[0],1)
            db.close()

    def test_outage_preserves_page_size_and_head_of_fragmented_queue(self):
        from archive_scout.cdx.indexer import IndexPlan
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); db=open_database(root); config=self.config(root)
            first=PendingWindow('20061031000115','20061031000115',page_size=100000, strategy='resume')
            second=PendingWindow('20061031000116','20061031000116',page_size=100000, strategy='resume')
            plan=IndexPlan([first,second],0,2)
            saved=[]
            def persist(payload, complete, seen, error_id):
                saved.append(payload)
            outage=TransientRequestError('HTTP 503',status=503,splittable=True)
            with patch('archive_scout.cdx.indexer.transient_backoff',return_value=0):
                _defer_transient_window(config,db,plan,first,persist,700000,None,outage,None,0,2,threading.Event())
            self.assertEqual([(x.start,x.end) for x in plan.pending], [('20061031000115','20061031000115'),('20061031000116','20061031000116')])
            self.assertEqual(first.page_size,100000)
            self.assertEqual(decode_plan(saved[-1],[]).pending[0].start, first.start)
            db.close()

    def test_endpoint_failure_log_identifies_cause_and_next_step(self):
        messages=[]
        client=HttpClient(FixedRateLimiter(0),1,1,'test',threading.Event(),retry_callback=lambda *x: messages.append(x[2]))
        try:
            client._report_cdx_endpoint_failure('https://web.archive.org/cdx/search/cdx',TransientRequestError('HTTP 502',status=502), 'https://web.archive.org/web/timemap/json',13.5)
            self.assertIn('HTTP 502',messages[-1]);self.assertIn('13.5s',messages[-1]);self.assertIn('timemap/json',messages[-1]);self.assertIn('checkpoint retained',messages[-1])
        finally:
            client.close()

if __name__=='__main__':unittest.main()
