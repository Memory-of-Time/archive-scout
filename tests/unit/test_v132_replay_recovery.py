from __future__ import annotations

import base64
import hashlib
import threading
import unittest
from contextlib import closing
from unittest.mock import patch

from archive_scout.network.transports import (ContentEncodingMismatch, IncompleteRepresentationError,
    PayloadDigestMismatch, ResilientTransport, _decoded_verified_chunks)


class ReplayRouteRecovery(unittest.TestCase):
    def test_one_answer_keeps_single_admission_until_the_route_confirms_again(self):
        stop = threading.Event()
        clock = [50.0]
        with closing(ResilientTransport(pool_size=8, connect_timeout=1, read_timeout=1,
                                       trust_env=False)) as transport:
            transport.replay_route_recovery = True
            first = transport.backend_names[0]
            with patch('archive_scout.network.transports.time.monotonic', side_effect=lambda: clock[0]):
                old_turn = transport._await_backend(first, stop, wait=False)
                for backend in transport.backend_names:
                    for _ in range(transport.CONNECTION_FAILURE_THRESHOLD):
                        turn = transport._await_backend(backend, stop, wait=False)
                        transport._backend_result(turn, False)
                self.assertEqual(transport.dispatch_slots(8), 0)
                self.assertEqual(transport.snapshot()['replay_recovery_incidents'], 1)
                transport._backend_reachable(old_turn)
                self.assertEqual(transport.snapshot()['replay_confirmations_remaining'], 2)
                clock[0] += transport.BACKEND_RECOVERY_SECONDS
                transport._backend_reachable(transport._await_backend(first, stop, wait=False))
                self.assertEqual(transport.dispatch_slots(8), 1)
                non_http = transport._await_backend(first, stop, wait=False)
                transport._backend_result(non_http, True)
                self.assertEqual(transport.dispatch_slots(8), 1)
                transport._backend_result(transport._await_backend(first, stop, wait=False), False)
                self.assertEqual(transport.snapshot()['replay_confirmations_remaining'], 2)
                transport._backend_reachable(transport._await_backend(first, stop, wait=False))
                self.assertEqual(transport.dispatch_slots(8), 1)
                transport._backend_reachable(transport._await_backend(first, stop, wait=False))
                self.assertEqual(transport.dispatch_slots(8), 8)
                self.assertEqual(transport.snapshot()['replay_confirmations_remaining'], 0)


class NoneEncodingRecovery(unittest.TestCase):
    def test_none_header_requires_a_complete_matching_indexed_payload(self):
        payload = b'complete archived response'
        digest = base64.b32encode(hashlib.sha1(payload).digest()).decode()
        stop = threading.Event()
        headers = {'content-encoding': 'none', 'content-length': str(len(payload))}
        self.assertEqual(b''.join(_decoded_verified_chunks([payload], headers, stop,
                                                           expected_digest=digest)), payload)
        self.assertIn('CDX SHA-1 verified', headers['x-archive-scout-encoding-recovery'])
        with self.assertRaises(ContentEncodingMismatch):
            list(_decoded_verified_chunks([payload], {'content-encoding': 'none'}, stop))
        with self.assertRaises(PayloadDigestMismatch):
            list(_decoded_verified_chunks([b'unrelated response'], {'content-encoding': 'none'}, stop,
                                          expected_digest=digest))
        with self.assertRaises(IncompleteRepresentationError):
            list(_decoded_verified_chunks([payload], {'content-encoding': 'none', 'content-length': '200'},
                                          stop, expected_digest=digest))
