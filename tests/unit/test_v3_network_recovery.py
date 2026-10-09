from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import HttpClient, TransientRequestError
from archive_scout.cdx.indexer import index_archive
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.downloads.rate_limit import FixedRateLimiter
from archive_scout.events import ConnectivityPaused
from archive_scout.network.transports import ResilientTransport, TransportExhaustedError, TransportResponse


class ExhaustedTransport:
    def __init__(self) -> None:
        self.calls = 0

    def request(self, url, headers, max_bytes, stop_event):
        self.calls += 1
        raise TransportExhaustedError(url, [("httpx", TimeoutError("timed out")), ("curl", OSError("offline"))])

    def close(self) -> None:
        return


class FakeBackend:
    def __init__(self, name: str, outcomes: list[object]) -> None:
        self.name = name
        self.outcomes = list(outcomes)
        self.calls = 0

    def request(self, url, headers, max_bytes, stop_event):
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return TransportResponse(200, {}, url, b"ok", self.name, 0.01)

    def close(self) -> None:
        return


class V3NetworkRecoveryTests(unittest.TestCase):
    def test_transport_falls_back_and_remembers_the_working_backend(self) -> None:
        first = FakeBackend("httpx", [OSError("blocked")])
        second = FakeBackend("urllib3", [object(), object()])
        transport = ResilientTransport.__new__(ResilientTransport)
        transport.callback = None
        transport.lock = threading.Lock()
        transport.cooldown_until = {}
        transport.last_success = None
        transport.backends = {"httpx": first, "urllib3": second}
        transport.order = ["httpx", "urllib3"]

        response = transport.request("https://web.archive.org/", {}, 1024, threading.Event())
        self.assertEqual(response.backend, "urllib3")
        self.assertEqual(first.calls, 1)
        self.assertEqual(second.calls, 1)

        response = transport.request("https://web.archive.org/", {}, 1024, threading.Event())
        self.assertEqual(response.backend, "urllib3")
        self.assertEqual(first.calls, 1)
        self.assertEqual(second.calls, 2)

    def test_exhausted_multi_backend_error_becomes_resumable_transient_error(self) -> None:
        transport = ExhaustedTransport()
        client = HttpClient(
            FixedRateLimiter(0),
            retries=1,
            timeout=1,
            user_agent="test",
            stop_event=threading.Event(),
            transport=transport,
        )
        with self.assertRaises(TransientRequestError) as raised:
            client.get("https://web.archive.org/cdx/search/cdx", 1024)
        self.assertTrue(raised.exception.timed_out)
        self.assertTrue(raised.exception.splittable)
        self.assertEqual(transport.calls, 1)

    def test_unreachable_wayback_pauses_cleanly_and_persists_queue(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                text_collapse_scope="year",
                output_dir=root,
                targets=["example.com/*"],
                keywords=["x"],
                from_date="20010101000000",
                to_date="20010101000000",
                cdx_delay=0,
                retries=1,
                network=NetworkConfig(
                    index_strategy="resume",
                    failure_pause_threshold=2,
                    retry_base_seconds=1,
                    retry_max_seconds=1,
                ),
            ).normalized()
            database = open_database(root)
            transient = TransientRequestError("offline", timed_out=True, splittable=True)
            with patch("archive_scout.cdx.client.HttpClient.get_cdx_any", side_effect=transient), patch(
                "archive_scout.cdx.indexer.transient_backoff", return_value=0
            ):
                with self.assertRaises(ConnectivityPaused):
                    index_archive(config, database, threading.Event())
            state = database.execute("SELECT complete,resume_key FROM index_state").fetchone()
            self.assertEqual(state["complete"], 0)
            self.assertIn('"failures":2', state["resume_key"])
            self.assertEqual(database.execute("SELECT COUNT(*) FROM errors WHERE retryable=1").fetchone()[0], 1)
            database.close()


if __name__ == "__main__":
    unittest.main()
