from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

from archive_scout.cdx.client import HttpClient, PermanentRequestError
from archive_scout.content import classify_replay_content
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import blocked_site_reasons, list_site_issues, record_site_issue
from archive_scout.downloads.rate_limit import FixedRateLimiter
from archive_scout.downloads.validation import classify_exception
from archive_scout.network.transports import TransportResponse


class _PolicyTransport:
    def __init__(self, runtime_error: str) -> None:
        self.runtime_error = runtime_error

    def request(self, url, headers, max_bytes, stop_event):
        return TransportResponse(
            403,
            {"x-archive-wayback-runtime-error": self.runtime_error},
            url,
            b"",
            "test",
            0.01,
        )

    def close(self) -> None:
        pass


class SiteIssueTests(unittest.TestCase):
    def test_replay_policy_pages_have_specific_categories(self):
        self.assertEqual(
            classify_replay_content("This URL has been excluded from the Wayback Machine", "https://web.archive.org/web/1/http://x/"),
            "wayback_excluded",
        )
        self.assertEqual(
            classify_replay_content("Page cannot be displayed due to robots.txt", "https://web.archive.org/web/1/http://x/"),
            "robots_blocked",
        )
        self.assertEqual(classify_exception(RuntimeError("robots.txt")), ("robots_blocked", 403, False))

    def test_cdx_http_policy_error_distinguishes_robots(self):
        client = HttpClient(
            FixedRateLimiter(0), 1, 1, "test", threading.Event(),
            transport=_PolicyTransport("Page cannot be displayed due to robots.txt"),
        )
        try:
            with self.assertRaises(PermanentRequestError) as raised:
                client.get("https://web.archive.org/cdx/search/cdx?url=example.com", 1024)
            self.assertEqual(raised.exception.category, "robots_blocked")
            self.assertEqual(classify_exception(raised.exception), ("robots_blocked", 403, False))
        finally:
            client.close()

    def test_site_issue_deduplicates_and_builds_host_circuit(self):
        with tempfile.TemporaryDirectory() as temp:
            database = open_database(Path(temp))
            first = record_site_issue(database, "EXAMPLE.COM", "media_index", "wayback_excluded", "first")
            second = record_site_issue(database, "example.com", "media_index", "wayback_excluded", "second")
            database.commit()
            self.assertEqual(first, second)
            rows = list_site_issues(database)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["occurrence_count"], 2)
            self.assertEqual(blocked_site_reasons(database), {"example.com": "wayback_excluded"})
            database.close()


if __name__ == "__main__":
    unittest.main()
