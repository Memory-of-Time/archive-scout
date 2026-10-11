from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import (
    CDXRows,
    HttpClient,
    TransientRequestError,
    parse_cdx_text_rows,
)
from archive_scout.cdx.indexer import index_archive
from archive_scout.cdx.parallel import PageFetchResult, effective_page_workers
from archive_scout.config import MediaConfig, NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_captures, upsert_media_captures
from archive_scout.downloads.downloader import prepare_download_rows
from archive_scout.downloads.retry import retry_error_urls
from archive_scout.downloads.rate_limit import FixedRateLimiter
from archive_scout.media.downloader import retry_media_errors
from archive_scout.media.indexer import _accept_media_rows
from archive_scout.network.transports import TransportResponse, is_transport_connection_failure
from archive_scout.events import Stopped
from archive_scout.utils import utc_now


class LargeProjectStabilityTests(unittest.TestCase):
    def test_fifty_thousand_text_rows_parse_to_compact_tuples(self):
        lines = [
            f"20010101000000 text/html 200 D{i} 123 http://example.com/page/{i}".encode()
            for i in range(50000)
        ]
        payload = bytearray(b"\n".join(lines) + b"\n\nresume-token\n")
        result = parse_cdx_text_rows(payload, "https://example.invalid/cdx")
        self.assertEqual(len(result.rows), 50000)
        self.assertEqual(result.resume_key, "resume-token")
        self.assertIsInstance(result.rows[0], tuple)
        self.assertEqual(result.rows[-1][1], "http://example.com/page/49999")

    def test_compact_fifty_thousand_row_database_insert(self):
        with tempfile.TemporaryDirectory() as temp:
            database = open_database(Path(temp))
            target_id = get_or_create_target(database, "example.com/*")
            rows = (
                (
                    "20010101000000",
                    f"http://example.com/page/{index}",
                    "text/html",
                    "200",
                    f"D{index}",
                    "123",
                )
                for index in range(50000)
            )
            with database:
                changed = upsert_captures(database, rows, target_id, "stability_test")
            self.assertEqual(changed, 50000)
            self.assertEqual(database.execute("SELECT COUNT(*) FROM captures").fetchone()[0], 50000)
            database.close()

    def test_large_pages_keep_request_speed_but_cap_resident_buffers(self):
        self.assertEqual(effective_page_workers(10, 9), 10)
        self.assertEqual(effective_page_workers(10, 1), 10)

    def test_empty_successful_text_response_is_a_valid_empty_index(self):
        result = parse_cdx_text_rows(bytearray(), "https://example.invalid/cdx")
        self.assertEqual(result.rows, [])
        self.assertIsNone(result.resume_key)


    def test_full_resume_index_commits_fifty_thousand_rows_and_finishes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(text_collapse_scope="year", 
                output_dir=root,
                targets=["example.com/*"],
                keywords=[],
                from_date="20010101",
                to_date="20010101",
                cdx_delay=0,
                network=NetworkConfig(index_strategy="resume"),
            ).normalized()
            database = open_database(root)
            first_page = CDXRows(
                [
                    (
                        "20010101000000",
                        f"http://example.com/page/{index}",
                        "text/html",
                        "200",
                        f"D{index}",
                        "123",
                    )
                    for index in range(50000)
                ],
                "next-page",
            )
            with patch(
                "archive_scout.cdx.client.HttpClient.get_cdx_rows_any",
                side_effect=[first_page, CDXRows([])],
            ) as request:
                index_archive(config, database, threading.Event())
            self.assertEqual(request.call_count, 2)
            self.assertEqual(database.execute("SELECT COUNT(*) FROM captures").fetchone()[0], 50000)
            state = database.execute("SELECT complete FROM index_state").fetchone()
            self.assertEqual(state["complete"], 1)
            database.close()

    def test_complete_connection_failure_pauses_after_three_saved_attempts(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(text_collapse_scope="year", 
                output_dir=root,
                targets=["example.com/*"],
                keywords=[],
                from_date="20010101",
                to_date="20010101",
                cdx_delay=0,
                network=NetworkConfig(
                    index_strategy="resume",
                    connection_failure_pause_threshold=3,
                    connection_retry_seconds=1,
                ),
            ).normalized()
            database = open_database(root)
            offline = TransientRequestError(
                "DNS/proxy/TLS setup failed",
                connection_failed=True,
                splittable=False,
            )
            class CancelAfterRetries(threading.Event):
                def __init__(self):
                    super().__init__()
                    self.waits = 0
                def wait(self, _timeout=None):
                    self.waits += 1
                    if self.waits >= 3:
                        self.set()
                    return self.is_set()
            stop = CancelAfterRetries()
            with patch("archive_scout.cdx.client.HttpClient.get_cdx_any", side_effect=offline) as request:
                with self.assertRaises(Stopped):
                    index_archive(config, database, stop)
            self.assertEqual(request.call_count, 3)
            state = database.execute("SELECT complete,resume_key FROM index_state").fetchone()
            self.assertEqual(state["complete"], 0)
            self.assertIn('"failures":3', state["resume_key"])
            database.close()


    def test_paged_complete_connection_failure_uses_the_saved_connection_circuit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(text_collapse_scope="year", 
                output_dir=root,
                targets=["example.com/*"],
                keywords=[],
                from_date="20010101",
                to_date="20011231",
                cdx_delay=0,
                network=NetworkConfig(
                    index_strategy="paged",
                    connection_failure_pause_threshold=3,
                    connection_retry_seconds=1,
                ),
            ).normalized()
            database = open_database(root)
            offline = TransientRequestError(
                "all transports failed during connection setup",
                connection_failed=True,
                splittable=False,
            )

            class FakeClient:
                def __init__(self):
                    self.page_count_calls = 0
                    self.closed = False

                def get_cdx_any(self, *_args, **_kwargs):
                    self.page_count_calls += 1
                    return "1"

                def close(self):
                    self.closed = True

            client = FakeClient()
            page_attempts = []

            def failed_pages(*_args, **_kwargs):
                page_attempts.append(1)
                yield PageFetchResult(0, [], 0.01, offline)

            class CancelAfterRetries(threading.Event):
                def __init__(self):
                    super().__init__()
                    self.waits = 0
                def wait(self, _timeout=None):
                    self.waits += 1
                    if self.waits >= 3:
                        self.set()
                    return self.is_set()
            with patch("archive_scout.cdx.indexer._client_for_config", return_value=client), patch(
                "archive_scout.cdx.indexer.iter_cdx_pages", side_effect=failed_pages
            ):
                with self.assertRaises(Stopped):
                    index_archive(config, database, CancelAfterRetries())

            self.assertEqual(client.page_count_calls, 1)
            self.assertEqual(len(page_attempts), 3)
            self.assertTrue(client.closed)
            state = database.execute(
                "SELECT complete,resume_key FROM index_state"
            ).fetchone()
            self.assertEqual(state["complete"], 0)
            self.assertIn('"failures":3', state["resume_key"])
            database.close()

    def test_large_explicit_retry_selection_avoids_sqlite_variable_limit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(text_collapse_scope="year", 
                output_dir=root,
                targets=["example.com/*"],
                keywords=["example"],
                from_date="2001",
                to_date="2001",
            ).normalized()
            database = open_database(root)
            target_id = get_or_create_target(database, "example.com/*")
            with database:
                upsert_captures(
                    database,
                    (
                        (
                            "20010101000000",
                            f"http://example.com/page/{index}.html",
                            "text/html",
                            "200",
                            f"D{index}",
                            "123",
                        )
                        for index in range(2000)
                    ),
                    target_id,
                    "selection-test",
                )
            selected = [row[0] for row in database.execute(
                "SELECT id FROM captures ORDER BY id LIMIT 1500"
            )]
            total, rows = prepare_download_rows(
                database, config, [], states=("pending",), capture_ids=selected
            )
            self.assertEqual(total, 1500)
            self.assertEqual(sum(1 for _ in rows), 1500)
            database.close()

    def test_media_filter_keeps_compact_cdx_rows_compact(self):
        row = (
            "20010101000000",
            "http://example.com/image.jpg",
            "image/jpeg",
            "200",
            "DIGEST",
            "123",
        )
        accepted = _accept_media_rows([row], MediaConfig().normalized())
        self.assertEqual(len(accepted), 1)
        self.assertIs(accepted[0][0], row)
        self.assertIsInstance(accepted[0][0], tuple)

    def test_curl_connection_timeout_wording_is_classified_as_connection_failure(self):
        self.assertTrue(
            is_transport_connection_failure(
                OSError("curl: (7) Failed to connect to web.archive.org: Connection timed out")
            )
        )

    def test_download_candidates_are_streamed_from_a_sqlite_queue(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(text_collapse_scope="year", 
                output_dir=root,
                targets=["example.com/*"],
                keywords=["example"],
                from_date="2001",
                to_date="2001",
            ).normalized()
            database = open_database(root)
            target_id = get_or_create_target(database, "example.com/*")
            with database:
                upsert_captures(
                    database,
                    (
                        (
                            "20010101000000",
                            f"http://example.com/page/{index}",
                            "text/html",
                            "200",
                            f"D{index}",
                            "123",
                        )
                        for index in range(5000)
                    ),
                    target_id,
                    "queue-test",
                )
                database.execute(
                    "UPDATE captures SET query_signature=?",
                    ("queue-test",),
                )
            # Use the actual signature selected by the config for this focused queue test.
            from archive_scout.cdx.parameters import cdx_query_signature
            with database:
                database.execute(
                    "UPDATE captures SET query_signature=?",
                    (cdx_query_signature(config),),
                )
            total, rows = prepare_download_rows(database, config, [], states=("pending",))
            self.assertEqual(total, 5000)
            self.assertFalse(isinstance(rows, list))
            first_ten = [next(rows)["id"] for _ in range(10)]
            self.assertEqual(first_ten, sorted(first_ten))
            database.close()


    def test_oversized_cdx_response_becomes_splittable_saved_work(self):
        class OversizedTransport:
            def request(self, *_args, **_kwargs):
                raise RuntimeError("response exceeds 67108864 bytes")

            def close(self):
                return None

        client = HttpClient(
            FixedRateLimiter(0),
            retries=1,
            timeout=10,
            user_agent="ArchiveScoutTest",
            stop_event=threading.Event(),
            transport=OversizedTransport(),
        )
        with self.assertRaises(TransientRequestError) as caught:
            client.get_cdx_rows_any(
                ("https://example.invalid/cdx",),
                [("url", "example.com/*"), ("output", "txt")],
                max_bytes=1024,
            )
        self.assertTrue(caught.exception.splittable)
        self.assertIn("safe in-memory budget", str(caught.exception))

    def test_parser_memory_pressure_becomes_splittable_saved_work(self):
        class ValidTransport:
            def request(self, url, _headers, _max_bytes, _stop_event):
                return TransportResponse(
                    status=200,
                    headers={"content-type": "text/plain"},
                    final_url=url,
                    data=b"20010101000000 text/html 200 D 1 http://example.com/\n",
                    backend="test",
                    elapsed=0.01,
                )

            def close(self):
                return None

        client = HttpClient(
            FixedRateLimiter(0),
            retries=1,
            timeout=10,
            user_agent="ArchiveScoutTest",
            stop_event=threading.Event(),
            transport=ValidTransport(),
        )
        with patch("archive_scout.cdx.client.parse_cdx_text_rows", side_effect=MemoryError):
            with self.assertRaises(TransientRequestError) as caught:
                client.get_cdx_rows_any(
                    ("https://example.invalid/cdx",),
                    [("url", "example.com/*"), ("output", "txt")],
                )
        self.assertTrue(caught.exception.splittable)
        self.assertIn("exceeded available memory", str(caught.exception))


    def test_large_error_retry_selection_avoids_sqlite_variable_limit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = open_database(root)
            target_id = get_or_create_target(database, "example.com/*")
            with database:
                upsert_captures(
                    database,
                    (
                        (
                            "20010101000000",
                            f"http://example.com/retry/{index}",
                            "text/html",
                            "200",
                            f"D{index}",
                            "123",
                        )
                        for index in range(1200)
                    ),
                    target_id,
                    "retry-selection",
                )
                capture_ids = [int(row[0]) for row in database.execute("SELECT id FROM captures ORDER BY id")]
                now = utc_now()
                database.executemany(
                    """
                    INSERT INTO errors(capture_id,operation,category,message,retryable,first_seen,last_seen)
                    VALUES(?, 'download', 'timeout', 'timeout', 1, ?, ?)
                    """,
                    ((capture_id, now, now) for capture_id in capture_ids),
                )
            config = ProjectConfig(text_collapse_scope="year", 
                output_dir=root,
                targets=["example.com/*"],
                keywords=["example"],
                retry_capture_ids=capture_ids,
            ).normalized()
            with patch("archive_scout.downloads.retry.download_archive") as mocked:
                retry_error_urls(config, database, 1, threading.Event(), None)
            self.assertEqual(len(mocked.call_args.kwargs["capture_ids"]), 1200)
            database.close()

    def test_large_media_retry_selection_avoids_sqlite_variable_limit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = open_database(root)
            with database:
                upsert_media_captures(
                    database,
                    (
                        (
                            (
                                "20010101000000",
                                f"http://example.com/media/{index}.jpg",
                                "image/jpeg",
                                "200",
                                f"M{index}",
                                "123",
                            ),
                            "image",
                            ".jpg",
                        )
                        for index in range(1200)
                    ),
                    None,
                    "media-retry-selection",
                )
                media_ids = [int(row[0]) for row in database.execute("SELECT id FROM media_captures ORDER BY id")]
                database.execute("UPDATE media_captures SET state='error'")
                now = utc_now()
                database.executemany(
                    """
                    INSERT INTO errors(media_capture_id,operation,category,message,retryable,first_seen,last_seen)
                    VALUES(?, 'media_download', 'timeout', 'timeout', 1, ?, ?)
                    """,
                    ((media_id, now, now) for media_id in media_ids),
                )
            config = ProjectConfig(text_collapse_scope="year", output_dir=root, targets=["example.com/*"], keywords=[]).normalized()
            with patch("archive_scout.media.downloader.download_media") as mocked:
                retry_media_errors(
                    config, database, threading.Event(), media_capture_ids=media_ids
                )
            self.assertEqual(len(mocked.call_args.kwargs["media_capture_ids"]), 1200)
            database.close()

    def test_unexpected_local_index_error_is_not_retried_forever(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(text_collapse_scope="year", 
                output_dir=root,
                targets=["example.com/*"],
                keywords=[],
                from_date="20010101",
                to_date="20010101",
                cdx_delay=0,
                network=NetworkConfig(index_strategy="resume"),
            ).normalized()
            database = open_database(root)
            with patch(
                "archive_scout.cdx.client.HttpClient.get_cdx_any",
                side_effect=ValueError("local parser defect"),
            ) as request:
                with self.assertRaisesRegex(ValueError, "local parser defect"):
                    index_archive(config, database, threading.Event())
            self.assertEqual(request.call_count, 1)
            error = database.execute(
                "SELECT category,retryable FROM errors ORDER BY id DESC LIMIT 1"
            ).fetchone()
            self.assertEqual(error["category"], "unexpected_index_error")
            self.assertEqual(error["retryable"], 0)
            database.close()


if __name__ == "__main__":
    unittest.main()
