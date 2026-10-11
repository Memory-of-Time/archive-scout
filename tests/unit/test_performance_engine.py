from __future__ import annotations

import hashlib
import tempfile
import threading
import unittest
from pathlib import Path

from archive_scout.cdx.client import HttpClient
from archive_scout.config import ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import (
    get_or_create_keyword_set,
    get_or_create_target,
    start_scan_run,
    upsert_capture,
    upsert_captures,
    upsert_document,
    save_match,
)
from archive_scout.downloads.downloader import prepare_download_rows
from archive_scout.downloads.rate_limit import FixedRateLimiter
from archive_scout.media.downloader import fetch_media, iter_media_download_rows
from archive_scout.network.transports import TransportFileResponse
from archive_scout.scanning.automaton import LiteralAutomaton
from archive_scout.scanning.keywords import compile_keywords, compile_prefilter
from archive_scout.scanning.scoring import link_is_interesting
from archive_scout.scanning.rescanner import rescan_documents
from archive_scout.utils import hash_text, normalize_search, utc_now


class PerformanceEngineTests(unittest.TestCase):
    def test_aho_automaton_handles_large_literal_sets(self):
        patterns = [f"keyword-{index}" for index in range(5000)] + ["lost media archive"]
        automaton = LiteralAutomaton(patterns)
        self.assertTrue(automaton.search_any("a lost media archive reference"))
        self.assertEqual(automaton.find("keyword-7 and keyword-70"), {"keyword-7", "keyword-70"})
        self.assertFalse(automaton.search_any("unrelated text"))

        compiled = compile_keywords(patterns)
        prefilter = compile_prefilter(compiled)
        self.assertTrue(prefilter.matches({"body": ""}, {"body": "a lost media archive reference"}))
        self.assertFalse(prefilter.matches({"body": ""}, {"body": "unrelated text"}))


    def test_excluded_only_prefilter_preserves_positive_link_and_download_scope_semantics(self):
        patterns = compile_keywords(["excluded: blocked"])
        prefilter = compile_prefilter(patterns)
        self.assertFalse(prefilter.has_positive_rules)
        self.assertFalse(
            link_is_interesting("http://example.com/ordinary.html", patterns, prefilter)
        )

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = open_database(root)
            target_id = get_or_create_target(database, "example.com/*")
            upsert_capture(
                database,
                {
                    "original": "http://example.com/ordinary.html",
                    "timestamp": "20010101000000",
                    "mimetype": "text/html",
                    "statuscode": "200",
                    "digest": "A",
                    "length": "100",
                },
                target_id,
                "sig",
            )
            capture_id = int(database.execute("SELECT id FROM captures").fetchone()[0])
            config = ProjectConfig(
                output_dir=root,
                targets=["example.com/*"],
                keywords=["excluded: blocked"],
                download_scope="keyword_urls",
            )
            total, rows = prepare_download_rows(
                database, config, patterns, capture_ids=[capture_id]
            )
            self.assertEqual(total, 0)
            self.assertEqual(list(rows), [])
            self.assertEqual(
                database.execute("SELECT state FROM captures WHERE id=?", (capture_id,)).fetchone()[0],
                "skipped",
            )
            database.close()

    def test_unchanged_cdx_rows_do_not_rewrite_database(self):
        with tempfile.TemporaryDirectory() as temp:
            database = open_database(Path(temp))
            target_id = get_or_create_target(database, "example.com/*")
            rows = [
                ("20010101000000", "http://example.com/a", "text/html", "200", "A", "10"),
                ("20010101000001", "http://example.com/b", "text/html", "200", "B", "20"),
            ]
            with database:
                self.assertEqual(upsert_captures(database, rows, target_id, "sig"), 2)
            updated = database.execute(
                "SELECT GROUP_CONCAT(updated_at,'|') FROM captures ORDER BY id"
            ).fetchone()[0]
            with database:
                self.assertEqual(upsert_captures(database, rows, target_id, "sig"), 0)
            self.assertEqual(
                updated,
                database.execute("SELECT GROUP_CONCAT(updated_at,'|') FROM captures ORDER BY id").fetchone()[0],
            )
            database.close()


    def test_unchanged_document_and_match_do_not_rewrite_large_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            path = root / "page.html"
            raw = "<html><body>archive body</body></html>"
            path.write_text(raw, encoding="utf-8")
            database = open_database(root)
            target_id = get_or_create_target(database, "example.com/*")
            upsert_capture(
                database,
                {
                    "original": "http://example.com/a",
                    "timestamp": "20010101000000",
                    "mimetype": "text/html",
                    "statuscode": "200",
                    "digest": "A",
                    "length": str(path.stat().st_size),
                },
                target_id,
                "sig",
            )
            capture_id = int(database.execute("SELECT id FROM captures").fetchone()[0])
            document_id = upsert_document(
                database,
                capture_id,
                path,
                "Archive",
                "archive body",
                [],
                hash_text(raw),
                hash_text(normalize_search("archive body")),
                path.stat().st_size,
            )
            keyword_set_id = get_or_create_keyword_set(database, "set", ["archive"])
            run_id = start_scan_run(database, keyword_set_id, "run", 1, "rescan")
            analysis = {
                "score": 10,
                "hits": {"archive": 1},
                "hit_fields": {"archive": ["body"]},
                "snippets": ["archive body"],
                "interesting_links": [],
                "proximity": {},
            }
            save_match(database, run_id, document_id, analysis)
            database.commit()
            before = database.total_changes
            with database:
                same_document_id = upsert_document(
                    database,
                    capture_id,
                    path,
                    "Archive",
                    "archive body",
                    [],
                    hash_text(raw),
                    hash_text(normalize_search("archive body")),
                    path.stat().st_size,
                )
                save_match(database, run_id, document_id, analysis)
            self.assertEqual(same_document_id, document_id)
            self.assertEqual(database.total_changes - before, 0)
            database.close()

    def test_text_and_media_queues_prioritize_known_small_files(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = open_database(root)
            target_id = get_or_create_target(database, "example.com/*")
            ids = []
            for index, length in enumerate((500, 0, 20, 100)):
                upsert_capture(
                    database,
                    {
                        "original": f"http://example.com/{index}.html",
                        "timestamp": f"2001010100000{index}",
                        "mimetype": "text/html",
                        "statuscode": "200",
                        "digest": str(index),
                        "length": str(length),
                    },
                    target_id,
                    "sig",
                )
                ids.append(int(database.execute("SELECT last_insert_rowid()").fetchone()[0]))
            config = ProjectConfig(output_dir=root, targets=["example.com/*"], keywords=["example"])
            total, rows = prepare_download_rows(database, config, [], capture_ids=ids)
            ordered = [int(row["length"]) for row in rows]
            self.assertEqual(total, 4)
            self.assertEqual(ordered, [20, 100, 500, 0])

            now = utc_now()
            database.executemany(
                """
                INSERT INTO media_captures(
                    original_url,timestamp,query_signature,media_kind,extension,length,state,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,'pending',?,?)
                """,
                (
                    (f"http://example.com/{index}.jpg", f"2001010100001{index}", "media", "image", ".jpg", length, now, now)
                    for index, length in enumerate((900, 0, 30, 300))
                ),
            )
            total, media_rows = iter_media_download_rows(database, ["state='pending'"], [])
            self.assertEqual(total, 4)
            self.assertEqual([int(row["length"]) for row in media_rows], [30, 300, 900, 0])
            database.close()

    def test_rescan_uses_bounded_parallel_workers_and_preserves_results(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = open_database(root)
            target_id = get_or_create_target(database, "example.com/*")
            for index in range(24):
                path = root / f"page-{index}.html"
                body = f"<html><body>archive needle {index}</body></html>"
                path.write_text(body, encoding="utf-8")
                upsert_capture(
                    database,
                    {
                        "original": f"http://example.com/{index}",
                        "timestamp": f"2001010100{index:04d}",
                        "mimetype": "text/html",
                        "statuscode": "200",
                        "digest": str(index),
                        "length": str(path.stat().st_size),
                    },
                    target_id,
                    "sig",
                )
                capture_id = int(database.execute("SELECT id FROM captures ORDER BY id DESC LIMIT 1").fetchone()[0])
                upsert_document(
                    database,
                    capture_id,
                    path,
                    "",
                    f"archive needle {index}",
                    [],
                    hash_text(body),
                    hash_text(normalize_search(body)),
                    path.stat().st_size,
                )
            keyword_set_id = get_or_create_keyword_set(database, "parallel", ["archive needle"])
            run_id = start_scan_run(database, keyword_set_id, "parallel", 1, "rescan")
            progress = []
            rescan_documents(
                database,
                run_id,
                ["archive needle"],
                threading.Event(),
                progress.append,
                workers=4,
            )
            self.assertEqual(
                database.execute("SELECT COUNT(*) FROM document_matches WHERE scan_run_id=?", (run_id,)).fetchone()[0],
                24,
            )
            self.assertTrue(progress)
            self.assertEqual(progress[-1].detail["workers"], 4)
            database.commit()
            before = database.total_changes
            rescan_documents(
                database,
                run_id,
                ["archive needle"],
                threading.Event(),
                workers=4,
            )
            self.assertEqual(database.total_changes - before, 0)
            database.close()


    def test_http_client_streamed_transfer_uses_transport_file_response(self):
        class FileTransport:
            def close(self):
                return None

            def download(self, url, _headers, destination, max_bytes, stop_event):
                self.url = url
                self.max_bytes = max_bytes
                self.stop_event = stop_event
                data = b"streamed-body" * 2048
                Path(destination).write_bytes(data)
                return TransportFileResponse(
                    status=200,
                    headers={"content-type": "application/octet-stream"},
                    final_url=url,
                    path=Path(destination),
                    bytes_written=len(data),
                    content_hash=hashlib.sha256(data).hexdigest(),
                    preview=data[:20000],
                    backend="test",
                    elapsed=0.01,
                )

        with tempfile.TemporaryDirectory() as temp:
            transport = FileTransport()
            stop_event = threading.Event()
            client = HttpClient(
                FixedRateLimiter(0),
                1,
                5,
                "Archive Scout test",
                stop_event,
                transport=transport,
            )
            destination = Path(temp) / "body.part"
            result = client.download_to_path(
                "https://example.invalid/file", destination, 1_000_000
            )
            self.assertEqual(result["backend"], "test")
            self.assertEqual(result["bytes"], destination.stat().st_size)
            self.assertEqual(result["content_hash"], hashlib.sha256(destination.read_bytes()).hexdigest())
            client.close()

    def test_media_fetch_streams_to_part_file_without_get_buffer(self):
        class StreamingClient:
            def download_to_path(self, _url, destination, _max_bytes, _accept="*/*"):
                data = b"JPEG" * 4096
                Path(destination).write_bytes(data)
                return {
                    "path": Path(destination),
                    "bytes": len(data),
                    "content_hash": hashlib.sha256(data).hexdigest(),
                    "preview": data[:20000],
                    "status": 200,
                    "headers": {"content-type": "image/jpeg"},
                    "final_url": "https://web.archive.org/web/20010101000000id_/http://example.com/a.jpg",
                }

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            database = open_database(root)
            now = utc_now()
            database.execute(
                """
                INSERT INTO media_captures(
                    original_url,timestamp,query_signature,media_kind,extension,length,state,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,'pending',?,?)
                """,
                ("http://example.com/a.jpg", "20010101000000", "sig", "image", ".jpg", 16384, now, now),
            )
            row = database.execute("SELECT * FROM media_captures").fetchone()
            config = ProjectConfig(output_dir=root, targets=["example.com/*"], keywords=["example"])
            result = fetch_media(row, config, StreamingClient())
            self.assertTrue(Path(result["path"]).exists())
            self.assertEqual(result["bytes"], 16384)
            self.assertFalse(Path(str(result["path"]) + ".part").exists())
            database.close()


if __name__ == "__main__":
    unittest.main()
