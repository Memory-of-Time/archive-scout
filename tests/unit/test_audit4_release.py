from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from archive_scout.cdx.parameters import cdx_query_signature
from archive_scout.classification import (
    RESOURCE_CLASSIFIER_REVISION,
    classify_capture_inventory,
    classify_payload_prefix,
)
from archive_scout.config import MediaConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import (
    get_or_create_media_target,
    get_or_create_target,
    upsert_captures,
    upsert_media_captures,
)
from archive_scout.downloads.downloader import (
    _current_discard_spool_bytes,
    _finish_discard_cleanup,
    _reconcile_text_media_handoffs,
    prepare_acquisition_rows,
)
from archive_scout.media.indexer import media_query_signature
from archive_scout.ui.dashboard import read_dashboard_counts
from archive_scout.utils import utc_now


class Audit4ReleaseTests(unittest.TestCase):
    def test_common_payload_policy_avoids_short_magic_false_positives(self):
        self.assertEqual(
            classify_payload_prefix(b"BMW owners discuss archives", "text/plain", "http://x/a.txt").resource_class,
            "text",
        )
        self.assertEqual(
            classify_payload_prefix(b"ID3 formats explained here", "text/plain", "http://x/a.txt").resource_class,
            "text",
        )
        bmp = b"BM" + (54).to_bytes(4, "little") + b"\x00" * 4 + (54).to_bytes(4, "little") + b"\x00" * 40
        self.assertEqual(classify_payload_prefix(bmp, "application/octet-stream", "http://x/a").resource_class, "image")
        id3 = b"ID3\x04\x00\x00\x00\x00\x00\x10" + b"\x00" * 16
        self.assertEqual(classify_payload_prefix(id3, "application/octet-stream", "http://x/a").resource_class, "audio")

    def test_utf16_svg_and_avif_follow_resource_purpose_not_printability(self):
        self.assertEqual(
            classify_payload_prefix("needle".encode("utf-16-le"), "text/plain; charset=utf-16le", "http://x/a").resource_class,
            "text",
        )
        self.assertEqual(
            classify_payload_prefix(b"\xff\xfeh\x00i\x00", "application/octet-stream", "http://x/a").resource_class,
            "text",
        )
        svg = b'<?xml version="1.0"?><svg xmlns="http://www.w3.org/2000/svg"></svg>'
        self.assertEqual(classify_payload_prefix(svg, "application/xml", "http://x/asset").resource_class, "image")
        avif = b"\x00\x00\x00\x20ftypavif\x00\x00\x00\x00avifmif1"
        self.assertEqual(classify_payload_prefix(avif, "application/octet-stream", "http://x/asset").resource_class, "image")

    def test_collapsed_ranges_are_date_bound_but_uncollapsed_ranges_reuse_coverage(self):
        a = ProjectConfig(Path("."), ["example.com/*"], [], from_date="2000", to_date="2009").normalized()
        b = ProjectConfig(Path("."), ["example.com/*"], [], from_date="2000", to_date="2019").normalized()
        self.assertNotEqual(cdx_query_signature(a), cdx_query_signature(b))
        ua = ProjectConfig(Path("."), ["example.com/*"], [], from_date="2000", to_date="2009", cdx_collapses=[]).normalized()
        ub = ProjectConfig(Path("."), ["example.com/*"], [], from_date="2000", to_date="2019", cdx_collapses=[]).normalized()
        self.assertEqual(cdx_query_signature(ua), cdx_query_signature(ub))

    def test_replay_selector_enforces_active_date_bounds_for_date_independent_inventory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            broad = ProjectConfig(root, ["example.com/*"], [], from_date="2000", to_date="2010", cdx_collapses=[]).normalized()
            narrow = ProjectConfig(root, ["example.com/*"], [], from_date="2005", to_date="2005", cdx_collapses=[]).normalized()
            self.assertEqual(cdx_query_signature(broad), cdx_query_signature(narrow))
            db = open_database(root)
            now = utc_now()
            with db:
                for year in (2000, 2005, 2010):
                    db.execute(
                        """INSERT INTO captures(original_url,timestamp,query_signature,mimetype,statuscode,length,state,
                               resource_class,resource_classifier_revision,created_at,updated_at)
                           VALUES(?,?,?,?,?,100,'pending','text',?,?,?)""",
                        (f"http://example.com/{year}", f"{year}0101000000", cdx_query_signature(broad), "text/html", "200",
                         RESOURCE_CLASSIFIER_REVISION, now, now),
                    )
            _total, rows, _stats = prepare_acquisition_rows(db, narrow, None)
            self.assertEqual([str(row["timestamp"]) for row in rows], ["20050101000000"])
            db.close()

    def test_cdx_insert_persists_urlkey_and_current_classification_without_second_pass(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(root, ["example.com/*"], [], from_date="2001", to_date="2001").normalized()
            db = open_database(root)
            with db:
                target = get_or_create_target(db, "example.com/*", {})
                upsert_captures(db, [{
                    "urlkey": "com,example)/a", "timestamp": "20010101000000", "original": "http://example.com/a",
                    "mimetype": "text/html", "statuscode": "200", "digest": "d", "length": "10",
                }], target, cdx_query_signature(cfg))
            row = db.execute("SELECT urlkey,resource_class,resource_classifier_revision FROM captures").fetchone()
            self.assertEqual((row["urlkey"], row["resource_class"], int(row["resource_classifier_revision"])),
                             ("com,example)/a", "text", RESOURCE_CLASSIFIER_REVISION))
            counts = classify_capture_inventory(db, cdx_query_signature(cfg))
            self.assertEqual(sum(counts.values()), 0)
            db.close()

    def test_stale_classifier_keyset_plan_needs_no_temp_sort(self):
        with tempfile.TemporaryDirectory() as temp:
            db = open_database(Path(temp))
            plan = [str(row[3]) for row in db.execute(
                """EXPLAIN QUERY PLAN SELECT id,original_url,mimetype,state,skip_reason
                   FROM captures INDEXED BY captures_classification_idx
                   WHERE query_signature=? AND id>? AND resource_classifier_revision<?
                   ORDER BY id LIMIT ?""",
                ("sig", 0, RESOURCE_CLASSIFIER_REVISION, 5000),
            )]
            self.assertFalse(any("TEMP B-TREE" in item.upper() for item in plan), plan)
            db.close()

    def test_media_urlkey_is_persisted_and_snapshot_signature_is_date_bound(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(root, ["example.com/*"], [], from_date="2001", to_date="2002",
                                media=MediaConfig(enabled=True, include_images=True, include_videos=False)).normalized()
            later = ProjectConfig(root, ["example.com/*"], [], from_date="2001", to_date="2003",
                                  media=cfg.media).normalized()
            self.assertNotEqual(media_query_signature(cfg), media_query_signature(later))
            db = open_database(root)
            with db:
                target = get_or_create_media_target(db, "example.com/*")
                upsert_media_captures(db, [({
                    "urlkey": "com,example)/img", "timestamp": "20010101000000", "original": "http://example.com/img.jpg",
                    "mimetype": "image/jpeg", "statuscode": "200", "digest": "d", "length": "10",
                }, "image", ".jpg")], target, media_query_signature(cfg))
            self.assertEqual(db.execute("SELECT urlkey FROM media_captures").fetchone()[0], "com,example)/img")
            db.close()

    def test_handoff_snapshot_reconciliation_uses_urlkey_and_keeps_earliest(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(root, ["example.com/*"], [], from_date="2001", to_date="2001",
                                media=MediaConfig(enabled=True, include_images=True, include_videos=False,
                                                  snapshot_strategy="earliest")).normalized()
            db = open_database(root)
            sig = media_query_signature(cfg)
            image_dir = root / "media" / "images"
            image_dir.mkdir(parents=True)
            p1, p2 = image_dir / "a1.png", image_dir / "a2.png"
            p1.write_bytes(b"a"); p2.write_bytes(b"b")
            now = utc_now()
            with db:
                for stamp, path in (("20010101000000", p1), ("20010701000000", p2)):
                    db.execute(
                        """INSERT INTO media_captures(original_url,timestamp,source_type,query_signature,urlkey,media_kind,
                               extension,mimetype,statuscode,length,state,path,created_at,updated_at)
                           VALUES('http://example.com/asset?id=1',?,'text_validation_handoff',?,'com,example)/asset','image',
                                  '.png','image/png','200',1,'downloaded',?,?,?)""",
                        (stamp, sig, str(path), now, now),
                    )
                _reconcile_text_media_handoffs(db, cfg, sig)
            rows = db.execute("SELECT timestamp,state,path FROM media_captures ORDER BY timestamp").fetchall()
            self.assertEqual([(row["timestamp"], row["state"]) for row in rows],
                             [("20010101000000", "downloaded"), ("20010701000000", "skipped_strategy")])
            self.assertTrue(p1.exists()); self.assertFalse(p2.exists())
            db.close()

    def test_shared_retained_path_is_never_unlinked_by_discard_cleanup(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(root, ["example.com/*"], [], text_retention="discard_after_scan").normalized()
            db = open_database(root)
            path = root / "captures" / "shared.txt"
            path.parent.mkdir(parents=True); path.write_text("shared", encoding="utf-8")
            now = utc_now()
            with db:
                discard_id = db.execute(
                    """INSERT INTO captures(original_url,timestamp,query_signature,state,local_path,payload_availability,
                           payload_origin,payload_retention,created_at,updated_at)
                       VALUES('http://example.com/a','20010101000000','s','downloaded',?,'cleanup_pending',
                              'acquired','discard_after_scan',?,?)""", (str(path), now, now)).lastrowid
                db.execute(
                    """INSERT INTO captures(original_url,timestamp,query_signature,state,local_path,payload_availability,
                           payload_origin,payload_retention,created_at,updated_at)
                       VALUES('http://example.com/b','20010102000000','s','downloaded',?,'retained',
                              'legacy','keep',?,?)""", (str(path), now, now))
            settled = _finish_discard_cleanup(db, cfg, [(int(discard_id), path)])
            self.assertIn(int(discard_id), settled)
            self.assertTrue(path.exists())
            row = db.execute("SELECT payload_availability,payload_retention,cleanup_pending FROM captures WHERE id=?", (discard_id,)).fetchone()
            self.assertEqual((row["payload_availability"], row["payload_retention"], int(row["cleanup_pending"])),
                             ("retained", "keep", 0))
            db.close()

    def test_partial_discard_file_is_included_in_resumed_spool_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = open_database(root)
            final = root / "captures" / "a.txt"
            final.parent.mkdir(parents=True)
            partial = final.with_name(final.name + ".part")
            partial.write_bytes(b"x" * 12345)
            now = utc_now()
            with db:
                db.execute(
                    """INSERT INTO captures(original_url,timestamp,query_signature,state,local_path,payload_availability,
                           payload_origin,payload_retention,created_at,updated_at)
                       VALUES('http://example.com/a','20010101000000','s','pending',?,'partial',
                              'acquired','discard_after_scan',?,?)""", (str(final), now, now))
            self.assertEqual(_current_discard_spool_bytes(db, root), 12345)
            db.close()

    def test_dashboard_deadline_never_turns_interrupted_counts_into_exact_zero(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            db = open_database(root)
            now = utc_now()
            with db:
                db.executemany(
                    """INSERT INTO captures(original_url,timestamp,query_signature,state,created_at,updated_at)
                       VALUES(?, '20010101000000','s','pending',?,?)""",
                    ((f"http://example.com/{i}", now, now) for i in range(6000)),
                )
            db.close()
            ticks = {"n": 0}
            def fake_monotonic():
                ticks["n"] += 1
                return 0.0 if ticks["n"] == 1 else 999.0
            with mock.patch("archive_scout.ui.dashboard.time.monotonic", side_effect=fake_monotonic):
                counts = read_dashboard_counts(root / "archive_scout.sqlite3", max_query_seconds=1.0)
            self.assertFalse(counts["_exact"])
            self.assertEqual(counts["_status"], "deadline_exceeded")
            self.assertTrue(any(counts[key] is None for key in ("pending", "downloaded_unscanned", "downloaded")))


if __name__ == "__main__":
    unittest.main()
