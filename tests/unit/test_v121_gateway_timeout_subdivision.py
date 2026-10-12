"""Bounded HTTP 504 recovery, without lost windows or healthy-path overhead."""
from __future__ import annotations

import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.client import HttpClient, TransientRequestError
from archive_scout.cdx.indexer import (
    GATEWAY_504_MAX_SPLITS_WITHOUT_RESPONSE,
    IndexPlan, PendingWindow, _bisect_persistent_gateway_504,
    _maybe_subdivide_persistent_gateway_504, decode_plan, encode_plan,
    index_archive,
)
from archive_scout.config import NetworkConfig, ProjectConfig
from archive_scout.database.connection import open_database
from archive_scout.events import Stopped


def configuration(root: Path) -> ProjectConfig:
    return ProjectConfig(
        output_dir=root, targets=["*.liveleak.com/*"], keywords=[],
        from_date="20060101", to_date="20081231", cdx_delay=0,
        retries=1, text_collapse_scope="range",
        network=NetworkConfig(index_strategy="resume"),
    ).normalized()


def gateway() -> TransientRequestError:
    return TransientRequestError("HTTP 504 after one request", status=504, splittable=True)


class Gateway504RecoveryTests(unittest.TestCase):
    def test_bisection_has_no_gaps_or_overlaps_and_preserves_settings(self):
        original = PendingWindow("20060101000000", "20081231235959", page_size=100000,
                                 strategy="resume", page_blocks=9, pagination_supported=False)
        left, right = _bisect_persistent_gateway_504(original)
        from datetime import datetime, timedelta
        stamp = lambda value: datetime.strptime(value, "%Y%m%d%H%M%S")
        self.assertEqual(left.start, original.start)
        self.assertEqual(right.end, original.end)
        self.assertEqual(stamp(left.end) + timedelta(seconds=1), stamp(right.start))
        self.assertEqual((left.page_size, right.page_size), (100000, 100000))
        self.assertEqual((left.page_blocks, right.page_blocks), (9, 9))
        self.assertFalse(left.pagination_supported)
        self.assertEqual(len(_bisect_persistent_gateway_504(PendingWindow(
            "20060101000000", "20060102000000"))), 0)
        original.resume_key = "keep-this-cursor"
        self.assertEqual(_bisect_persistent_gateway_504(original), [])
        original.resume_key = None
        original.page = 2
        self.assertEqual(_bisect_persistent_gateway_504(original), [])
        original.page = 0
        original.page_count = 4
        self.assertEqual(_bisect_persistent_gateway_504(original), [])

    def test_gateway_failure_counter_is_persisted_and_reset_by_other_errors(self):
        with tempfile.TemporaryDirectory() as folder:
            db = open_database(Path(folder))
            try:
                window = PendingWindow("20060101000000", "20081231235959", strategy="resume")
                plan = IndexPlan([window], 0, 1)
                state = []
                record = lambda payload, completed, seen, error: state.append(payload)
                def consider(error):
                    return _maybe_subdivide_persistent_gateway_504(
                        db, plan, window, error, record, 0, None, None, 0, 1)
                self.assertFalse(consider(gateway()))
                self.assertEqual(window.gateway_504_failures, 1)
                self.assertFalse(consider(TransientRequestError("HTTP 503", status=503)))
                self.assertEqual(window.gateway_504_failures, 0)
                self.assertFalse(consider(gateway()))
                self.assertTrue(consider(gateway()))
                self.assertEqual(len(plan.pending), 2)
                self.assertEqual(plan.completed, 0)
                self.assertEqual(plan.gateway_splits_without_response, 1)
                roundtrip = decode_plan(encode_plan(plan), [])
                self.assertEqual(roundtrip.gateway_splits_without_response, 1)
                self.assertEqual(len(roundtrip.pending), 2)
                self.assertEqual(roundtrip.planned, 2)
                self.assertEqual(len(state), 1)
            finally:
                db.close()

    def test_mixed_503_and_504_does_not_subdivide(self):
        with tempfile.TemporaryDirectory() as folder:
            db = open_database(Path(folder))
            try:
                window = PendingWindow("20060101000000", "20081231235959", strategy="resume")
                plan = IndexPlan([window], 0, 1)
                mixed = TransientRequestError("all CDX endpoints failed: HTTP 504; HTTP 503",
                                              status=504, splittable=True)
                for _ in range(5):
                    self.assertFalse(_maybe_subdivide_persistent_gateway_504(
                        db, plan, window, mixed, lambda *a: None, 0, None, None, 0, 1))
                self.assertEqual(len(plan.pending), 1)
            finally:
                db.close()

    def test_persisted_budget_prevents_unbounded_outage_fragmentation(self):
        with tempfile.TemporaryDirectory() as folder:
            db = open_database(Path(folder))
            try:
                window = PendingWindow("20060101000000", "20081231235959", strategy="resume")
                plan = IndexPlan([window], 0, 1, GATEWAY_504_MAX_SPLITS_WITHOUT_RESPONSE)
                for _ in range(10):
                    self.assertFalse(_maybe_subdivide_persistent_gateway_504(
                        db, plan, window, gateway(), lambda *a: None, 0, None, None, 0, 1))
                self.assertEqual(len(plan.pending), 1)
                self.assertEqual(plan.gateway_splits_without_response, GATEWAY_504_MAX_SPLITS_WITHOUT_RESPONSE)
                self.assertEqual(decode_plan(encode_plan(plan), []).gateway_splits_without_response,
                                 GATEWAY_504_MAX_SPLITS_WITHOUT_RESPONSE)
            finally:
                db.close()

    def test_failed_checkpoint_write_does_not_advance_plan(self):
        with tempfile.TemporaryDirectory() as folder:
            db = open_database(Path(folder))
            try:
                window = PendingWindow("20060101000000", "20081231235959", strategy="resume")
                window.gateway_504_failures = 1
                plan = IndexPlan([window], 0, 1)
                def fail(*args):
                    raise sqlite3.OperationalError("checkpoint unavailable")
                with self.assertRaisesRegex(sqlite3.OperationalError, "checkpoint unavailable"):
                    _maybe_subdivide_persistent_gateway_504(db, plan, window, gateway(), fail,
                                                            0, None, None, 0, 1)
                self.assertEqual(plan.pending, [window])
                self.assertEqual(plan.planned, 1)
                self.assertEqual(plan.gateway_splits_without_response, 0)
            finally:
                db.close()

    def test_stop_and_resume_after_split_does_not_skip_any_half(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            db = open_database(root)
            try:
                with patch.object(HttpClient, "get_cdx_any", side_effect=[gateway(), gateway(), Stopped()]), \
                     patch("archive_scout.cdx.indexer.transient_backoff", return_value=0):
                    with self.assertRaises(Stopped):
                        index_archive(configuration(root), db, threading.Event())
                state = db.execute("SELECT complete,seen,plan_json FROM index_coverage").fetchone()
                self.assertEqual((state["complete"], state["seen"]), (0, 0))
                saved = decode_plan(state["plan_json"], [])
                self.assertEqual(len(saved.pending), 2)
                self.assertEqual(saved.gateway_splits_without_response, 1)
                with patch.object(HttpClient, "get_cdx_any", return_value=[]) as get:
                    index_archive(configuration(root), db, threading.Event())
                    self.assertEqual(get.call_count, 2)
                self.assertEqual(db.execute("SELECT complete FROM index_coverage").fetchone()[0], 1)
            finally:
                db.close()

    def test_retry_count_survives_stop_before_subdivision(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            db = open_database(root)
            try:
                with patch.object(HttpClient, "get_cdx_any", side_effect=[gateway(), Stopped()]), \
                     patch("archive_scout.cdx.indexer.transient_backoff", return_value=0):
                    with self.assertRaises(Stopped):
                        index_archive(configuration(root), db, threading.Event())
                saved = db.execute("SELECT plan_json,complete FROM index_coverage").fetchone()
                plan = decode_plan(saved["plan_json"], [])
                self.assertEqual(saved["complete"], 0)
                self.assertEqual(len(plan.pending), 1)
                self.assertEqual(plan.pending[0].gateway_504_failures, 1)
                with patch.object(HttpClient, "get_cdx_any", side_effect=[gateway(), Stopped()]), \
                     patch("archive_scout.cdx.indexer.transient_backoff", return_value=0):
                    with self.assertRaises(Stopped):
                        index_archive(configuration(root), db, threading.Event())
                state = db.execute("SELECT plan_json,complete FROM index_coverage").fetchone()
                plan = decode_plan(state["plan_json"], [])
                self.assertEqual(len(plan.pending), 2)
                self.assertEqual(plan.gateway_splits_without_response, 1)
                self.assertEqual(state["complete"], 0)
            finally:
                db.close()

    def test_healthy_cdx_has_no_extra_requests(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            db = open_database(root)
            try:
                with patch.object(HttpClient, "get_cdx_any", return_value=[]) as get:
                    index_archive(configuration(root), db, threading.Event())
                    self.assertEqual(get.call_count, 1)
                row = db.execute("SELECT complete,seen FROM index_coverage").fetchone()
                self.assertEqual((row["complete"], row["seen"]), (1, 0))
            finally:
                db.close()


if __name__ == "__main__":
    unittest.main()
