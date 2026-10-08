from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

from archive_scout.cdx.client import RateLimitDeferred
from archive_scout.cdx.parameters import cdx_query_signatures
from archive_scout.classification import capture_body_coverage, capture_routing_decision
from archive_scout.config import NetworkConfig, ProjectConfig, ResearchConfig, REPORT_FIELD_NAMES
from archive_scout.constants import SCHEMA_VERSION, VERSION
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, start_operation_run, upsert_capture, upsert_document
from archive_scout.document_store import document_body
from archive_scout.operations import run_project
from archive_scout.projects.importers import import_text_folder
from archive_scout.projects.merge import merge_projects
from archive_scout.research.index import _research_config_fingerprint
from archive_scout.ui.dashboard import read_classification_rows, read_dashboard_counts
from archive_scout.utils import hash_text, normalize_search


class V104FundamentalsTests(unittest.TestCase):
    def test_release_identity_workflow_and_schema(self):
        self.assertEqual(VERSION, "1.1.0")
        self.assertEqual(SCHEMA_VERSION, 13)
        root = Path(__file__).resolve().parents[2]
        workflow = (root / ".github" / "workflows" / "tests.yml").read_text(encoding="utf-8")
        self.assertIn("importlib.metadata.version('archive-scout') == '1.1.0'", workflow)
        self.assertIn("macos-15-intel", workflow)
        self.assertIn("workflow_dispatch", workflow)

    def test_classification_visibility_helpers_keep_class_route_and_body_separate(self):
        self.assertEqual(
            capture_routing_decision("image", "skipped", "deferred_to_media", "not_acquired"),
            "deferred_to_media",
        )
        self.assertEqual(capture_body_coverage("image", "skipped", "not_acquired"), "non_text")
        self.assertEqual(
            capture_routing_decision("text", "downloaded_unscanned", None, "retained_unscanned"),
            "downloaded_awaiting_scan",
        )
        self.assertEqual(capture_body_coverage("text", "downloaded_unscanned", "retained_unscanned"), "body_available")
        self.assertEqual(capture_body_coverage("text", "downloaded", "discarded"), "discarded")
        for field in ("resource_class", "classification_reason", "routing_decision", "body_coverage", "skip_reason"):
            self.assertIn(field, REPORT_FIELD_NAMES["all_indexed_urls"])
        self.assertIn("bodies_searched", REPORT_FIELD_NAMES["summary"])

    def test_dashboard_reconciles_operation_classes_routes_and_body_coverage(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(root, ["example.com/*"], [], from_date="2001", to_date="2001").normalized()
            db = open_database(root)
            target_id = get_or_create_target(db, cfg.targets[0])
            signature = cdx_query_signatures(cfg)[0]
            fixtures = [
                ("text", "downloaded_unscanned", None, "retained_unscanned", "metadata:text"),
                ("image", "skipped", "deferred_to_media", "not_acquired", "mime:image/jpeg"),
                ("video", "skipped", "deferred_to_media", "not_acquired", "mime:video/mp4"),
                ("audio", "skipped", "known_non_text", "not_acquired", "mime:audio/mpeg"),
                ("media_descriptor", "skipped", "classified_media_descriptor", "not_acquired", "extension:m3u"),
                ("other_binary", "skipped", "unsupported_binary", "not_acquired", "signature:zip"),
                ("unknown", "error", None, "not_acquired", "conflicting metadata"),
            ]
            for index, (resource_class, state, skip_reason, availability, evidence) in enumerate(fixtures):
                upsert_capture(
                    db,
                    {
                        "original": f"http://example.com/{index}",
                        "timestamp": f"200101010000{index:02d}",
                        "mimetype": "application/octet-stream",
                        "statuscode": "200",
                        "digest": f"d{index}",
                        "length": "10",
                    },
                    target_id,
                    signature,
                )
                db.execute(
                    """UPDATE captures SET resource_class=?,state=?,skip_reason=?,payload_availability=?,classification_reason=?
                       WHERE original_url=?""",
                    (resource_class, state, skip_reason, availability, evidence, f"http://example.com/{index}"),
                )
            start_operation_run(db, "download_only", VERSION, config_json=json.dumps(cfg.to_payload()))
            db.commit()
            db.close()

            counts = read_dashboard_counts(root / "archive_scout.sqlite3", include_operation_scope=True)
            self.assertEqual(counts["operation_scope"], "frozen_config")
            self.assertEqual(counts["operation_total"], 7)
            self.assertEqual(counts["operation_saved_unscanned"], 1)
            self.assertEqual(counts["operation_skipped"], 5)
            self.assertEqual(counts["operation_failed"], 1)
            for key in (
                "operation_class_text", "operation_class_image", "operation_class_video", "operation_class_audio",
                "operation_class_media_descriptor", "operation_class_other_binary", "operation_class_unknown",
            ):
                self.assertEqual(counts[key], 1, key)
            self.assertEqual(counts["operation_deferred_media"], 2)
            self.assertEqual(counts["operation_skipped_non_text"], 3)
            self.assertEqual(counts["operation_body_available"], 1)
            self.assertEqual(counts["operation_body_non_text"], 5)
            self.assertEqual(counts["operation_body_url_only"], 1)

            rows = read_classification_rows(root / "archive_scout.sqlite3", resource_class="image")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["routing_decision"], "deferred_to_media")
            self.assertEqual(rows[0]["classification_reason"], "mime:image/jpeg")
            self.assertEqual(rows[0]["skip_reason"], "deferred_to_media")

    def test_download_only_automatically_recovers_same_operation_after_service_pause(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            cfg = ProjectConfig(
                root,
                ["example.com/*"],
                [],
                from_date="2001",
                to_date="2001",
                network=NetworkConfig(persistent_retries=True),
                research=ResearchConfig(enabled=False, auto_build=False),
            ).normalized()
            result = {"queued": 0, "downloaded": 0, "skipped": 0, "errors": 0}
            pause = RateLimitDeferred(
                "temporary service throttle",
                status=429,
                eligible_at_epoch=time.time() - 0.01,
            )
            with mock.patch("archive_scout.operations.index_archive"), mock.patch(
                "archive_scout.operations.download_archive_only", side_effect=[pause, result]
            ) as acquisition:
                run_project(cfg, "download_only", threading.Event())
            self.assertEqual(acquisition.call_count, 2)
            db = open_database(root)
            operations = db.execute("SELECT id,status FROM operation_runs ORDER BY id").fetchall()
            self.assertEqual(len(operations), 1)
            self.assertEqual(operations[0]["status"], "complete")
            db.close()

    def test_import_ingests_utf16_into_project_with_valid_local_timestamp(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            root = base / "project"
            source = base / "incoming"
            source.mkdir()
            original = source / "page.txt"
            original.write_bytes("needle from utf16".encode("utf-16"))
            db = open_database(root)
            imported = import_text_folder(root, source, db, threading.Event())
            self.assertEqual(imported, 1)
            row = db.execute(
                """SELECT c.timestamp,c.local_path,c.payload_availability,c.payload_origin,d.*
                   FROM captures c JOIN documents d ON d.capture_id=c.id"""
            ).fetchone()
            self.assertEqual(len(str(row["timestamp"])), 14)
            self.assertTrue(str(row["timestamp"]).isdigit())
            local = Path(str(row["local_path"]))
            self.assertTrue(local.is_relative_to(root.resolve()))
            self.assertTrue(local.is_file())
            self.assertEqual(row["payload_availability"], "retained")
            self.assertEqual(row["payload_origin"], "local_import")
            self.assertIn("needle from utf16", document_body(row))
            db.close()

    def test_merge_preserves_download_only_payload_and_does_not_fabricate_discarded_file(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            source_root = base / "source"
            destination_root = base / "destination"
            source = open_database(source_root)
            target = get_or_create_target(source, "example.com/*")
            for index in range(2):
                upsert_capture(
                    source,
                    {
                        "original": f"http://example.com/{index}", "timestamp": f"2001010100000{index}",
                        "mimetype": "text/html", "statuscode": "200", "digest": f"x{index}", "length": "5",
                    },
                    target,
                    "sig",
                )
            retained = source_root / "captures" / "download-only.txt"
            retained.parent.mkdir(parents=True, exist_ok=True)
            retained.write_text("saved", encoding="utf-8")
            source.execute(
                """UPDATE captures SET state='downloaded_unscanned',local_path=?,bytes_saved=5,
                   payload_availability='retained_unscanned',payload_origin='replay',resource_class='text'
                   WHERE original_url='http://example.com/0'""",
                (str(retained),),
            )
            discarded_id = int(source.execute(
                "SELECT id FROM captures WHERE original_url='http://example.com/1'"
            ).fetchone()[0])
            unavailable_path = source_root / "captures" / "discarded-source.txt"
            upsert_document(
                source, discarded_id, unavailable_path, "Discarded", "historical evidence", [],
                hash_text("historical evidence"), hash_text(normalize_search("historical evidence")), 19,
            )
            source.execute(
                """UPDATE captures SET state='downloaded',local_path=NULL,payload_availability='discarded',
                   payload_origin='replay',resource_class='text',discarded_at='2026-01-01T00:00:00+00:00'
                   WHERE id=?""",
                (discarded_id,),
            )
            source.commit()
            source.close()

            destination = open_database(destination_root)
            merge_projects(destination_root, source_root, destination)
            kept = destination.execute("SELECT * FROM captures WHERE original_url='http://example.com/0'").fetchone()
            discarded = destination.execute("SELECT * FROM captures WHERE original_url='http://example.com/1'").fetchone()
            kept_path = Path(str(kept["local_path"]))
            self.assertTrue(kept_path.is_file())
            self.assertEqual(kept_path.read_text(encoding="utf-8"), "saved")
            self.assertEqual(kept["payload_availability"], "retained_unscanned")
            self.assertEqual(discarded["payload_availability"], "discarded")
            self.assertIsNone(discarded["local_path"])
            discarded_document = destination.execute(
                "SELECT * FROM documents WHERE capture_id=?", (int(discarded["id"]),)
            ).fetchone()
            self.assertIsNotNone(discarded_document)
            self.assertFalse(Path(str(discarded_document["path"])).exists())
            self.assertIn("historical evidence", document_body(discarded_document))
            destination.close()

    def test_research_cache_fingerprint_changes_with_derived_settings(self):
        base = ResearchConfig(vector_dimensions=64, excerpt_chars=2000, entity_extraction=True).normalized()
        changed_dimensions = ResearchConfig(vector_dimensions=256, excerpt_chars=2000, entity_extraction=True).normalized()
        changed_entities = ResearchConfig(vector_dimensions=64, excerpt_chars=2000, entity_extraction=False).normalized()
        self.assertNotEqual(_research_config_fingerprint(base), _research_config_fingerprint(changed_dimensions))
        self.assertNotEqual(_research_config_fingerprint(base), _research_config_fingerprint(changed_entities))


if __name__ == "__main__":
    unittest.main()
