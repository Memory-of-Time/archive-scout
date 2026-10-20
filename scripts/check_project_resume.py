"""Read-only safety snapshot and verification for Archive Scout project upgrades.

Use while Archive Scout is stopped:
  python scripts/check_project_resume.py PATH --snapshot before.json
  python scripts/check_project_resume.py PATH --compare before.json

Never creates a missing SQLite database or modifies a project. Supports
v1.2.5 schema-10 projects, v1.2.4 schema-9 projects, and earlier supported versions.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

COUNT_TABLES = (
    "captures", "documents", "media_captures", "index_state", "index_coverage",
    "index_pages", "media_index_state", "scan_runs", "document_matches",
    "keyword_hits", "errors", "quick_search_runs", "quick_search_hits",
)


def _tables(db: sqlite3.Connection) -> set[str]:
    return {str(row[0]) for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _fingerprint_sample(db: sqlite3.Connection, maximum: int) -> dict[str, str]:
    """Sample stable immutable capture identity with only O(log N) PK lookups."""
    samples = {}
    if maximum < 1:
        return samples
    for candidate in sorted({1, maximum, *(max(1, maximum * i // 12) for i in range(1, 12))}):
        row = db.execute(
            "SELECT id,original_url,timestamp FROM captures WHERE id>=? ORDER BY id LIMIT 1",
            (candidate,),
        ).fetchone()
        if row is not None:
            key = str(row[0])
            payload = json.dumps([int(row[0]), str(row[1]), str(row[2])], separators=(",", ":"))
            samples[key] = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return samples


def snapshot(project_dir: Path, sample_ids: list[int] | None = None) -> dict:
    root = Path(project_dir).expanduser().resolve()
    path = root / "archive_scout.sqlite3"
    if not path.is_file():
        raise FileNotFoundError(f"No existing project database: {path}; refusing to create an empty project")
    # Read-only URI recognizes committed WAL pages without mutating the DB.
    db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=15)
    try:
        db.execute("PRAGMA query_only=ON")
        with db:  # One consistent read transaction, no writes.
            db.execute("BEGIN")
            tables = _tables(db)
            if "schema_info" not in tables or "captures" not in tables:
                raise RuntimeError("No recognized Archive Scout schema/captures tables; project not modified")
            version_row = db.execute("SELECT version FROM schema_info LIMIT 1").fetchone()
            if version_row is None:
                raise RuntimeError("Missing Archive Scout schema version; project not modified")
            max_id = int(db.execute("SELECT COALESCE(MAX(id),0) FROM captures").fetchone()[0])
            counts = {table: int(db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                      for table in COUNT_TABLES if table in tables}
            capture_states = {str(state): int(count) for state, count in db.execute(
                "SELECT state,COUNT(*) FROM captures GROUP BY state")}
            media_states = ({str(state): int(count) for state, count in db.execute(
                "SELECT state,COUNT(*) FROM media_captures GROUP BY state")}
                if "media_captures" in tables else {})
            signatures = {str(signature): int(count) for signature, count in db.execute(
                "SELECT query_signature,COUNT(*) FROM captures GROUP BY query_signature")}
            sampled = _fingerprint_sample(db, max_id)
            if sample_ids is not None:
                sampled = {}
                for capture_id in sample_ids:
                    row = db.execute("SELECT id,original_url,timestamp FROM captures WHERE id=?", (capture_id,)).fetchone()
                    if row is not None:
                        payload = json.dumps([int(row[0]), str(row[1]), str(row[2])], separators=(",", ":"))
                        sampled[str(capture_id)] = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        return {
            "format": 1, "schema_version": int(version_row[0]),
            "max_capture_id": max_id, "counts": counts,
            "capture_states": capture_states, "media_states": media_states,
            "capture_query_signatures": signatures,
            "capture_identity_samples": sampled,
        }
    finally:
        db.close()


def compare(before: dict, after: dict) -> list[str]:
    problems: list[str] = []
    if int(before.get("schema_version", -1)) > int(after.get("schema_version", -1)):
        problems.append("database schema version decreased")
    for table, old_count in before.get("counts", {}).items():
        current = after.get("counts", {}).get(table, 0)
        if current < old_count:
            problems.append(f"{table}: {current:,} records now, expected at least {old_count:,}")
    if after.get("max_capture_id", 0) < before.get("max_capture_id", 0):
        problems.append("maximum capture ID decreased")
    for key, fingerprint in before.get("capture_identity_samples", {}).items():
        if after.get("capture_identity_samples", {}).get(key) != fingerprint:
            # Check the original ID explicitly below in case the sample grid shifted
            # when more captures were added after the upgrade.
            problems.append(f"capture identity sample at ID {key} changed or cannot be verified")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only Archive Scout 1.2.0 -> 1.2.1 project inventory safety check")
    parser.add_argument("project", type=Path, help="Existing Archive Scout project folder (contains project.json and archive_scout.sqlite3)")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--snapshot", type=Path, help="Save before-upgrade counts/checkpoints evidence to JSON")
    group.add_argument("--compare", type=Path, help="Compare after-upgrade state to a saved snapshot")
    args = parser.parse_args(argv)
    try:
        before = json.loads(args.compare.read_text(encoding="utf-8")) if args.compare else None
        sample_ids = [int(x) for x in before.get("capture_identity_samples", {})] if before else None
        current = snapshot(args.project, sample_ids=sample_ids)
        if args.snapshot:
            output = args.snapshot.expanduser().resolve()
            if output.exists():
                raise FileExistsError(f"Snapshot exists; refusing to overwrite: {output}")
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(current, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            print(f"Read-only baseline saved: {output}")
            print(f"Captures: {current['counts']['captures']:,}; schema {current['schema_version']}")
            return 0
        errors = compare(before, current)
        if errors:
            print("INTEGRITY COMPARISON FAILED:", file=sys.stderr)
            for item in errors:
                print(" - " + item, file=sys.stderr)
            return 2
        print("PASS: historical project inventory counts and identity samples preserved")
        print(f"Captures: {before['counts']['captures']:,} before -> {current['counts']['captures']:,} now")
        print("Note: this verifies durable inventory, not live Wayback download availability.")
        return 0
    except (OSError, sqlite3.Error, ValueError, KeyError, RuntimeError) as exc:
        print(f"Project check failed safely (nothing changed): {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
