from __future__ import annotations

import json
import os
import platform
import sqlite3
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Callable

from ..constants import VERSION
from ..events import ProgressEvent
from ..utils import utc_now


def _rows(database: sqlite3.Connection, query: str, params: tuple = ()) -> list[dict]:
    return [dict(row) for row in database.execute(query, params).fetchall()]


def _project_summary(project_path: Path) -> dict:
    if not project_path.exists():
        return {"available": False}
    try:
        project = json.loads(project_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"available": True, "readable": False, "error_type": type(exc).__name__}
    keyword_sets = project.get("keyword_sets") or []
    selected_sets = sum(1 for item in keyword_sets if isinstance(item, dict) and item.get("selected", True))
    media = project.get("media") if isinstance(project.get("media"), dict) else {}
    network = project.get("network") if isinstance(project.get("network"), dict) else {}
    ai = project.get("ai") if isinstance(project.get("ai"), dict) else {}
    return {
        "available": True,
        "readable": True,
        "version": project.get("version"),
        "mode": project.get("mode"),
        "target_count": len(project.get("targets") or []),
        "keyword_count": len(project.get("keywords") or []),
        "keyword_set_count": len(keyword_sets),
        "selected_keyword_set_count": selected_sets,
        "media_enabled": bool(media.get("enabled")),
        "network_backend": network.get("backend"),
        "network_endpoint_mode": network.get("endpoint_mode"),
        "network_index_strategy": network.get("index_strategy"),
        "ai_provider": ai.get("provider"),
        "ai_model": ai.get("model"),
        "auto_backup": bool(project.get("auto_backup")),
    }


def export_diagnostics(
    root: Path,
    database: sqlite3.Connection,
    callback: Callable[[ProgressEvent], None] | None = None,
) -> Path:
    root = Path(root)
    output = root / "reports" / "diagnostics.zip"
    output.parent.mkdir(parents=True, exist_ok=True)
    system = {
        "generated_at": utc_now(),
        "archive_scout_version": VERSION,
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "operating_system": platform.system(),
        "operating_system_release": platform.release(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "frozen": bool(getattr(sys, "frozen", False)),
    }
    counts = {}
    for table in (
        "targets", "captures", "documents", "scan_runs", "document_matches", "errors",
        "media_captures", "forum_threads", "forum_posts", "extractions", "network_events",
        "ai_runs", "ai_results", "site_issues", "media_discovery_queue",
    ):
        try:
            counts[table] = int(database.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        except Exception:
            counts[table] = None
    payloads = {
        "system.json": system,
        "counts.json": counts,
        "project-summary.json": _project_summary(root / "project.json"),
        "error-summary.json": _rows(
            database,
            """
            SELECT operation,category,retryable,resolved,ignored,COUNT(*) AS count,MAX(last_seen) AS latest
            FROM errors
            GROUP BY operation,category,retryable,resolved,ignored
            ORDER BY operation,category,retryable,resolved,ignored
            """,
        ),
        "site-issue-summary.json": _rows(
            database,
            """
            SELECT host,stage,category,http_status,resolved,occurrence_count,first_seen,last_seen
            FROM site_issues
            ORDER BY resolved,last_seen DESC,host
            LIMIT 1000
            """,
        ),
        "network-summary.json": _rows(
            database,
            """
            SELECT stage,backend,status,COUNT(*) AS count,MAX(created_at) AS latest
            FROM network_events
            GROUP BY stage,backend,status
            ORDER BY stage,backend,status
            """,
        ),
        "operation-summary.json": _rows(
            database,
            """
            SELECT mode,status,app_version,COUNT(*) AS count,MAX(updated_at) AS latest
            FROM operation_runs
            GROUP BY mode,status,app_version
            ORDER BY mode,status,app_version
            """,
        ),
        "report-presence.json": {
            name: (root / "reports" / name).exists()
            for name in ("integrity.txt", "repair.txt")
        },
        "sqlite-integrity.json": {"result": database.execute("PRAGMA integrity_check").fetchone()[0]},
    }
    with tempfile.TemporaryDirectory(prefix="archive-scout-diagnostics-", dir=str(output.parent)) as temp:
        temporary_output = Path(temp) / output.name
        with zipfile.ZipFile(temporary_output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, payload in payloads.items():
                archive.writestr(name, json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n")
        os.replace(temporary_output, output)
    if callback:
        callback(ProgressEvent("diagnostics", f"Diagnostics written to {output}"))
    return output
