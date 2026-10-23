"""Cheap, positive inventory admission checks for acquisition phases.

Each phase checks committed evidence once, not once per HTTP request. Missing
checkpoint rows are not equivalent to completed empty inventories.
"""
from __future__ import annotations

import sqlite3
from .cdx.parameters import cdx_query_signature
from .cdx.indexer import uncovered_index_ranges
from .media.indexer import media_index_state_signature, ALL_EXTENSIONS_STATE
from .config import ProjectConfig


class IndexCoveragePending(RuntimeError):
    """Unfinished inventory must not permit any new replay requests."""


def text_coverage_gaps(database: sqlite3.Connection, config: ProjectConfig) -> list[str]:
    gaps: list[str] = []
    for target in config.targets:
        scoped = config.for_target(target)
        signature = cdx_query_signature(scoped)
        found = database.execute("SELECT id FROM targets WHERE pattern=?", (target,)).fetchone()
        if found is None:
            gaps.append(f"{target}: no index checkpoint")
            continue
        target_id = int(found[0])
        if scoped.text_collapse_scope == "year":
            for year in range(scoped.from_year, scoped.to_year + 1):
                start = max(scoped.from_date, f"{year:04d}0101000000")
                end = min(scoped.to_date, f"{year:04d}1231235959")
                if start > end:
                    continue
                row = database.execute(
                    "SELECT 1 FROM index_state WHERE target_id=? AND year=? AND query_signature=? AND complete=1",
                    (target_id, year, signature),
                ).fetchone()
                if not row:
                    gaps.append(f"{target}: {year} not proven complete")
        else:
            if uncovered_index_ranges(database, target_id, signature, scoped.from_date, scoped.to_date):
                gaps.append(f"{target}: date range not proven complete")
    return gaps


def media_coverage_gaps(database: sqlite3.Connection, config: ProjectConfig) -> list[str]:
    media = config.media.normalized()
    signature = media_index_state_signature(config)
    gaps: list[str] = []
    for target in media.targets or config.targets:
        scoped = config.for_target(target) if target in config.targets else config
        years = ([0] if scoped.text_collapse_scope == "range" and scoped.from_year != scoped.to_year
                 else list(range(scoped.from_year, scoped.to_year + 1)))
        row = database.execute("SELECT id FROM media_targets WHERE pattern=?", (target,)).fetchone()
        if row is None:
            gaps.append(f"{target}: no media inventory")
            continue
        for year in years:
            done = database.execute(
                "SELECT 1 FROM media_index_state WHERE target_id=? AND extension=? "
                "AND year=? AND query_signature=? AND complete=1",
                (int(row[0]), ALL_EXTENSIONS_STATE, year, signature),
            ).fetchone()
            if not done:
                gaps.append(f"{target}: media inventory {year or 'range'} incomplete")
    return gaps


def require_text_coverage(database: sqlite3.Connection, config: ProjectConfig) -> None:
    gaps = text_coverage_gaps(database, config)
    if gaps:
        raise IndexCoveragePending("Indexing incomplete; acquisition not admitted: " + "; ".join(gaps[:4]))


def require_media_coverage(database: sqlite3.Connection, config: ProjectConfig) -> None:
    gaps = media_coverage_gaps(database, config)
    if gaps:
        raise IndexCoveragePending("Media inventory incomplete; acquisition not admitted: " + "; ".join(gaps[:4]))


def require_embedded_coverage(database: sqlite3.Connection, config: ProjectConfig,
                              signature: str) -> None:
    """Every saved document and lookup must be accounted for before media I/O."""
    if not config.media.discover_embedded:
        return
    if database.execute("""SELECT 1 FROM documents d
        LEFT JOIN media_discovery_documents m ON m.document_id=d.id AND m.query_signature=?
        WHERE m.document_id IS NULL OR m.content_hash IS NOT d.content_hash LIMIT 1""",
        (signature,)).fetchone():
        raise IndexCoveragePending("Embedded media discovery has unprocessed or changed saved documents")
    if database.execute("""SELECT 1 FROM media_discovery_queue WHERE query_signature=?
        AND (state IN ('pending','error','resolving','blocked')
             OR (state='unavailable' AND COALESCE(last_error,'')!='')) LIMIT 1""",
        (signature,)).fetchone():
        raise IndexCoveragePending("Embedded media lookup obligations remain unresolved")


def record_admission(db: sqlite3.Connection, operation_run_id: int,
                     phase: str, config: ProjectConfig) -> None:
    """Record a positive, frozen scope fingerprint once per admitted phase."""
    import hashlib
    import json
    from .media.indexer import media_query_signature
    semantic = {
        "targets": [(target, cdx_query_signature(config.for_target(target))) for target in config.targets],
        "media": media_query_signature(config) if phase in ("media", "embedded") else None,
        "retention": getattr(config, "keep_downloaded_text", None),
        "phase": phase,
    }
    digest = hashlib.sha256(json.dumps(semantic, sort_keys=True, default=str).encode()).hexdigest()
    with db:
        db.execute("""INSERT OR REPLACE INTO phase_admissions
                 (operation_run_id,phase,scope_digest,admitted_at)
                 VALUES (?,?,?,datetime('now'))""", (operation_run_id, phase, digest))
