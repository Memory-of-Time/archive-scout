"""One saved-capture corpus for local work and its completion gates."""
from __future__ import annotations

import sqlite3
from dataclasses import replace

from ..cdx.parameters import cdx_query_signature
from ..config import ProjectConfig


def discovery_scope_config(config: ProjectConfig) -> ProjectConfig:
    """Saved-page sites take precedence over separate direct-media targets."""
    return config if config.targets else replace(config, targets=config.media.normalized().targets)


def scope_rows(config: ProjectConfig) -> list[tuple[str, str, str, str]]:
    return sorted((target, cdx_query_signature(scoped), scoped.from_date, scoped.to_date)
                  for target in config.targets for scoped in [config.for_target(target)])


def prepare_saved_scope(database: sqlite3.Connection, config: ProjectConfig) -> None:
    database.execute("""CREATE TEMP TABLE IF NOT EXISTS archive_scout_saved_scope(
        pattern TEXT NOT NULL,signature TEXT NOT NULL,start TEXT NOT NULL,end TEXT NOT NULL,
        PRIMARY KEY(pattern,signature,start,end)) WITHOUT ROWID""")
    database.execute("DELETE FROM archive_scout_saved_scope")
    database.executemany("INSERT OR IGNORE INTO archive_scout_saved_scope VALUES(?,?,?,?)", scope_rows(config))


def saved_scope_predicate(alias: str = "c") -> str:
    # Alias is an internal identifier, never user input.
    if alias not in {"c", "captures"}:
        raise ValueError("unsupported capture alias")
    return f"""EXISTS(SELECT 1 FROM archive_scout_saved_scope s JOIN targets t ON t.pattern=s.pattern
        WHERE s.signature={alias}.query_signature AND {alias}.timestamp BETWEEN s.start AND s.end
        AND (t.id={alias}.target_id OR EXISTS(SELECT 1 FROM capture_target_membership membership
            WHERE membership.capture_id={alias}.id AND membership.target_id=t.id)))"""
