"""One saved-capture corpus for local work and its completion gates."""
from __future__ import annotations

import sqlite3
from dataclasses import replace

from ..cdx.parameters import index_scopes
from ..config import ProjectConfig


def discovery_scope_config(config: ProjectConfig) -> ProjectConfig:
    """Saved-page sites take precedence over separate direct-media targets."""
    return config if config.targets else replace(config, targets=config.media.normalized().targets)


def scope_rows(config: ProjectConfig) -> list[tuple[str, str, str, str]]:
    return sorted((target, signature, scoped.from_date, scoped.to_date)
                  for target, scoped, signature in index_scopes(config))


def prepare_saved_scope(database: sqlite3.Connection, config: ProjectConfig) -> None:
    database.execute("""CREATE TEMP TABLE IF NOT EXISTS archive_scout_saved_scope(
        pattern TEXT NOT NULL,signature TEXT NOT NULL,start TEXT NOT NULL,end TEXT NOT NULL,
        PRIMARY KEY(pattern,signature,start,end)) WITHOUT ROWID""")
    database.execute("DELETE FROM archive_scout_saved_scope")
    database.executemany("INSERT OR IGNORE INTO archive_scout_saved_scope VALUES(?,?,?,?)", scope_rows(config))


def scope_membership_predicate(table: str, alias: str = "c") -> str:
    if alias not in {"c", "captures"} or table not in {
        "archive_scout_saved_scope", "archive_scout_download_scopes", "archive_scout_retry_scopes",
    }:
        raise ValueError("unsupported capture scope")
    return f"""(EXISTS(SELECT 1 FROM targets t JOIN {table} s
        ON s.pattern=t.pattern AND s.signature={alias}.query_signature AND {alias}.timestamp BETWEEN s.start AND s.end
        WHERE t.id={alias}.target_id)
        OR EXISTS(SELECT 1 FROM capture_target_membership m JOIN targets t ON t.id=m.target_id
            JOIN {table} s ON s.pattern=t.pattern AND s.signature={alias}.query_signature
              AND {alias}.timestamp BETWEEN s.start AND s.end
            WHERE m.capture_id={alias}.id))"""


def saved_scope_predicate(alias: str = "c") -> str:
    return scope_membership_predicate("archive_scout_saved_scope", alias)
