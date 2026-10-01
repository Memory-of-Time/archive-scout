from __future__ import annotations

import sqlite3
from urllib.parse import urlsplit

from ..utils import utc_now


def trace_provenance(database: sqlite3.Connection) -> int:
    created = 0

    def write_group(items: list[sqlite3.Row]) -> int:
        if len(items) < 2:
            return 0
        source = items[0]
        source_host = (urlsplit(str(source["original_url"])).hostname or "").casefold()
        values: list[tuple] = []
        for mirror in items[1:]:
            mirror_host = (urlsplit(str(mirror["original_url"])).hostname or "").casefold()
            if source_host == mirror_host and source["original_url"] == mirror["original_url"]:
                continue
            values.append(
                (
                    source["document_id"],
                    mirror["document_id"],
                    source["method"],
                    float(mirror["similarity"]),
                    source["timestamp"],
                    mirror["timestamp"],
                    utc_now(),
                )
            )
        if values:
            database.executemany(
                """
                INSERT OR IGNORE INTO provenance_edges(
                    source_document_id,mirror_document_id,method,similarity,
                    source_timestamp,mirror_timestamp,created_at
                ) VALUES(?,?,?,?,?,?,?)
                """,
                values,
            )
        return len(values)

    with database:
        database.execute("DELETE FROM provenance_edges")
        current_group: int | None = None
        group_rows: list[sqlite3.Row] = []
        for row in database.execute(
            """
            SELECT dg.id AS group_id,dg.method,dm.document_id,dm.similarity,
                   c.original_url,c.timestamp
            FROM duplicate_groups dg
            JOIN duplicate_members dm ON dm.group_id=dg.id
            JOIN documents d ON d.id=dm.document_id
            JOIN captures c ON c.id=d.capture_id
            ORDER BY dg.id,c.timestamp,dm.document_id
            """
        ):
            group_id = int(row["group_id"])
            if current_group is not None and group_id != current_group:
                created += write_group(group_rows)
                group_rows.clear()
            current_group = group_id
            group_rows.append(row)
        created += write_group(group_rows)
    return created
