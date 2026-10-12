from __future__ import annotations

import sqlite3
from collections import Counter

from ..classification import classify_indexed_resource
from ..utils import utc_now

KINDS = ("text", "image", "video", "audio", "media_descriptor", "other_binary", "unknown")

def classify_indexed_captures(database: sqlite3.Connection, signature: str, stop_event=None, batch_size: int = 5000) -> int:
    """Backfill only missing classifications; indexing can be slow, not acquisition.

    Read by bounded keyset (not OFFSET), and write in batches. Unchanged indexed
    rows are not needlessly reclassified when another operation resumes.
    """
    cursor = 0
    updated = 0
    while True:
        if stop_event is not None and stop_event.is_set():
            from ..events import Stopped
            raise Stopped
        rows = database.execute(
            """SELECT c.id,c.original_url,c.mimetype FROM captures c
               LEFT JOIN capture_routing r ON r.capture_id=c.id
               WHERE c.query_signature=? AND c.id>? AND r.capture_id IS NULL
               ORDER BY c.id LIMIT ?""",
            (signature,cursor,max(1,batch_size)),
        ).fetchall()
        if not rows:
            return updated
        now=utc_now()
        values=[]
        for row in rows:
            cursor=int(row["id"])
            decision=classify_indexed_resource(str(row["original_url"]),str(row["mimetype"] or ""))
            values.append((cursor,decision.resource_class,decision.reason,int(decision.confident),"pending",now))
        with database:
            database.executemany(
                """INSERT OR IGNORE INTO capture_routing
                   (capture_id,resource_class,evidence,confident,routing,updated_at)
                   VALUES (?,?,?,?,?,?)""",values)
        updated+=len(values)

def classification_counts(database: sqlite3.Connection, signature: str|None=None) -> dict[str,int]:
    """One grouped read on demand, never on the capture-saving hot path."""
    if signature:
        rows=database.execute(
            """SELECT COALESCE(r.resource_class,'unknown'),COALESCE(r.routing,'pending'),COUNT(*)
               FROM captures c LEFT JOIN capture_routing r ON r.capture_id=c.id
               WHERE c.query_signature=? GROUP BY 1,2""",(signature,),
        ).fetchall()
    else:
        rows=database.execute(
            """SELECT COALESCE(r.resource_class,'unknown'),COALESCE(r.routing,'pending'),COUNT(*)
               FROM captures c LEFT JOIN capture_routing r ON r.capture_id=c.id
               GROUP BY 1,2"""
        ).fetchall()
    totals={kind:0 for kind in KINDS}
    dispositions=Counter()
    for kind,routing,count in rows:
        totals[str(kind) if str(kind) in totals else 'unknown']+=int(count)
        dispositions[str(routing)]+=int(count)
    totals.update({"route_"+key:value for key,value in dispositions.items()})
    return totals
