from __future__ import annotations

import sqlite3
import hashlib
from pathlib import Path
from typing import Callable

from ..events import ProgressEvent, Stopped
from ..utils import atomic_write_text, atomic_write_lines, utc_now
from itertools import chain


class _DiskReferences:
    def __init__(self,database,kind):
        self.database,self.kind=database,kind
        database.execute('CREATE TEMP TABLE IF NOT EXISTS scout_integrity_paths(kind TEXT,path TEXT,PRIMARY KEY(kind,path)) WITHOUT ROWID')
        database.execute('DELETE FROM scout_integrity_paths WHERE kind=?',(kind,))
    def add(self,path):
        self.database.execute('INSERT OR IGNORE INTO scout_integrity_paths VALUES(?,?)',(self.kind,str(path)))
    def __contains__(self,path):
        return bool(self.database.execute('SELECT 1 FROM scout_integrity_paths WHERE kind=? AND path=?',(self.kind,str(path))).fetchone())


class _DiskIssues:
    def __init__(self,database):
        self.database=database
        database.execute('CREATE TEMP TABLE IF NOT EXISTS scout_integrity_issues(id INTEGER PRIMARY KEY,message TEXT)')
        database.execute('DELETE FROM scout_integrity_issues')
        self.count=0
    def append(self,message):
        self.database.execute('INSERT INTO scout_integrity_issues(message) VALUES(?)',(message,))
        self.count += 1
    def __len__(self):
        return self.count
    def __iter__(self):
        for row in self.database.execute('SELECT message FROM scout_integrity_issues ORDER BY id'):
            yield row[0]


def check_project_integrity(
    root: Path,
    database: sqlite3.Connection,
    callback: Callable[[ProgressEvent], None] | None = None,
    *, verify_hashes: bool = True, stop_event=None,
) -> Path:
    issues = _DiskIssues(database)
    referenced = _DiskReferences(database,"text")
    total = int(database.execute("SELECT COUNT(*) FROM documents").fetchone()[0])
    documents = database.execute(
        """
        SELECT d.id,d.path,d.size_bytes,d.content_hash,c.id AS capture_id,c.original_url,c.state
        FROM documents d JOIN captures c ON c.id=d.capture_id ORDER BY d.id
        """
    )
    def sha256_file(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024*1024), b""):
                if stop_event is not None and stop_event.is_set(): raise Stopped
                digest.update(block)
        return digest.hexdigest()

    for index, row in enumerate(documents, 1):
        if stop_event is not None and stop_event.is_set(): raise Stopped
        path = Path(row["path"])
        referenced.add(path.resolve())
        if not path.exists():
            issues.append(f"MISSING_FILE\tdocument={row['id']}\tcapture={row['capture_id']}\t{row['original_url']}\t{path}")
        elif not path.is_file():
            issues.append(f"NOT_A_FILE\tdocument={row['id']}\t{path}")
        elif path.stat().st_size == 0:
            issues.append(f"EMPTY_FILE\tdocument={row['id']}\t{row['original_url']}\t{path}")
        elif int(row["size_bytes"] or 0) and path.stat().st_size != int(row["size_bytes"]):
            issues.append(
                f"SIZE_MISMATCH\tdocument={row['id']}\tdatabase={row['size_bytes']}\tdisk={path.stat().st_size}\t{path}"
            )
        elif verify_hashes and row["content_hash"] and sha256_file(path) != str(row["content_hash"]):
            issues.append(f"HASH_MISMATCH\tdocument={row['id']}\t{path}")
        if callback and (index % 100 == 0 or index == total):
            callback(ProgressEvent("integrity", f"Checked {index:,}/{total:,} saved documents", index, total))
    bad_captures = database.execute(
        """
        SELECT id,original_url,state,document_id FROM captures
        WHERE (state='downloaded' AND document_id IS NULL)
           OR (document_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM documents d WHERE d.id=captures.document_id))
        ORDER BY id
        """
    )
    for row in bad_captures:
        if stop_event is not None and stop_event.is_set(): raise Stopped
        issues.append(
            f"BROKEN_DATABASE_LINK\tcapture={row['id']}\tstate={row['state']}\tdocument={row['document_id']}\t{row['original_url']}"
        )
    # Run media checks only when Integrity is explicitly selected. We never
    # hash media during normal acquisition, indexing, or scanning.
    media_total = int(database.execute("SELECT COUNT(*) FROM media_captures WHERE path IS NOT NULL AND path<>''").fetchone()[0])
    media_referenced = _DiskReferences(database,"media")
    rows = database.execute("SELECT id,path,bytes_saved,content_hash FROM media_captures WHERE path IS NOT NULL AND path<>'' ORDER BY id")
    for row in rows:
        if stop_event is not None and stop_event.is_set(): raise Stopped
        file_path = Path(row["path"])
        media_referenced.add(file_path.resolve())
        if not file_path.is_file():
            issues.append(f"MISSING_MEDIA_FILE\tmedia={row['id']}\t{file_path}")
        elif int(row["bytes_saved"] or 0) and file_path.stat().st_size != int(row["bytes_saved"]):
            issues.append(f"MEDIA_SIZE_MISMATCH\tmedia={row['id']}\t{file_path}")
        elif verify_hashes and row["content_hash"] and sha256_file(file_path) != str(row["content_hash"]):
            issues.append(f"MEDIA_HASH_MISMATCH\tmedia={row['id']}\t{file_path}")

    capture_root = root / "captures"
    if capture_root.exists():
        for path in capture_root.rglob("*.txt"):
            if stop_event is not None and stop_event.is_set(): raise Stopped
            if path.resolve() not in referenced:
                issues.append(f"ORPHAN_FILE\t{path}")
    unresolved = database.execute(
        "SELECT operation,category,COUNT(*) AS count FROM errors WHERE resolved=0 GROUP BY operation,category ORDER BY operation,category"
    ).fetchall()
    lines = [
        "Archive Scout project integrity report",
        f"Generated: {utc_now()}",
        f"Project: {root}",
        f"Documents checked: {total:,}",
        f"Media checked: {media_total:,}",
        f"Content hashes verified: {bool(verify_hashes)}",
        f"Issues found: {len(issues):,}",
        "",
        "UNRESOLVED ERROR COUNTS",
    ]
    lines.extend(f"{row['operation']}\t{row['category']}\t{row['count']}" for row in unresolved)
    lines.extend(["", "INTEGRITY ISSUES"])
    path = root / "reports" / "integrity.txt"
    atomic_write_lines(path,chain(lines,issues or ["None"]))
    return path
