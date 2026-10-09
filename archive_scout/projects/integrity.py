from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Callable

from ..events import ProgressEvent, Stopped
from ..utils import atomic_write_lines, utc_now


def check_project_integrity(
    root: Path,
    database: sqlite3.Connection,
    callback: Callable[[ProgressEvent], None] | None = None,
    *, stop_event=None,
) -> Path:
    """Complete SQLite/FTS and manifest inventory with disk-backed membership."""
    root = Path(root)
    report = root / 'reports' / 'integrity.txt'
    report.parent.mkdir(parents=True, exist_ok=True)
    count = total = documents_total = 0
    fd, name = tempfile.mkstemp(prefix='integrity-issues-', suffix='.tmp', dir=report.parent)
    issue_path = Path(name)
    table = 'archive_scout_integrity_paths'

    def stopped():
        if stop_event is not None and stop_event.is_set():
            raise Stopped

    def normalize(path):
        try:
            return os.path.normcase(str(path.resolve()))
        except OSError:
            return os.path.normcase(str(path.absolute()))

    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as issues:
            def issue(message):
                nonlocal count
                count += 1
                issues.write(message + '\n')

            stopped()
            database.set_progress_handler(lambda: int(stop_event is not None and stop_event.is_set()), 10000)
            if callback:
                callback(ProgressEvent('integrity_database', 'Checking SQLite and full-text index integrity'))
            healthy = True
            for row in database.execute('PRAGMA quick_check'):
                stopped()
                if str(row[0]).lower() != 'ok':
                    healthy = False
                    issue('SQLITE_INTEGRITY\t' + str(row[0]))
            if healthy:
                for row in database.execute('PRAGMA foreign_key_check'):
                    stopped()
                    healthy = False
                    issue('FOREIGN_KEY\t' + '\t'.join(map(str, row)))
                if database.execute("SELECT 1 FROM sqlite_master WHERE name='documents_fts'").fetchone():
                    database.execute('SAVEPOINT integrity_fts')
                    try:
                        database.execute("INSERT INTO documents_fts(documents_fts) VALUES('integrity-check')")
                    except sqlite3.DatabaseError as exc:
                        healthy = False
                        issue('FTS_INTEGRITY\t' + str(exc))
                    finally:
                        database.execute('ROLLBACK TO integrity_fts')
                        database.execute('RELEASE integrity_fts')
            database.execute(f'DROP TABLE IF EXISTS temp.{table}')
            database.execute(f'CREATE TEMP TABLE {table}(path TEXT PRIMARY KEY) WITHOUT ROWID')
            referenced = []
            def remember(path):
                referenced.append((normalize(path),))
                if len(referenced) >= 256:
                    flush_paths()
            def flush_paths():
                if referenced:
                    database.executemany(f'INSERT OR IGNORE INTO {table}(path) VALUES(?)', referenced)
                    referenced.clear()

            if healthy:
                total = int(database.execute('SELECT COUNT(*) FROM captures').fetchone()[0])
                documents_total = int(database.execute('SELECT COUNT(*) FROM documents').fetchone()[0])
                captures = database.execute('''SELECT c.id,c.original_url,c.state,c.document_id,c.local_path,
                    c.payload_availability,c.cleanup_pending,c.bytes_saved,d.id AS joined_document_id,
                    d.path AS document_path,d.size_bytes FROM captures c
                    LEFT JOIN documents d ON d.id=c.document_id ORDER BY c.id''')
                for index, row in enumerate(captures, 1):
                    stopped()
                    availability = str(row['payload_availability'] or 'not_acquired')
                    cleanup = int(row['cleanup_pending'] or 0)
                    local = str(row['local_path'] or row['document_path'] or '').strip()
                    path = Path(local) if local else None
                    if path is not None:
                        remember(path)
                    if availability != 'discarded' and not cleanup:
                        complete = availability in {'retained','retained_unscanned','spooled_unscanned'} or (
                            availability == 'not_acquired' and local and row['state'] in {'downloaded','downloaded_unscanned','scanning'})
                        if complete:
                            if path is None or not path.exists():
                                issue(f"MISSING_FILE\tcapture={row['id']}\t{row['original_url']}\t{path or ''}")
                            elif not path.is_file():
                                issue(f"NOT_A_FILE\tcapture={row['id']}\t{path}")
                            else:
                                size = path.stat().st_size
                                expected = int(row['bytes_saved'] or row['size_bytes'] or 0)
                                if not size and expected:
                                    issue(f"EMPTY_RETAINED_PAYLOAD\tcapture={row['id']}\t{row['original_url']}\t{path}")
                                elif expected and size != expected:
                                    issue(f"SIZE_MISMATCH\tcapture={row['id']}\tdatabase={expected}\tdisk={size}\t{path}")
                        elif availability == 'partial' and path is None:
                            issue(f"MISSING_PARTIAL_PATH\tcapture={row['id']}\t{row['original_url']}")
                    if row['document_id'] is not None and row['joined_document_id'] is None:
                        issue(f"BROKEN_DATABASE_LINK\tcapture={row['id']}\tstate={row['state']}\tdocument={row['document_id']}\t{row['original_url']}")
                    if callback and (index % 250 == 0 or index == total):
                        callback(ProgressEvent('integrity', f'Checked {index:,}/{total:,} capture manifests', index, total))
                if callback:
                    callback(ProgressEvent('integrity_references', 'Checking saved document and media references'))
                for source, column in (('documents','path'),('media_captures','path')):
                    for row in database.execute(f"SELECT {column} FROM {source} WHERE COALESCE({column},'')<>''"):
                        stopped()
                        remember(Path(str(row[0])))
                flush_paths()
                files_checked = 0
                if callback:
                    callback(ProgressEvent('integrity_files', 'Checking capture files for unreferenced payloads'))
                if (root / 'captures').exists():
                    for path in (root / 'captures').rglob('*'):
                        stopped()
                        if path.is_file() and path.suffix != '.part':
                            files_checked += 1
                            if not database.execute(f'SELECT 1 FROM {table} WHERE path=?', (normalize(path),)).fetchone():
                                issue(f'ORPHAN_FILE\t{path}')
                            if callback and files_checked % 250 == 0:
                                callback(ProgressEvent('integrity_files', f'Checked {files_checked:,} files', files_checked, None))
            else:
                issue('MANIFEST_INVENTORY_INCOMPLETE\tDatabase corruption prevents a trustworthy inventory')
            issues.flush()
            def lines():
                yield 'Archive Scout project integrity report'
                yield f'Generated: {utc_now()}'
                yield f'Project: {root}'
                yield f'Capture manifests checked: {total:,}'
                yield f'Documents recorded: {documents_total:,}'
                yield f'Issues found: {count:,}'
                yield 'SQLite and FTS checks: ' + ('passed' if healthy else 'FAILED')
                yield ''
                yield 'UNRESOLVED, UNIGNORED ERROR COUNTS'
                if healthy:
                    for row in database.execute('''SELECT operation,category,COUNT(*) AS count FROM errors
                         WHERE resolved=0 AND ignored=0 GROUP BY operation,category ORDER BY operation,category'''):
                        stopped()
                        yield f"{row['operation']}\t{row['category']}\t{row['count']}"
                yield ''
                yield 'INTEGRITY ISSUES'
                if count:
                    with issue_path.open(encoding='utf-8') as handle:
                        for line in handle:
                            stopped()
                            yield line.rstrip('\n')
                else:
                    yield 'None'
            stopped()
            if callback:
                callback(ProgressEvent('integrity_report', 'Writing the complete integrity report'))
            atomic_write_lines(report, lines())
        return report
    except sqlite3.OperationalError:
        stopped()
        raise
    finally:
        database.set_progress_handler(None, 0)
        try:
            database.execute(f'DROP TABLE IF EXISTS temp.{table}')
        finally:
            issue_path.unlink(missing_ok=True)
