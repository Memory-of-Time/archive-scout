"""Short-lived, capture-bound proof spanning file publication and SQL commit.

Receipts exist only until the owner commits raw-byte evidence. They do not
replace SQLite state, extend completed scope, or make a partial body resumable.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import re
from pathlib import Path

from ..utils import atomic_write_bytes


LEGACY_RECOVERY_KEY = 'replay_legacy_checked:'


def note_recovery_run(database, run_id: int) -> None:
    previous = database.execute('SELECT MAX(id) FROM operation_runs WHERE id<?', (run_id,)).fetchone()[0]
    marker = database.execute("SELECT value FROM project_meta WHERE key='replay_recovery_run'").fetchone()
    if (marker[0] if marker else None) != (str(previous) if previous is not None else None):
        database.execute("DELETE FROM project_meta WHERE key GLOB ?", (LEGACY_RECOVERY_KEY + '*',))
    database.execute("INSERT OR REPLACE INTO project_meta VALUES('replay_recovery_run',?)", (str(run_id),))


def _legacy_receipt_path(path: Path) -> Path:
    return path.with_name(path.name + '.validated.json')


def recovery_directory(root: Path) -> Path:
    return root / 'captures' / '.replay-recovery'


def receipt_path(path: Path) -> Path:
    captures = path.parent.parent.parent
    legacy = _legacy_receipt_path(path)
    if captures.name == 'captures':
        return recovery_directory(captures.parent) / legacy.name
    return legacy


def write_receipt(path: Path, row, result: dict) -> None:
    value = {
        'version': 1, 'capture_id': int(row['id']),
        'original_url': str(row['original_url']), 'timestamp': str(row['timestamp']),
        'content_hash': str(result['content_hash']), 'bytes_saved': int(result['bytes_saved']),
        'response_content_type': str(result.get('response_content_type') or ''),
        'final_url': str(result.get('final_url') or ''),
        'http_status': int(result.get('http_status') or 200),
    }
    if not re.fullmatch(r'[0-9a-f]{64}', value['content_hash']):
        raise ValueError('validated transfer has no SHA-256 proof')
    destination = receipt_path(path)
    if destination.parent != path.parent and not destination.parent.is_dir():
        destination.parent.mkdir(parents=True, exist_ok=True)
        sync_directory(destination.parent.parent)
    atomic_write_bytes(destination, json.dumps(value, ensure_ascii=True).encode('utf-8'))
    if destination.parent != path.parent:
        sync_directory(destination.parent)


def discard_receipt(path: Path) -> None:
    # A cleanup failure must not undo a committed capture. The bounded residue
    # is harmless: recovery always checks identity AND the full file hash.
    for receipt in dict.fromkeys((receipt_path(path), _legacy_receipt_path(path))):
        try:
            receipt.unlink(missing_ok=True)
        except OSError:
            pass


def sync_directory(path: Path) -> None:
    """Persist a POSIX rename where supported; Windows has no directory fsync."""
    if os.name == 'nt':
        return
    import errno
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError as exc:
        if exc.errno not in {errno.EINVAL, errno.ENOTSUP, errno.EBADF}:
            raise


def read_receipt_file(path: Path) -> dict | None:
    try:
        with path.open('rb') as handle:
            raw = handle.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            return None
        value = json.loads(raw)
        if (not isinstance(value, dict) or value.get('version') != 1
                or type(value.get('capture_id')) is not int
                or not 0 < value['capture_id'] < 2**63
                or not re.fullmatch(r'[0-9a-f]{64}', str(value.get('content_hash', '')))
                or type(value.get('bytes_saved')) is not int
                or value['bytes_saved'] < 1):
            return None
        return value
    except (OSError, ValueError, TypeError, UnicodeError):
        return None


def _receipts(path: Path, row):
    for candidate in dict.fromkeys((receipt_path(path), _legacy_receipt_path(path))):
        value = read_receipt_file(candidate)
        if (value and value['capture_id'] == int(row['id'])
                and value.get('original_url') == str(row['original_url'])
                and value.get('timestamp') == str(row['timestamp'])):
            yield value


def read_receipt(path: Path, row) -> dict | None:
    return next(_receipts(path, row), None)


def prove_file(path: Path, row, *, expected_hash=None, expected_size=None,
               receipt_for: Path | None = None) -> dict | None:
    """Stream hash once. No readable-prefix or filename-only acceptance path."""
    sha256, sha1, length = hashlib.sha256(), hashlib.sha1(), 0
    with path.open('rb') as handle:
        for segment in iter(lambda: handle.read(1024 * 1024), b''):
            sha256.update(segment); sha1.update(segment); length += len(segment)
    digest = sha256.hexdigest()
    if expected_hash and digest == expected_hash and length == int(expected_size or 0):
        return {'content_hash': digest, 'bytes_saved': length, 'source': 'database'}
    # An existing database proof must never be bypassed by weak CDX metadata.
    # A newer validated receipt can legitimately describe a replacement transfer.
    for receipt in _receipts(receipt_for or path, row):
        if digest == receipt['content_hash'] and length == receipt['bytes_saved']:
            return {**receipt, 'source': 'receipt'}
    if expected_hash:
        return None
    cdx = str(dict(row).get('digest') or '').upper().removeprefix('SHA1:').rstrip('=')
    if re.fullmatch(r'[A-Z2-7]{32}', cdx):
        if base64.b32encode(sha1.digest()).decode() == cdx:
            return {'content_hash': digest, 'bytes_saved': length, 'source': 'cdx'}
    return None


def preserve_unverified(path: Path) -> None:
    """Keep questionable evidence outside the normal retained-body filename."""
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for segment in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(segment)
    label = path.name
    if len(label.encode('utf-8', 'surrogatepass')) > 120:
        identity = hashlib.sha256(label.encode('utf-8', 'surrogatepass')).hexdigest()[:16]
        label = label[:40] + '__' + identity
    destination = path.with_name(label + '.unverified-' + digest.hexdigest())
    if destination.exists():
        # Keep an existing version; replacing it could destroy inspection evidence.
        return
    os.replace(path, destination)
