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


def receipt_path(path: Path) -> Path:
    return path.with_name(path.name + '.validated.json')


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
    atomic_write_bytes(receipt_path(path), json.dumps(value, ensure_ascii=True).encode('utf-8'))


def discard_receipt(path: Path) -> None:
    # A cleanup failure must not undo a committed capture. The bounded residue
    # is harmless: recovery always checks identity AND the full file hash.
    try:
        receipt_path(path).unlink(missing_ok=True)
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


def read_receipt(path: Path, row) -> dict | None:
    try:
        with receipt_path(path).open('rb') as handle:
            raw = handle.read(1024 * 1024 + 1)
        if len(raw) > 1024 * 1024:
            return None
        value = json.loads(raw)
        if (not isinstance(value, dict) or value.get('version') != 1
                or value.get('capture_id') != int(row['id'])
                or value.get('original_url') != str(row['original_url'])
                or value.get('timestamp') != str(row['timestamp'])
                or not re.fullmatch(r'[0-9a-f]{64}', str(value.get('content_hash', '')))
                or type(value.get('bytes_saved')) is not int
                or value['bytes_saved'] < 1):
            return None
        return value
    except (OSError, ValueError, TypeError, UnicodeError):
        return None


def prove_file(path: Path, row, *, expected_hash=None, expected_size=None,
               receipt_for: Path | None = None) -> dict | None:
    """Stream hash once. No readable-prefix or filename-only acceptance path."""
    receipt = read_receipt(receipt_for or path, row)
    sha256, sha1, length = hashlib.sha256(), hashlib.sha1(), 0
    with path.open('rb') as handle:
        for segment in iter(lambda: handle.read(1024 * 1024), b''):
            sha256.update(segment); sha1.update(segment); length += len(segment)
    digest = sha256.hexdigest()
    if expected_hash and digest == expected_hash and length == int(expected_size or 0):
        return {'content_hash': digest, 'bytes_saved': length, 'source': 'database'}
    # An existing database proof must never be bypassed by weak CDX metadata.
    # A newer validated receipt can legitimately describe a replacement transfer.
    if receipt and digest == receipt['content_hash'] and length == receipt['bytes_saved']:
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
    destination = path.with_name(path.name + '.unverified-' + digest.hexdigest())
    if destination.exists():
        # Keep an existing version; replacing it could destroy inspection evidence.
        return
    os.replace(path, destination)
