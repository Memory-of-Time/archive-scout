from __future__ import annotations

import json
import sqlite3
import zlib
from pathlib import Path
from typing import Mapping, Any

from .content import decode_bytes, parse_page


def compress_text(value: str) -> bytes:
    if not value:
        return b''
    return zlib.compress(value.encode('utf-8', 'replace'), level=6)


def decompress_text(value: object) -> str:
    if value in (None, b'', ''):
        return ''
    try:
        payload = bytes(value) if not isinstance(value, str) else value.encode('latin-1')
        return zlib.decompress(payload).decode('utf-8', 'replace')
    except Exception:
        return ''


def _get(row: Mapping[str, Any] | sqlite3.Row, key: str, default: Any = '') -> Any:
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return default


def document_body(row: Mapping[str, Any] | sqlite3.Row) -> str:
    body = str(_get(row, 'body_text', '') or '')
    if body:
        return body
    compressed = _get(row, 'body_zlib', None)
    body = decompress_text(compressed)
    if body:
        return body
    path = Path(str(_get(row, 'path', '') or ''))
    if not path.is_file():
        return ''
    try:
        raw = decode_bytes(path.read_bytes(), '')
        _title, visible, _links = parse_page(raw, str(_get(row, 'original_url', '') or ''))
        return visible
    except Exception:
        return ''


def document_raw(row: Mapping[str, Any] | sqlite3.Row, content_type: str = '') -> str:
    path = Path(str(_get(row, 'path', '') or ''))
    if not path.is_file():
        return ''
    return decode_bytes(path.read_bytes(), content_type)


def document_links(row: Mapping[str, Any] | sqlite3.Row) -> list[str]:
    try:
        payload = json.loads(str(_get(row, 'links_json', '[]') or '[]'))
    except Exception:
        return []
    return [str(value) for value in payload] if isinstance(payload, list) else []
