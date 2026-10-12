from __future__ import annotations

import json
from typing import Any

try:  # Optional accelerator; source installs remain dependency-light.
    import orjson as _orjson
except ImportError:  # pragma: no cover - depends on the runtime environment
    _orjson = None

JSONDecodeErrors = (json.JSONDecodeError,) if _orjson is None else (json.JSONDecodeError, _orjson.JSONDecodeError)


def dumps(value: Any, *, sort_keys: bool = False, indent: bool = False) -> str:
    if _orjson is not None:
        option = 0
        if sort_keys:
            option |= _orjson.OPT_SORT_KEYS
        if indent:
            option |= _orjson.OPT_INDENT_2
        return _orjson.dumps(value, option=option).decode("utf-8")
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=sort_keys,
        indent=2 if indent else None,
        separators=None if indent else (",", ":"),
    )


def loads(value: str | bytes | bytearray | memoryview) -> Any:
    if _orjson is not None:
        return _orjson.loads(value)
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("utf-8", "replace")
    return json.loads(value)


def accelerated() -> bool:
    return _orjson is not None
