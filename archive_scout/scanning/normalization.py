"""Scanner-local normalization, equivalent to utils.normalize_search.

Keep acquisition/query normalization untouched. Splitting Unicode whitespace in
C avoids constructing a regex replacement for every word of every scan view.
"""
from __future__ import annotations

import html
import unicodedata
import urllib.parse

from ..utils import decode_common_escapes


def normalize_search(value: str) -> str:
    value = value or ""
    if "%" in value:
        value = urllib.parse.unquote(value)
    if "&" in value:
        value = html.unescape(value)
    if "\\" in value:
        value = decode_common_escapes(value)
    value = unicodedata.normalize("NFKC", value).casefold().replace("_", " ")
    return " ".join(value.split())
