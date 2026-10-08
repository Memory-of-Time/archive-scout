"""Shared byte-evidence policy for replay sniffing and complete text decoding."""
from __future__ import annotations

import codecs
import re

CHARSET_PATTERN = re.compile(r"charset\s*=\s*['\"]?([A-Za-z0-9._-]+)", re.I)


class TextDecodingError(UnicodeError):
    """Retain the payload and retry decoding; this is not connection damage."""


def _canonical(value: str) -> str:
    try:
        return codecs.lookup(value).name
    except LookupError:
        return value.casefold()


def encoding_candidates(data: bytes, content_type: str = "") -> list[str]:
    for bom, encoding in ((codecs.BOM_UTF32_LE, "utf-32-le"),
                          (codecs.BOM_UTF32_BE, "utf-32-be"),
                          (codecs.BOM_UTF8, "utf-8-sig"),
                          (codecs.BOM_UTF16_LE, "utf-16-le"),
                          (codecs.BOM_UTF16_BE, "utf-16-be")):
        if data.startswith(bom):
            return [encoding]
    sample = data[:16384]
    evidence: list[str] = []
    if len(sample) >= 8 and b"\x00" in sample:
        ratios = [sample[offset::4].count(0) / max(1, len(sample[offset::4])) for offset in range(4)]
        if min(ratios[1:]) > .7 and ratios[0] < .1:
            evidence.append("utf-32-le")
        elif min(ratios[:3]) > .7 and ratios[3] < .1:
            evidence.append("utf-32-be")
        even, odd = sample[::2], sample[1::2]
        if odd.count(0) / max(1, len(odd)) > .25 and even.count(0) / len(even) < .05:
            evidence.append("utf-16-le")
        elif even.count(0) / len(even) > .25 and odd.count(0) / max(1, len(odd)) < .05:
            evidence.append("utf-16-be")
    head = sample.decode("ascii", "ignore")
    declarations = []
    for value in (content_type, head):
        match = CHARSET_PATTERN.search(value or "")
        if match:
            declarations.append(_canonical(match.group(1)))
    xml = re.search(r"<\?xml[^>]+encoding\s*=\s*['\"]([^'\"]+)", head, re.I)
    if xml:
        declarations.append(_canonical(xml.group(1)))
    wide = any(value.startswith(("utf-16", "utf-32")) for value in declarations)
    if wide and not evidence:
        # A generic wide encoding needs a BOM or endianness evidence. A plainly
        # ASCII representation with prose/markup is stronger than a bad header.
        ascii_text = bool(sample) and all(9 <= value <= 13 or 32 <= value < 127 for value in sample)
        prose = bool(re.search(r"[A-Za-z]{3,}[ \r\n\t]+[A-Za-z]{3,}|<[!?/A-Za-z]", head))
        if ascii_text and prose:
            declarations = [value for value in declarations if not value.startswith(("utf-16", "utf-32"))]
        elif any(value in {"utf-16", "utf-32"} for value in declarations):
            raise TextDecodingError("Wide charset has no BOM or trustworthy endianness evidence")
        else:
            # Explicit LE/BE declarations support non-ASCII wide text even when
            # the prefix has few NULs, e.g. a page containing only CJK characters.
            return list(dict.fromkeys(declarations))
    return list(dict.fromkeys(evidence + declarations + ["utf-8", "windows-1252", "latin-1"]))


def decode_text(data: bytes, content_type: str = "") -> tuple[str, str]:
    candidates = encoding_candidates(data, content_type)
    for encoding in candidates:
        try:
            return data.decode(encoding), encoding
        except (LookupError, UnicodeError):
            continue
    # A malformed declared/BOM wide representation must not silently become
    # Latin-1, replacement characters, or a successfully scanned empty match.
    raise TextDecodingError("Payload cannot be decoded completely with its validated encoding")


def decode_prefix(data: bytes, content_type: str = "") -> tuple[str, str]:
    """A sniffing prefix may end inside a multibyte character."""
    for encoding in encoding_candidates(data, content_type):
        try:
            return codecs.getincrementaldecoder(encoding)("strict").decode(data, final=False), encoding
        except (LookupError, UnicodeError):
            continue
    raise TextDecodingError("Prefix cannot be decoded with its validated encoding")
