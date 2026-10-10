from __future__ import annotations

import html
import re
import urllib.parse
from html.parser import HTMLParser
from pathlib import Path

try:
    from selectolax.lexbor import LexborHTMLParser
except Exception:  # pragma: no cover - fallback retained for source-only environments
    LexborHTMLParser = None

from .constants import BINARY_EXTENSIONS, TEXT_EXTENSIONS
from .utils import clean_space

URL_PATTERN = re.compile(r'''(?ix)\b(?:https?://|ftp://|www\.)[^\s<>"'()\[\]{}]+''')
TITLE_PATTERN = re.compile(r"(?is)<title[^>]*>(.*?)</title>")
TAG_PATTERN = re.compile(r"(?is)<[^>]+>")
CHARSET_PATTERN = re.compile(r"charset\s*=\s*['\"]?([A-Za-z0-9._-]+)", re.IGNORECASE)
REPLAY_ERROR_MARKERS = (
    ("this url has been excluded from the wayback machine", "wayback_excluded"),
    ("blocked site error", "wayback_excluded"),
    ("page cannot be displayed due to robots.txt", "robots_blocked"),
    ("not saved because of robots.txt", "robots_blocked"),
    ("wayback machine doesn't have that page archived", "missing_capture"),
    ("wayback machine has not archived that url", "missing_capture"),
    ("not in archive", "missing_capture"),
    ("the machine that serves this file is down", "origin_unavailable"),
)


class PageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []
        self.text: list[str] = []
        self.ignore_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lowered = tag.lower()
        if lowered in {"script", "style", "noscript", "svg"}:
            self.ignore_depth += 1
        for key, value in attrs:
            if value and key.lower() in {"href", "src", "data", "poster", "action", "movie"}:
                self.links.append(value.strip())

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in {"script", "style", "noscript", "svg"} and self.ignore_depth:
            self.ignore_depth -= 1

    def handle_data(self, data: str) -> None:
        if not self.ignore_depth and data:
            self.text.append(data)


def safe_urlsplit(url: str):
    try:
        return urllib.parse.urlsplit(url)
    except (TypeError, ValueError, UnicodeError):
        return None


def normalize_link(raw: str, base: str) -> str:
    raw = html.unescape(raw or "").strip().strip("'\"").rstrip(".,;:!?)]]}")
    if not raw:
        return ""
    if raw.lower().startswith("www."):
        raw = "http://" + raw
    try:
        return urllib.parse.urljoin(base, raw)
    except ValueError:
        return raw


def title_from_html(raw: str) -> str:
    match = TITLE_PATTERN.search(raw)
    if not match:
        return ""
    return clean_space(html.unescape(TAG_PATTERN.sub(" ", match.group(1))))[:500]


def decode_bytes(data: bytes, content_type: str = "") -> str:
    """Decode text once, respecting BOM and declared legacy/wide encodings."""
    for bom, encoding in (
        (b"\xef\xbb\xbf", "utf-8-sig"),
        (b"\xff\xfe\x00\x00", "utf-32"),
        (b"\x00\x00\xfe\xff", "utf-32"),
        (b"\xff\xfe", "utf-16"),
        (b"\xfe\xff", "utf-16"),
    ):
        if data.startswith(bom):
            return data.decode(encoding)
    candidates: list[str] = []
    match = CHARSET_PATTERN.search(content_type)
    if match:
        candidates.append(match.group(1))
    head = data[:4096].decode("ascii", "ignore")
    match = CHARSET_PATTERN.search(head)
    if match:
        candidates.append(match.group(1))
    candidates.extend(["utf-8", "windows-1252", "latin-1"])
    for encoding in dict.fromkeys(candidates):
        try:
            return data.decode(encoding)
        except (LookupError, UnicodeError):
            continue
    return data.decode("utf-8", "replace")


def _binary_signature(data: bytes) -> bool:
    """Detect unambiguous binary structures; avoid two-byte BM/ID3 false positives."""
    if data.startswith((b"\x89PNG\r\n\x1a\n", b"\xff\xd8\xff", b"GIF87a", b"GIF89a", b"%PDF-", b"PK\x03\x04", b"\x1f\x8b\x08")):
        return True
    if len(data) >= 12 and data.startswith(b"RIFF") and data[8:12] in {b"WEBP", b"WAVE", b"AVI "}:
        return True
    if len(data) >= 26 and data.startswith(b"BM"):
        file_size = int.from_bytes(data[2:6], "little")
        pixel_offset = int.from_bytes(data[10:14], "little")
        dib_header = int.from_bytes(data[14:18], "little")
        if dib_header in {12, 40, 52, 56, 64, 108, 124} and 14 + dib_header <= pixel_offset <= file_size:
            return True
    if len(data) >= 12 and data[4:8] == b"ftyp":
        return True
    if len(data) >= 10 and data.startswith(b"ID3") and data[3] <= 4 and all(b < 128 for b in data[6:10]):
        return True
    if data.startswith((b"OggS\x00", b"fLaC", b"\x1a\x45\xdf\xa3", b"\x7fELF", b"MZ\x90\x00")):
        return True
    return False


def looks_textual_bytes(data: bytes, content_type: str = "") -> bool:
    """Require plausible decoded text even for a misleading text/* declaration."""
    if not data:
        return True
    sample = data[:8192]
    if _binary_signature(sample):
        return False
    mime = (content_type or "").split(";", 1)[0].strip().casefold()
    if mime.startswith(("image/", "audio/", "video/", "font/")):
        return False
    if sample.startswith((b"\xef\xbb\xbf", b"\xff\xfe", b"\xfe\xff", b"\x00\x00\xfe\xff")):
        try:
            decoded = decode_bytes(data[:min(len(data), 16384)], content_type)
            return sum(ord(ch) < 9 or 13 < ord(ch) < 32 for ch in decoded[:4096]) / max(1, len(decoded[:4096])) < 0.04
        except UnicodeError:
            return False
    # UTF-16/32 without BOM is accepted only with an explicit supported charset.
    declared = CHARSET_PATTERN.search(content_type)
    if declared and declared.group(1).casefold().replace("_", "-").startswith(("utf-16", "utf-32")):
        try:
            decoded = sample.decode(declared.group(1))
            return "\ufffd" not in decoded and sum(ord(ch) < 9 or 13 < ord(ch) < 32 for ch in decoded) / max(1, len(decoded)) < .04
        except (LookupError, UnicodeError):
            return False
    if b"\x00" in sample:
        return False
    control = sum(byte < 9 or 13 < byte < 32 for byte in sample)
    return control / len(sample) < 0.04


def is_text_candidate(url: str, mimetype: str = "") -> bool:
    parsed = safe_urlsplit(url)
    filename = parsed.path.rsplit("/", 1)[-1] if parsed else ""
    filename = urllib.parse.unquote(filename)
    extension = filename[filename.rfind("."):].casefold() if "." in filename else ""
    mime = (mimetype or "").split(";", 1)[0].strip().casefold()
    # Strong MIME evidence can override a misleading file extension.
    if mime.startswith("text/") or mime in {"application/json", "application/javascript", "application/xml", "application/xhtml+xml", "application/ld+json", "image/svg+xml"}:
        return True
    if mime.startswith(("image/", "audio/", "video/", "font/")):
        return False
    if extension in TEXT_EXTENSIONS:
        return True
    if extension in BINARY_EXTENSIONS:
        return False
    if mime in {"application/pdf", "application/zip", "application/octet-stream"}:
        return False
    return True

def parse_page(raw: str, original: str) -> tuple[str, str, list[str]]:
    links: set[str] = set()
    title = ""
    visible = ""
    if LexborHTMLParser is not None:
        try:
            tree = LexborHTMLParser(raw)
            title_node = tree.css_first("title")
            if title_node is not None:
                title = clean_space(title_node.text(deep=True, separator=" ", strip=True))[:500]
            root = tree.root
            if root is not None:
                for node in root.traverse():
                    attrs = node.attributes
                    for key in ("href", "src", "data", "poster", "action", "movie"):
                        value = attrs.get(key)
                        if value:
                            normalized = normalize_link(value, original)
                            if normalized:
                                links.add(normalized)
                for tag in ("script", "style", "noscript", "svg"):
                    for node in tree.css(tag):
                        node.decompose()
                text_root = tree.body or tree.root
                if text_root is not None:
                    visible = clean_space(text_root.text(deep=True, separator=" ", strip=True))
        except Exception:
            title = ""
            visible = ""
            links.clear()
    if not visible and not links:
        parser = PageParser()
        try:
            parser.feed(raw)
        except Exception:
            pass
        visible = clean_space(" ".join(parser.text))
        for value in parser.links:
            normalized = normalize_link(value, original)
            if normalized:
                links.add(normalized)
    if not title:
        title = title_from_html(raw)
    for value in URL_PATTERN.findall(raw):
        normalized = normalize_link(value, original)
        if normalized:
            links.add(normalized)
    return title, visible, sorted(links)


def classify_replay_content(raw: str, final_url: str) -> str | None:
    """Return a specific Wayback replay problem when an error shell was saved as HTTP 200.

    Wayback sometimes serves explanatory HTML instead of the requested capture. Keeping
    these reasons separate makes the UI useful: an exclusion or robots policy should not
    look like a transient network failure, while an unavailable origin can be retried later.
    """
    lowered = raw[:20000].casefold()
    for marker, category in REPLAY_ERROR_MARKERS:
        if marker in lowered:
            return category
    if "/web/" not in final_url and "web.archive.org" in final_url:
        return "invalid_wayback_replay"
    return None
