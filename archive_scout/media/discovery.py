from __future__ import annotations

import html
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, unquote_plus, urlsplit

from ..config import MediaConfig
from ..content import decode_bytes, normalize_link, safe_urlsplit
from ..parsing.embeds import EMBED_URL_PATTERN, FLASHVARS_URL_PATTERN, SCRIPT_MEDIA_PATTERN
from .extensions import allowed_media_url, extension_from_url, media_kind

CSS_URL_PATTERN = re.compile(r"(?is)url\(\s*(['\"]?)(.*?)\1\s*\)")
WAYBACK_REPLAY_PATTERN = re.compile(r"^/web/\d{1,14}[a-z_]{0,4}/(https?://.+)$", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class DiscoveredMedia:
    url: str
    kind_hint: str = ""


def _host_from_target(target: str) -> str:
    value = target.strip().replace("*.", "").rstrip("*").rstrip("/")
    if not value:
        return ""
    if "://" not in value:
        value = "http://" + value
    try:
        return (urlsplit(value).hostname or "").casefold()
    except ValueError:
        return ""


def target_hosts(targets: list[str]) -> set[str]:
    return {host for target in targets if (host := _host_from_target(target))}


def hosts_related(host: str, targets: set[str]) -> bool:
    host = host.casefold().strip(".")
    for target in targets:
        target = target.casefold().strip(".")
        if host == target or host.endswith("." + target) or target.endswith("." + host):
            return True
    return False


def unwrap_wayback_url(url: str) -> str:
    parsed = safe_urlsplit(url)
    if not parsed or (parsed.hostname or "").casefold() != "web.archive.org":
        return url
    match = WAYBACK_REPLAY_PATTERN.match(parsed.path or "")
    if match:
        original = unquote(match.group(1))
        if parsed.query:
            original += "?" + parsed.query
        return original
    return url


def _normalize(raw: str, base_url: str) -> str:
    raw = html.unescape(raw or "").strip().strip("'\"")
    if not raw or raw.casefold().startswith(("data:", "javascript:", "mailto:", "blob:")):
        return ""
    value = normalize_link(raw, base_url)
    return unwrap_wayback_url(value)


class _MediaParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.items: list[DiscoveredMedia] = []

    @staticmethod
    def _attrs(attrs: list[tuple[str, str | None]]) -> dict[str, str]:
        return {key.casefold(): html.unescape(value or "") for key, value in attrs}

    def _add(self, raw: str, hint: str = "") -> None:
        value = _normalize(raw, self.base_url)
        if value:
            self.items.append(DiscoveredMedia(value, hint))

    def _add_srcset(self, raw: str, hint: str = "image") -> None:
        for part in (raw or "").split(","):
            value = part.strip().split(None, 1)[0] if part.strip() else ""
            self._add(value, hint)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.casefold()
        mapped = self._attrs(attrs)
        if tag == "img":
            for key in (
                "src", "data-src", "data-original", "data-lazy-src", "data-original-src",
                "data-url", "data-image", "data-thumb", "data-thumbnail",
            ):
                self._add(mapped.get(key, ""), "image")
            for key in ("srcset", "data-srcset", "data-lazy-srcset"):
                self._add_srcset(mapped.get(key, ""), "image")
        elif tag == "video":
            for key in ("src", "data-src", "data-video", "data-url"):
                self._add(mapped.get(key, ""), "video")
            for key in ("poster", "data-poster"):
                self._add(mapped.get(key, ""), "image")
        elif tag == "source":
            mime = mapped.get("type", "").casefold()
            hint = "image" if mime.startswith("image/") else "video" if mime.startswith("video/") else ""
            for key in ("src", "data-src"):
                self._add(mapped.get(key, ""), hint)
            for key in ("srcset", "data-srcset"):
                self._add_srcset(mapped.get(key, ""), hint or "image")
        elif tag in {"embed", "object"}:
            raw = mapped.get("src") or mapped.get("data") or mapped.get("data-src") or ""
            self._add(raw, "video")
        elif tag == "param":
            name = mapped.get("name", "").casefold()
            value = mapped.get("value", "")
            if name == "flashvars":
                for match in FLASHVARS_URL_PATTERN.finditer(value):
                    self._add(unquote_plus(match.group(1)), "video")
            elif name in {"movie", "file", "filename", "url", "src", "media", "clip"}:
                self._add(value, "video")
        elif tag in {"iframe", "frame"}:
            # Frame URLs themselves are not assumed to be media. Legacy player
            # extraction handles direct media configuration embedded in frames.
            self._add(mapped.get("data-video", ""), "video")
        elif tag == "a":
            self._add(mapped.get("href", ""), "")
        elif tag == "input" and mapped.get("type", "").casefold() == "image":
            self._add(mapped.get("src", ""), "image")
        elif tag == "meta":
            name = (mapped.get("property") or mapped.get("name") or "").casefold()
            content = mapped.get("content", "")
            if name in {
                "og:image", "og:image:url", "og:image:secure_url", "twitter:image",
                "twitter:image:src", "thumbnail", "thumbnailurl",
            }:
                self._add(content, "image")
            elif name in {
                "og:video", "og:video:url", "og:video:secure_url", "twitter:player:stream",
            }:
                self._add(content, "video")
        elif tag == "link":
            rel = {value.casefold() for value in mapped.get("rel", "").split()}
            as_type = mapped.get("as", "").casefold()
            if ("preload" in rel or "prefetch" in rel) and as_type in {"image", "video"}:
                self._add(mapped.get("href", ""), as_type)
            self._add_srcset(mapped.get("imagesrcset", ""), "image")

        for key in ("background", "data-background", "data-bg", "data-background-image"):
            self._add(mapped.get(key, ""), "image")
        style = mapped.get("style", "")
        for match in CSS_URL_PATTERN.finditer(style):
            self._add(match.group(2), "image")


def _allowed(
    candidate: DiscoveredMedia,
    media: MediaConfig,
    excluded_extensions: frozenset[str],
) -> DiscoveredMedia | None:
    allowed, kind, _ = allowed_media_url(candidate.url, media)
    if allowed and kind:
        return DiscoveredMedia(candidate.url, kind)
    if candidate.kind_hint in {"image", "video"}:
        if candidate.kind_hint == "image" and not media.include_images:
            return None
        if candidate.kind_hint == "video" and not media.include_videos:
            return None
        extension = extension_from_url(candidate.url)
        if extension and extension in excluded_extensions:
            return None
        # Extensionless/tag-identified media is retained so CDX MIME metadata can
        # make the final decision during lookup.
        if not extension:
            return candidate
        guessed = media_kind(extension)
        if guessed == candidate.kind_hint:
            return candidate
    return None


def discover_media(raw: str, base_url: str, media: MediaConfig, known_links: list[str] | None = None) -> list[DiscoveredMedia]:
    candidates: list[DiscoveredMedia] = []
    for link in known_links or []:
        value = _normalize(str(link), base_url)
        if value:
            candidates.append(DiscoveredMedia(value, ""))
    parser = _MediaParser(base_url)
    try:
        parser.feed(raw)
        parser.close()
    except Exception:
        pass
    candidates.extend(parser.items)
    # Legacy player/config URLs are recovered with regex passes over the same raw
    # text instead of feeding the whole document through a second HTMLParser.
    for pattern in (EMBED_URL_PATTERN, SCRIPT_MEDIA_PATTERN):
        for match in pattern.finditer(raw):
            value = _normalize(match.group(1), base_url)
            if value:
                candidates.append(DiscoveredMedia(value, "video"))
    for match in CSS_URL_PATTERN.finditer(raw):
        value = _normalize(match.group(2), base_url)
        if value:
            candidates.append(DiscoveredMedia(value, "image"))

    unique: dict[str, DiscoveredMedia] = {}
    excluded_extensions = frozenset(media.exclude_extensions)
    for candidate in candidates:
        accepted = _allowed(candidate, media, excluded_extensions)
        if not accepted:
            continue
        current = unique.get(accepted.url)
        if current is None or (not current.kind_hint and accepted.kind_hint):
            unique[accepted.url] = accepted
    return list(unique.values())


def safe_document_text(output_dir: Path, path_value: str, max_bytes: int) -> str:
    root = output_dir.absolute()
    path = Path(path_value)
    candidate = path if path.is_absolute() else root / path
    try:
        resolved = candidate.resolve()
        resolved_root = root.resolve()
    except (OSError, RuntimeError):
        return ""
    if resolved != resolved_root and resolved_root not in resolved.parents:
        return ""
    try:
        if not resolved.is_file():
            return ""
        with resolved.open("rb") as handle:
            data = handle.read(max(1, int(max_bytes)) + 1)
        if len(data) > max_bytes:
            data = data[:max_bytes]
        return decode_bytes(data)
    except OSError:
        return ""
