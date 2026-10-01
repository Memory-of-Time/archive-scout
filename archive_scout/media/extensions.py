from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from urllib.parse import unquote, urlsplit

from ..config import MediaConfig, normalize_extension
from ..constants import IMAGE_EXTENSIONS, VIDEO_EXTENSIONS


MEDIA_SUFFIX_PATTERN = re.compile(r"(?i)(\.[a-z0-9]{1,10})(?=$|[?&#;])")


MIME_FORMAT_EXTENSIONS = {
    "image/jpeg": ".jpg", "image/jpg": ".jpg", "image/png": ".png",
    "image/gif": ".gif", "image/webp": ".webp", "image/bmp": ".bmp",
    "image/svg+xml": ".svg", "image/avif": ".avif", "image/heic": ".heic",
    "image/tiff": ".tif", "video/mp4": ".mp4", "video/quicktime": ".mov",
    "video/x-flv": ".flv", "video/webm": ".webm", "video/x-msvideo": ".avi",
    "video/x-ms-wmv": ".wmv", "application/x-shockwave-flash": ".swf",
}

def format_extension_from_mime(mimetype: str) -> str:
    return MIME_FORMAT_EXTENSIONS.get((mimetype or "").split(";", 1)[0].strip().casefold(), "")


def extension_from_url(url: str) -> str:
    try:
        parsed = urlsplit(url)
        path = unquote(parsed.path or "")
        suffix = Path(path).suffix.casefold()
        if suffix and re.fullmatch(r"\.[a-z0-9]{1,10}", suffix, re.IGNORECASE):
            return suffix
        # Some archived URLs contain tracking data attached with '&' or ';'
        # directly to the path, so pathlib sees '.jpg&ref=...' as the suffix.
        last_match = None
        for match in MEDIA_SUFFIX_PATTERN.finditer(path):
            last_match = match
        if last_match is not None:
            return last_match.group(1).casefold()
        # Media can also be passed as a query value, for example file=clip.wmv.
        last_match = None
        for match in MEDIA_SUFFIX_PATTERN.finditer(unquote(parsed.query)):
            last_match = match
        return last_match.group(1).casefold() if last_match is not None else ""
    except Exception:
        return ""


def media_kind(extension: str, mimetype: str = "") -> str | None:
    extension = normalize_extension(extension)
    mime = (mimetype or "").split(";", 1)[0].casefold()
    if extension in IMAGE_EXTENSIONS or mime.startswith("image/"):
        return "image"
    if extension in VIDEO_EXTENSIONS or mime.startswith("video/") or mime in {
        "application/x-shockwave-flash",
        "application/futuresplash",
        "application/vnd.rn-realmedia",
        "application/x-mplayer2",
    }:
        return "video"
    return None


@lru_cache(maxsize=128)
def _media_policy(
    include_images: bool,
    include_videos: bool,
    include_extensions: tuple[str, ...],
    exclude_extensions: tuple[str, ...],
) -> tuple[tuple[str, ...], frozenset[str]]:
    """Compile the media allow-list once per settings combination.

    Media discovery can evaluate tens of thousands of URLs. Older code rebuilt
    normalized configuration objects and extension sets for every candidate.
    """
    excluded = frozenset(normalize_extension(value) for value in exclude_extensions if normalize_extension(value))
    selected: list[str] = []
    for raw in include_extensions:
        extension = normalize_extension(raw)
        kind = media_kind(extension)
        if not extension or extension in excluded or kind is None:
            continue
        if kind == "image" and not include_images:
            continue
        if kind == "video" and not include_videos:
            continue
        selected.append(extension)
    return tuple(dict.fromkeys(selected)), excluded


def _policy_for(config: MediaConfig) -> tuple[tuple[str, ...], frozenset[str]]:
    return _media_policy(
        bool(config.include_images),
        bool(config.include_videos),
        tuple(str(value) for value in config.include_extensions),
        tuple(str(value) for value in config.exclude_extensions),
    )


def selected_extensions(config: MediaConfig) -> list[str]:
    selected, _excluded = _policy_for(config)
    return list(selected)


def allowed_media_url(url: str, config: MediaConfig, mimetype: str = "") -> tuple[bool, str | None, str]:
    """Return provisional media eligibility from URL/MIME metadata.

    Final acceptance is based on the replay payload format in media.downloader.
    Dynamic endpoints such as image.php may therefore be queued from a specific
    MIME while misleading suffix/MIME conflicts remain provisional rather than
    being treated as proof of the final format.
    """
    url_extension = extension_from_url(url)
    mime_extension = format_extension_from_mime(mimetype)
    kind = media_kind(url_extension, mimetype)
    if not kind:
        return False, None, url_extension
    selected, excluded = _policy_for(config)
    if kind == "image" and not config.include_images:
        return False, kind, mime_extension or url_extension
    if kind == "video" and not config.include_videos:
        return False, kind, mime_extension or url_extension
    evidence_extensions = [
        value for value in (url_extension, mime_extension)
        if value and value in IMAGE_EXTENSIONS | VIDEO_EXTENSIONS
    ]
    if any(value in excluded for value in evidence_extensions):
        return False, kind, mime_extension or url_extension
    if evidence_extensions and not any(value in selected for value in evidence_extensions):
        return False, kind, mime_extension or url_extension
    # No known format suffix is still a valid provisional candidate when MIME
    # or embedding context establishes the broad kind. Payload validation later
    # enforces the user's concrete selected format list.
    return True, kind, mime_extension or (url_extension if url_extension in IMAGE_EXTENSIONS | VIDEO_EXTENSIONS else "")
