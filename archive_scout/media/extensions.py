from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from urllib.parse import unquote, urlsplit

from ..config import MediaConfig, normalize_extension
from ..constants import IMAGE_EXTENSIONS, VIDEO_EXTENSIONS


MEDIA_SUFFIX_PATTERN = re.compile(r"(?i)(\.[a-z0-9]{1,10})(?=$|[?&#;])")


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
    extension = extension_from_url(url)
    kind = media_kind(extension, mimetype)
    if not kind:
        return False, None, extension
    selected, excluded = _policy_for(config)
    if extension and extension not in selected:
        return False, kind, extension
    if extension in excluded:
        return False, kind, extension
    if kind == "image" and not config.include_images:
        return False, kind, extension
    if kind == "video" and not config.include_videos:
        return False, kind, extension
    return True, kind, extension
