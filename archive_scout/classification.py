from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass
from pathlib import Path

from .constants import AUDIO_EXTENSIONS, IMAGE_EXTENSIONS, TEXT_EXTENSIONS, VIDEO_EXTENSIONS

RESOURCE_CLASSIFIER_REVISION = 4
PREVIEW_BUDGET = 64 * 1024

MEDIA_DESCRIPTOR_EXTENSIONS = {".asx", ".m3u", ".m3u8", ".mpd", ".pls", ".ram", ".smil", ".smi", ".vtt", ".srt", ".wax", ".wvx", ".wmx", ".wpl"}

# Explicit preservation policy: these suffixes never enter *text* replay, even
# when historical CDX metadata incorrectly claims text/html. This is separate
# from the best-evidence content type (audio/descriptor/unknown may be nuanced).
SUFFIX_NON_TEXT_IMAGES = set(".jpeg .jpg .png .bmp .gif .tiff .psd .dib .webp .sr .ras .jfif .xxx".split())
SUFFIX_NON_TEXT_MEDIA = set(".webm .mpeg .mpg .wmv .mjpg .mjpeg .rv .avi .mov .3gp .rm .swf .flv .mkv .vob .ogv .ogg .drc .gifv .mng .mts .m2ts .ts .qt .yuv .rmvb .viv .asf .amv .m4p .m4v .mp2 .mpe .mpv .svi .3g2 .mxf .roq .nsv .f4v .f4p .f4a .f4b .asx .avchd .m2v .wma .wm .wax .wvx .wmx .wpl .dvr-ms .wmd .m1v .mpa .m3u .ivf .m4a .mp4 .mp4v .3gp2 .3gpp".split())
SUFFIX_NON_TEXT = SUFFIX_NON_TEXT_IMAGES | SUFFIX_NON_TEXT_MEDIA
SUFFIX_AUDIO = set(".ogg .mp2 .mpa .m4a .wma .f4a .f4b".split())

OTHER_BINARY_EXTENSIONS = {
    ".7z", ".ace", ".bin", ".bz2", ".cab", ".class", ".dmg", ".doc", ".docx", ".eot",
    ".exe", ".gz", ".iso", ".jar", ".otf", ".pdf", ".ppt", ".pptx", ".rar", ".tar",
    ".tgz", ".torrent", ".ttf", ".woff", ".woff2", ".xls", ".xlsx", ".zip",
}

TEXT_MIME_EXACT = {
    "application/ecmascript", "application/javascript", "application/json",
    "application/ld+json", "application/rss+xml", "application/atom+xml",
    "application/xhtml+xml", "application/xml", "text/javascript",
}
MEDIA_DESCRIPTOR_MIMES = {
    "application/dash+xml", "application/vnd.apple.mpegurl", "application/x-mpegurl",
    "application/smil+xml", "audio/mpegurl", "audio/x-mpegurl", "text/vtt",
}


@dataclass(frozen=True, slots=True)
class ResourceDecision:
    resource_class: str
    reason: str
    confident: bool


def normalized_mime(value: str | None) -> str:
    return str(value or "").split(";", 1)[0].strip().casefold()


def path_extension(url: str) -> str:
    try:
        path = urllib.parse.urlsplit(str(url or "")).path
    except (ValueError, TypeError, UnicodeError):
        return ""
    name = Path(urllib.parse.unquote(path)).name.casefold()
    # Legacy archives use filename.wmv;session and filename.wmv&token in
    # the URL *path*; query strings are never used to determine routing.
    suffix = Path(re.split(r"[;&]", name, maxsplit=1)[0]).suffix.casefold()
    return suffix if suffix.startswith(".") else ""


def _mime_class(mime: str) -> str | None:
    if not mime or mime in {"application/octet-stream", "application/binary", "binary/octet-stream"}:
        return None
    if mime in MEDIA_DESCRIPTOR_MIMES:
        return "media_descriptor"
    if mime == "image/svg+xml" or mime.startswith("image/"):
        return "image"
    if mime.startswith("video/") or "shockwave-flash" in mime:
        return "video"
    if mime.startswith("audio/"):
        return "audio"
    if mime.startswith(("font/", "application/font", "application/vnd.ms-font")):
        return "other_binary"
    if any(token in mime for token in ("pdf", "msword", "officedocument", "zip", "rar", "gzip", "7z", "octet-stream")):
        return "other_binary"
    if (
        mime.startswith("text/")
        or mime in TEXT_MIME_EXACT
        or mime.endswith("+json")
        or (mime.endswith("+xml") and mime != "image/svg+xml")
    ):
        return "text"
    return None


def _extension_class(ext: str) -> str | None:
    if not ext:
        return None
    if ext in MEDIA_DESCRIPTOR_EXTENSIONS:
        return "media_descriptor"
    if ext in IMAGE_EXTENSIONS:
        return "image"
    if ext in VIDEO_EXTENSIONS:
        return "video"
    if ext in AUDIO_EXTENSIONS:
        return "audio"
    if ext in OTHER_BINARY_EXTENSIONS:
        return "other_binary"
    if ext in TEXT_EXTENSIONS:
        return "text"
    return None



def suffix_non_text_decision(url: str) -> ResourceDecision | None:
    """Explicit suffix policy, independent of misleading CDX/replay MIME."""
    ext = path_extension(url)
    if ext not in SUFFIX_NON_TEXT:
        return None
    if ext in MEDIA_DESCRIPTOR_EXTENSIONS:
        kind = "media_descriptor"
    elif ext == ".xxx":
        kind = "other_binary"  # uncertain actual format, still excluded from text
    elif ext in SUFFIX_NON_TEXT_IMAGES:
        kind = "image"
    elif ext in SUFFIX_AUDIO:
        kind = "audio"
    else:
        kind = "video"  # routing group, not proof of video codec
    return ResourceDecision(kind, f"suffix_policy:{ext}", True)


def classify_indexed_resource(url: str, mimetype: str | None) -> ResourceDecision:
    """Classify CDX metadata without pretending metadata can prove payload bytes.

    Matching MIME/extension evidence is confident. A single strong MIME signal is
    confident when the URL has no contradictory known suffix. Conflicts and weak
    metadata intentionally remain unknown so the replay prefix validator can decide.
    """
    policy = suffix_non_text_decision(url)
    if policy is not None:
        return policy
    mime = normalized_mime(mimetype)
    ext = path_extension(url)
    mime_kind = _mime_class(mime)
    ext_kind = _extension_class(ext)

    if mime_kind and ext_kind:
        # Legacy ASX/RAM playlist suffixes describe media-related text even when
        # historical servers used broad ASF/RealAudio MIME labels for them.
        if ext_kind == "media_descriptor" and ext in {".asx", ".ram"}:
            return ResourceDecision("media_descriptor", f"descriptor_extension:{mime}|{ext}", True)
        if mime_kind == ext_kind:
            return ResourceDecision(mime_kind, f"mime+extension:{mime}|{ext}", True)
        # SVG is image policy even though XML is text-shaped.
        if ext == ".svg" and mime_kind in {"image", "text"}:
            return ResourceDecision("image", f"svg_policy:{mime}|{ext}", True)
        return ResourceDecision("unknown", f"conflict:{mime_kind}/{ext_kind}:{mime}|{ext}", False)
    if mime_kind:
        return ResourceDecision(mime_kind, f"mime:{mime}", True)
    if ext_kind:
        # A suffix alone is useful but can be misleading on historical dynamic sites.
        if ext_kind == "text":
            return ResourceDecision("text", f"extension:{ext}", False)
        return ResourceDecision(ext_kind, f"extension:{ext}", False)
    return ResourceDecision("unknown", "insufficient_metadata", False)




def classify_payload_kind(data: bytes, content_type: str, original_url: str) -> ResourceDecision:
    """Cheap structural check; a misleading text MIME cannot override magic bytes."""
    policy = suffix_non_text_decision(original_url)
    if policy is not None:
        return policy
    sample = data[:65536]
    if sample.startswith((b"\x89PNG\r\n\x1a\n", b"\xff\xd8\xff", b"GIF87a", b"GIF89a")):
        return ResourceDecision("image", "payload:image_magic", True)
    if len(sample) >= 12 and sample.startswith(b"RIFF"):
        kind = {b"WEBP":"image",b"AVI ":"video",b"WAVE":"audio"}.get(sample[8:12])
        if kind: return ResourceDecision(kind, "payload:riff", True)
    if len(sample) >= 12 and sample[4:8] == b"ftyp":
        brand = sample[8:12].lower()
        kind = "image" if brand in {b"avif",b"avis",b"heic",b"heix",b"mif1"} else "video"
        return ResourceDecision(kind, "payload:iso_media", True)
    if sample.startswith((b"%PDF-", b"PK\x03\x04", b"\x1f\x8b\x08")):
        return ResourceDecision("other_binary", "payload:binary_container", True)
    if sample.startswith((b"fLaC",b"OggS\x00",b"ID3\x03",b"ID3\x04")):
        return ResourceDecision("audio", "payload:audio_magic", True)
    if sample.startswith((b"\x1a\x45\xdf\xa3",b"FLV\x01")):
        return ResourceDecision("video", "payload:video_magic", True)
    from .content import looks_textual_bytes
    if normalized_mime(content_type) == "image/svg+xml" and (b"<svg" in sample[:4096].lower()):
        return ResourceDecision("image", "payload:svg", True)
    # Evidence from actual bytes outranks an incorrect archived MIME label.
    # Binary signatures above are definitive, but ordinary valid HTML can be
    # mislabeled as image/jpeg, and should still enter text scanning.
    if not looks_textual_bytes(data, "text/plain"):
        return ResourceDecision("other_binary", "payload:non_text", True)
    metadata = classify_indexed_resource(original_url, content_type)
    if metadata.resource_class == "media_descriptor":
        return metadata
    return ResourceDecision("text", "payload:validated_text", True)
