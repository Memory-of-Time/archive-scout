from __future__ import annotations

import re
import sqlite3
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .constants import AUDIO_EXTENSIONS, IMAGE_EXTENSIONS, TEXT_EXTENSIONS, VIDEO_EXTENSIONS
from .events import Stopped
from .resource_detection import structural_payload_class
from .utils import utc_now

RESOURCE_CLASSIFIER_REVISION = 3
PREVIEW_BUDGET = 64 * 1024

MEDIA_DESCRIPTOR_EXTENSIONS = {".asx", ".m3u", ".m3u8", ".mpd", ".pls", ".ram", ".smil", ".smi", ".vtt", ".srt"}
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
    suffix = Path(name).suffix.casefold()
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



def capture_routing_decision(
    resource_class: str | None,
    state: str | None,
    skip_reason: str | None = None,
    payload_availability: str | None = None,
) -> str:
    """Return one stable, user-facing routing/disposition label for a capture.

    Resource class describes what Archive Scout believes the archived response is;
    routing describes what the text/media pipeline actually did with that capture.
    Keep these dimensions separate so dashboards/reports never infer coverage from
    a single ``state`` value.
    """
    kind = str(resource_class or "unknown").strip().casefold() or "unknown"
    state_value = str(state or "pending").strip().casefold() or "pending"
    reason = str(skip_reason or "").strip().casefold()
    availability = str(payload_availability or "not_acquired").strip().casefold()

    if state_value == "error":
        return "failed"
    if state_value == "downloading":
        return "downloading"
    if state_value == "scanning":
        return "scanning"
    if availability == "partial":
        return "partial"
    if availability == "discarded":
        return "scanned_discarded"
    if state_value in {"downloaded", "downloaded_unscanned"} or availability in {
        "retained", "retained_unscanned", "spooled_unscanned", "cleanup_pending",
    }:
        return "downloaded" if state_value == "downloaded" else "downloaded_awaiting_scan"
    if reason in {"deferred_to_media", "payload_validation_deferred"}:
        return "deferred_to_media"
    if reason == "classified_media_descriptor":
        return "media_descriptor_excluded"
    if reason in {"known_non_text", "sniffed_non_text", "unsupported_binary", "classified_media"}:
        return "skipped_non_text"
    if reason == "url_keyword_filter":
        return "skipped_url_filter"
    if state_value == "skipped":
        return "skipped_other"
    if kind in {"image", "video", "audio", "media_descriptor", "other_binary"} and state_value == "pending":
        return "classified_not_routed"
    if kind == "unknown":
        return "awaiting_classification" if state_value == "pending" else state_value
    return state_value


def capture_body_coverage(
    resource_class: str | None,
    state: str | None,
    payload_availability: str | None,
) -> str:
    """Describe whether the capture body is actually available for local searching."""
    kind = str(resource_class or "unknown").strip().casefold() or "unknown"
    state_value = str(state or "pending").strip().casefold() or "pending"
    availability = str(payload_availability or "not_acquired").strip().casefold()
    if kind in {"image", "video", "audio", "other_binary"}:
        return "non_text"
    if availability == "discarded":
        return "discarded"
    if availability == "partial":
        return "partial"
    if availability in {"retained", "retained_unscanned", "spooled_unscanned", "cleanup_pending"}:
        return "body_available"
    if state_value in {"downloaded", "downloaded_unscanned", "scanning"}:
        return "body_available"
    if state_value == "error":
        return "unavailable_failed"
    return "url_only"

def classify_indexed_resource(url: str, mimetype: str | None) -> ResourceDecision:
    """Classify CDX metadata without pretending metadata can prove payload bytes.

    Matching MIME/extension evidence is confident. A single strong MIME signal is
    confident when the URL has no contradictory known suffix. Conflicts and weak
    metadata intentionally remain unknown so the replay prefix validator can decide.
    """
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


def payload_signature_class(data: bytes) -> str | None:
    """Compatibility wrapper around the shared structural byte policy."""
    kind, _reason = structural_payload_class(data)
    return kind if kind not in {"text", None} else None


def classify_payload_prefix(data: bytes, content_type: str, original_url: str) -> ResourceDecision:
    structural, structural_reason = structural_payload_class(data[:PREVIEW_BUDGET], content_type)
    if structural is not None:
        return ResourceDecision(structural, f"payload:{structural_reason}", True)

    meta = classify_indexed_resource(original_url, content_type)
    if meta.resource_class in {"image", "video", "audio", "other_binary", "media_descriptor"} and meta.confident:
        return meta
    sample = data[: min(len(data), PREVIEW_BUDGET)]
    if not sample:
        return ResourceDecision("unknown", "empty_prefix", False)
    if b"\x00" in sample:
        return ResourceDecision("other_binary", "nul_bytes_without_supported_text_encoding", True)
    controls = sum(byte < 9 or 13 < byte < 32 for byte in sample)
    if controls / max(1, len(sample)) >= 0.05:
        return ResourceDecision("other_binary", "binary_control_ratio", True)
    if meta.resource_class == "media_descriptor":
        return meta
    return ResourceDecision("text", "printable_payload_prefix", True)


def classify_capture_inventory(
    database: sqlite3.Connection,
    query_signature: str,
    *,
    allow_media_descriptors_as_text: bool = False,
    batch_size: int = 5000,
    stop_event=None,
    progress_callback=None,
) -> dict[str, int]:
    """Migrate stale metadata classifications in cancellable ID-keyset batches.

    Fresh CDX rows are classified during their existing insert transaction, so
    ordinary acquisition does not need a second project-sized preparation pass.
    The counts returned here describe rows updated by this migration pass only;
    callers that do not require exact aggregate totals avoid an extra GROUP BY.
    """
    counts = {name: 0 for name in ("text", "image", "video", "audio", "media_descriptor", "other_binary", "unknown")}
    last_id = 0
    processed = 0
    while True:
        if stop_event is not None and stop_event.is_set():
            raise Stopped
        rows = database.execute(
            """SELECT id,original_url,mimetype,state,skip_reason
               FROM captures INDEXED BY captures_classification_idx
               WHERE query_signature=? AND id>? AND resource_classifier_revision<?
               ORDER BY id LIMIT ?""",
            (query_signature, last_id, RESOURCE_CLASSIFIER_REVISION, max(1, int(batch_size))),
        ).fetchall()
        if not rows:
            break
        now = utc_now()
        updates: list[tuple[str, str, int, str | None, str | None, str, int]] = []
        for row in rows:
            last_id = int(row["id"])
            decision = classify_indexed_resource(str(row["original_url"]), str(row["mimetype"] or ""))
            resource_class = decision.resource_class if decision.confident else "unknown"
            reason = decision.reason if decision.confident else f"unresolved:{decision.reason}"
            route_text = resource_class == "text" or (resource_class == "media_descriptor" and allow_media_descriptors_as_text)
            state = str(row["state"] or "pending")
            skip_reason = row["skip_reason"]
            if state in {"pending", "skipped"}:
                if resource_class in {"image", "video", "audio"} and decision.confident:
                    state, skip_reason = "skipped", "classified_media"
                elif resource_class == "other_binary" and decision.confident:
                    state, skip_reason = "skipped", "unsupported_binary"
                elif resource_class == "media_descriptor" and decision.confident and not allow_media_descriptors_as_text:
                    state, skip_reason = "skipped", "classified_media_descriptor"
                elif route_text or resource_class == "unknown" or not decision.confident:
                    if skip_reason in {"classified_media", "unsupported_binary", "classified_media_descriptor", "known_non_text"}:
                        state, skip_reason = "pending", None
            counts[resource_class] += 1
            updates.append((resource_class, reason, RESOURCE_CLASSIFIER_REVISION, state, skip_reason, now, int(row["id"])))
        with database:
            database.executemany(
                """UPDATE captures SET resource_class=?,classification_reason=?,resource_classifier_revision=?,
                   state=?,skip_reason=?,updated_at=? WHERE id=?""",
                updates,
            )
        processed += len(updates)
        if progress_callback is not None:
            progress_callback(processed)
    return counts

