from __future__ import annotations

import errno
import re
import urllib.error

import httpx
import urllib3

from ..network.transports import PayloadValidationError, is_local_storage_error, is_transport_connection_failure, is_transport_read_timeout, is_transport_timeout
from ..text_encoding import TextDecodingError


def classify_exception(exc: Exception) -> tuple[str, int | None, bool]:
    """Classify failures from typed attributes first, message text last.

    Arbitrary digits in URLs/timestamps must never synthesize an HTTP status.
    Local filesystem failures are permanent operation problems, not network
    fallback candidates.
    """
    raw_status = getattr(exc, "status", None)
    try:
        status = int(raw_status) if raw_status is not None else None
    except (TypeError, ValueError):
        status = None
    explicit_category = str(getattr(exc, "category", "") or "").strip()
    message = str(exc)
    folded = message.casefold()

    if isinstance(exc, PayloadValidationError):
        return "payload_validation", status, True
    if isinstance(exc, TextDecodingError):
        return "content_encoding", status, True

    if explicit_category:
        permanent = {
            "wayback_excluded", "robots_blocked", "wayback_forbidden",
            "missing_capture", "http_client_error", "external_redirect_blocked",
            "live_redirect_blocked", "storage", "oversized_response",
        }
        if explicit_category in permanent:
            return explicit_category, status, False
        if explicit_category in {"rate_limit", "timeout", "connection", "protocol", "origin_unavailable", "http_server_error"}:
            return explicit_category, status, True

    if is_local_storage_error(exc):
        return "storage", status, False

    # Real typed status always wins over text. This is the only route that can
    # classify an exception as HTTP 429 without an explicit rate-limit category.
    if status is not None:
        if status == 429:
            return "rate_limit", status, True
        if status == 404:
            return "missing_capture", status, False
        if 400 <= status < 500:
            return "http_client_error", status, status in {408, 425, 429}
        if status >= 500:
            return "http_server_error", status, True

    replay_categories = {
        "wayback_excluded": ("wayback_excluded", None, False),
        "robots_blocked": ("robots_blocked", None, False),
        "missing_capture": ("missing_capture", 404, False),
        "invalid_wayback_replay": ("invalid_wayback_replay", None, False),
        "origin_unavailable": ("origin_unavailable", None, True),
        "soft_404": ("missing_capture", 404, False),
    }
    if folded in replay_categories:
        return replay_categories[folded]
    if "excluded from the wayback machine" in folded or "blocked site error" in folded:
        return "wayback_excluded", 403, False
    if "robots.txt" in folded:
        return "robots_blocked", 403, False
    if "rate limit" in folded:
        return "rate_limit", None, True

    # Message parsing requires an explicit HTTP token; timestamps such as
    # 20040429000000 therefore cannot be mistaken for status 429.
    match = re.search(r"\bhttp(?:/\d(?:\.\d)?)?\s+(\d{3})\b", folded)
    if match:
        parsed = int(match.group(1))
        if parsed == 429:
            return "rate_limit", parsed, True
        if parsed == 404:
            return "missing_capture", parsed, False
        if 400 <= parsed < 500:
            return "http_client_error", parsed, parsed in {408, 425, 429}
        return "http_server_error", parsed, True

    if is_transport_read_timeout(exc):
        return "timeout", None, True
    if is_transport_timeout(exc):
        return "connection_timeout" if is_transport_connection_failure(exc) else "timeout", None, True
    if isinstance(exc, httpx.RemoteProtocolError):
        return "protocol", None, True
    if is_transport_connection_failure(exc):
        return "connection", None, True
    if "ssl" in folded or "certificate" in folded:
        return "ssl", None, True
    if "exceeds" in folded and "bytes" in folded:
        return "oversized_response", None, False
    if "network failure" in folded or "urlopen" in folded:
        return "connection", None, True
    return "unknown", None, True
