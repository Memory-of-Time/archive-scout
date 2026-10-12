from __future__ import annotations

import re
import urllib.error



def classify_exception(exc: Exception) -> tuple[str, int | None, bool]:
    message = str(exc).casefold()
    status = getattr(exc, "status", None)
    explicit_category = str(getattr(exc, "category", "") or "")
    if explicit_category in {"wayback_excluded", "robots_blocked", "wayback_forbidden", "missing_capture", "http_client_error"}:
        return explicit_category, int(status) if status else None, False
    replay_categories = {
        "wayback_excluded": ("wayback_excluded", None, False),
        "robots_blocked": ("robots_blocked", None, False),
        "missing_capture": ("missing_capture", 404, False),
        "invalid_wayback_replay": ("invalid_wayback_replay", None, False),
        "origin_unavailable": ("origin_unavailable", None, True),
        "soft_404": ("missing_capture", 404, False),
    }
    if message in replay_categories:
        return replay_categories[message]
    if "excluded from the wayback machine" in message or "blocked site error" in message:
        return "wayback_excluded", 403, False
    if "robots.txt" in message:
        return "robots_blocked", 403, False
    if "429" in message or "rate limit" in message:
        return "rate_limit", 429, True
    match = re.search(r"http\s+(\d{3})", message)
    if match:
        status = int(match.group(1))
        if status == 404:
            return "missing_capture", status, False
        if 400 <= status < 500:
            return "http_client_error", status, status in {408, 425, 429}
        return "http_server_error", status, True
    if "timeout" in message or "timed out" in message:
        return "timeout", status, True
    if "ssl" in message or "certificate" in message:
        return "ssl", status, True
    if "exceeds" in message and "bytes" in message:
        return "oversized_response", status, False
    if "network failure" in message or "urlopen" in message or "connection" in message:
        return "connection", status, True
    return "unknown", status, True
