from __future__ import annotations

import urllib.parse

SITE_ISSUE_LABELS = {
    "wayback_excluded": "Wayback exclusion",
    "robots_blocked": "robots.txt restriction",
    "missing_capture": "capture unavailable",
    "origin_unavailable": "archived origin unavailable",
    "invalid_wayback_replay": "invalid Wayback replay",
    "wayback_forbidden": "Wayback access restriction",
    "http_client_error": "Wayback request rejected",
    "http_server_error": "Wayback service error",
    "rate_limit": "Wayback rate limit",
    "timeout": "Wayback timeout",
    "connection": "Wayback connection problem",
    "ssl": "TLS/certificate problem",
}

PERMANENT_SITE_ISSUES = {
    "wayback_excluded",
    "robots_blocked",
    "missing_capture",
    "invalid_wayback_replay",
    "wayback_forbidden",
}


def host_from_url(url: str) -> str:
    try:
        parsed = urllib.parse.urlsplit(str(url or ""))
    except (TypeError, ValueError, UnicodeError):
        return "unknown"
    return (parsed.hostname or "unknown").casefold()


def site_issue_message(category: str, url: str, stage: str, status: int | None = None) -> str:
    label = SITE_ISSUE_LABELS.get(category, category.replace("_", " "))
    host = host_from_url(url)
    status_text = f" (HTTP {int(status)})" if status else ""
    if category == "wayback_excluded":
        detail = "The Internet Archive reports that this material is excluded from Wayback access."
    elif category == "robots_blocked":
        detail = "Wayback is refusing this replay because of a robots.txt restriction recorded for the site."
    elif category == "wayback_forbidden":
        detail = "Wayback rejected this index request. Some broad CDX queries require different access or a narrower target."
    elif category == "missing_capture":
        detail = "The requested capture is not available from Wayback."
    elif category == "origin_unavailable":
        detail = "Wayback returned an archived error page indicating the original serving machine was unavailable."
    elif category == "invalid_wayback_replay":
        detail = "Wayback redirected to a non-replay page instead of the requested archived capture."
    elif category == "rate_limit":
        detail = "Wayback is rate-limiting requests; Archive Scout will preserve work and retry according to the configured policy."
    elif category in {"timeout", "connection", "http_server_error", "ssl"}:
        detail = "This appears to be a transient access problem and can be retried later."
    else:
        detail = "Wayback rejected or could not serve this request."
    return f"{host}: {label}{status_text} during {stage}. {detail}"


def should_surface_site_issue(category: str) -> bool:
    return category in SITE_ISSUE_LABELS
