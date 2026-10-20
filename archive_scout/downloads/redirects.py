from __future__ import annotations

import re
import urllib.parse

from ..config import ProjectConfig
from ..network.transports import RedirectPolicyError


def replay_original_url(url: str) -> str | None:
    """Return the embedded original URL from a Wayback replay URL."""
    try:
        parsed = urllib.parse.urlsplit(str(url))
    except ValueError:
        return None
    if parsed.hostname not in {"web.archive.org", "wwwb-app0.us.archive.org"}:
        return None
    match = re.match(r"^/web/[^/]+/(https?://.+)$", parsed.path + (("?" + parsed.query) if parsed.query else ""), re.I)
    if not match:
        return None
    return urllib.parse.unquote(match.group(1))


def _target_host_scope(config: ProjectConfig) -> list[tuple[str, bool]]:
    scopes: list[tuple[str, bool]] = []
    for target in config.targets:
        raw = str(target).strip().replace("*", "")
        if not raw:
            continue
        parsed = urllib.parse.urlsplit(raw if "://" in raw else "http://" + raw)
        host = (parsed.hostname or "").casefold().rstrip(".")
        if not host:
            continue
        settings = config.settings_for_target(target)
        match_type = str(settings.get("cdx_match_type") or config.cdx_match_type or "").casefold()
        scopes.append((host, match_type == "domain"))
    return scopes


def _host_in_project_scope(host: str, scopes: list[tuple[str, bool]]) -> bool:
    value = str(host or "").casefold().rstrip(".")
    for allowed, include_subdomains in scopes:
        if value == allowed or (include_subdomains and value.endswith("." + allowed)):
            return True
    return False


def make_replay_redirect_validator(config: ProjectConfig, source_original: str):
    """Return the shared archive-only redirect policy for one stored capture.

    The callback runs before the transport contacts each redirect destination.
    External is defined by the embedded original host, not by the common
    web.archive.org replay host. Live destinations are never silently attached
    to historical evidence in a saved historical capture.
    """
    normalized = config.normalized()
    scopes = _target_host_scope(normalized)
    source_host = (urllib.parse.urlsplit(source_original).hostname or "").casefold().rstrip(".")

    def validate(source_url: str, destination_url: str) -> None:
        if urllib.parse.urlsplit(destination_url).scheme.casefold() not in {"http", "https"}:
            raise RedirectPolicyError(source_url, destination_url, "live_redirect_blocked")
        embedded = replay_original_url(destination_url)
        if embedded is None:
            parsed = urllib.parse.urlsplit(destination_url)
            if parsed.scheme not in {"http", "https"} or parsed.hostname not in {"web.archive.org", "wwwb-app0.us.archive.org"}:
                raise RedirectPolicyError(source_url, destination_url, "live_redirect_blocked")
            # Archive-internal canonical/timestamp redirects without an embedded
            # original remain eligible; a later hop is checked again.
            return
        dest_host = (urllib.parse.urlsplit(embedded).hostname or "").casefold().rstrip(".")
        if dest_host == source_host or _host_in_project_scope(dest_host, scopes):
            return
        if normalized.download_external_redirects:
            return
        raise RedirectPolicyError(source_url, destination_url, "external_redirect_blocked")

    return validate


