from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass
from pathlib import PurePosixPath

from ..content import URL_PATTERN
from ..utils import clean_space

DATE_RE = re.compile(r"\b(?:19|20)\d{2}[-/.](?:0?[1-9]|1[0-2])[-/.](?:0?[1-9]|[12]\d|3[01])\b")
HASH_RE = re.compile(r"\b(?:[A-Fa-f0-9]{32}|[A-Fa-f0-9]{40}|[A-Fa-f0-9]{64})\b")
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
USERNAME_RE = re.compile(r"(?<!\w)(?:u/|@)([A-Za-z0-9_.-]{2,40})\b")
FILE_RE = re.compile(r"(?<![\w/])([A-Za-z0-9_.()\[\]-]{2,180}\.[A-Za-z0-9]{1,8})(?!\w)")


@dataclass(frozen=True, slots=True)
class Entity:
    kind: str
    value: str
    normalized: str


def _url_entities(url: str) -> list[Entity]:
    output: list[Entity] = []
    try:
        parsed = urllib.parse.urlsplit(url if "://" in url else "http://" + url)
    except ValueError:
        return output
    host = (parsed.hostname or "").casefold().strip(".")
    if host:
        output.append(Entity("domain", host, host))
    name = PurePosixPath(parsed.path).name
    if name and "." in name:
        output.append(Entity("filename", name, name.casefold()))
    return output


def extract_entities(title: str, body: str, original_url: str, links: list[str] | None = None, limit: int = 500) -> list[Entity]:
    text = f"{title}\n{body}"
    entities: dict[tuple[str, str], Entity] = {}

    def add(kind: str, value: str, normalized: str | None = None) -> None:
        value = clean_space(value)[:500]
        normalized_value = clean_space(normalized if normalized is not None else value).casefold()[:500]
        if not value or not normalized_value:
            return
        entities.setdefault((kind, normalized_value), Entity(kind, value, normalized_value))

    for item in _url_entities(original_url):
        add(item.kind, item.value, item.normalized)
    for url in (links or [])[:300]:
        for item in _url_entities(url):
            add(item.kind, item.value, item.normalized)
    for value in URL_PATTERN.findall(text[:500_000]):
        add("url", value, value.casefold())
        for item in _url_entities(value):
            add(item.kind, item.value, item.normalized)
    for value in DATE_RE.findall(text[:500_000]):
        add("date", value)
    for value in HASH_RE.findall(text[:500_000]):
        add("hash", value, value.casefold())
    for value in EMAIL_RE.findall(text[:500_000]):
        add("email", value, value.casefold())
    for value in USERNAME_RE.findall(text[:500_000]):
        add("username", value, value.casefold())
    for value in FILE_RE.findall(text[:500_000]):
        add("filename", value, value.casefold())
    return list(entities.values())[: max(10, int(limit))]
