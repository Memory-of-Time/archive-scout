from __future__ import annotations

from heapq import merge
from itertools import islice
from typing import Iterator

from ..utils import clean_space, normalize_search
from .keywords import CompiledRule


def _match_spans(item: CompiledRule, text: str, offset: int) -> Iterator[tuple[int, int]]:
    for match in item.pattern.finditer(text):
        yield match.start() + offset, match.end() + offset


def _contexts(display: str, spans: Iterator[tuple[int, int]], radius: int) -> Iterator[str]:
    start = end = None
    for left, right in spans:
        left = max(0, left - radius)
        right = min(len(display), right + radius)
        if start is not None and left > end:
            yield _excerpt(display, start, end)
            start = None
        if start is None:
            start, end = left, right
        else:
            end = max(end, right)
    if start is not None:
        yield _excerpt(display, start, end)


def _excerpt(text: str, start: int, end: int) -> str:
    return ("…" if start else "") + clean_space(text[start:end]) + ("…" if end < len(text) else "")


def field_snippets(original: str, by_text: dict[str, list[CompiledRule]], radius: int = 220) -> Iterator[str]:
    ascii_folded = original.strip().lower() if original.isascii() else None
    leading_space = len(original) - len(original.lstrip()) if ascii_folded is not None else 0
    original_spans = []
    normalized_spans = []
    for haystack, patterns in by_text.items():
        if haystack == original:
            original_spans.extend(_match_spans(item, haystack, 0) for item in patterns)
        elif haystack == ascii_folded:
            original_spans.extend(_match_spans(item, haystack, leading_space) for item in patterns)
        else:
            normalized_spans.append((haystack, merge(*(_match_spans(item, haystack, 0) for item in patterns))))
    yield from _contexts(original, merge(*original_spans), radius)
    for haystack, spans in normalized_spans:
        yield from _contexts(haystack, spans, radius)


def make_snippets(text: str, patterns: list[CompiledRule], limit: int | None = None, radius: int = 220) -> list[str]:
    normalized = normalize_search(text)
    by_text: dict[str, list[CompiledRule]] = {}
    for item in patterns:
        haystack = text if item.rule.case_sensitive else normalized
        by_text.setdefault(haystack, []).append(item)
        if item.rule.kind == "regex" and haystack != text:
            by_text.setdefault(text, []).append(item)
    snippets = field_snippets(text, by_text, radius)
    return list(snippets if limit is None else islice(snippets, max(0, limit)))
