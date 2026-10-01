from __future__ import annotations

import bisect
import re
from collections import Counter, deque

from ..constants import ARCHIVE_EXTENSIONS, MEDIA_EXTENSIONS
from ..content import safe_urlsplit
from .keywords import CompiledRule, KeywordPrefilter, keyword_url_match
from .normalization import normalize_search
from .snippets import make_snippets

SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+|[\r\n]+")
PARAGRAPH_SPLIT = re.compile(r"(?:\r?\n){2,}|</p\s*>|<br\s*/?>\s*<br\s*/?>", re.IGNORECASE)
WORD_PATTERN = re.compile(r"\S+")


def link_is_interesting(
    link: str,
    patterns: list[CompiledRule],
    prefilter: KeywordPrefilter | None = None,
) -> bool:
    parsed = safe_urlsplit(link)
    if parsed:
        filename = parsed.path.rsplit("/", 1)[-1]
        dot = filename.rfind(".")
        extension = filename[dot:].lower() if dot > 0 else ""
    else:
        extension = ""
    if extension in MEDIA_EXTENSIONS or extension in ARCHIVE_EXTENSIONS:
        return True
    if prefilter is not None:
        if not prefilter.has_positive_rules:
            return False
        normalized = normalize_search(link)
        return prefilter.matches({"url": link}, {"url": normalized})
    return keyword_url_match(link, patterns)


def _matches(item: CompiledRule, value: str, normalized_value: str):
    """Yield regex matches without materializing a per-rule list."""
    haystack = value if item.rule.case_sensitive else normalized_value
    return item.pattern.finditer(haystack)


def _match_count(item: CompiledRule, value: str, normalized_value: str) -> int:
    return sum(1 for _ in _matches(item, value, normalized_value))


def _literal_starts(text: str, expression: str):
    """Yield non-overlapping literal offsets without regex match objects."""
    offset = text.find(expression)
    while offset >= 0:
        yield offset
        offset = text.find(expression, offset + len(expression))


def _non_overlapping_count(spans: list[tuple[int, int]]) -> int:
    """Match Python regex finditer semantics for one literal pattern."""
    count = 0
    last_end = -1
    for start, end in sorted(spans):
        if start >= last_end:
            count += 1
            last_end = end
    return count


def _compact_markup_text(raw: str) -> str:
    # Retain the existing compact view for tag-split phrases. The separate full
    # source view is still searched, including comments and script contents.
    # Never repeatedly search an unterminated suffix containing thousands of
    # '<' characters. Only the prefix ending in the last '>' can contain tags.
    raw = raw or ""
    last_close = raw.rfind(">")
    if last_close >= 0 and "<" in raw:
        raw = re.sub(r"(?s)<[^>]*>", " ", raw[:last_close + 1]) + raw[last_close + 1:]
    return " ".join(raw.split())


def _matched_labels_in_segments(text: str, patterns: list[CompiledRule], splitter: re.Pattern[str]) -> int:
    bonus = 0
    # Repeated navigation/boilerplate need not be normalized and checked again.
    # The cache is per document, bounded by both entry count and segment size.
    cached: dict[str, int] = {}
    simple = [
        (item, normalize_search(item.rule.expression))
        for item in patterns if item.rule.kind != "excluded"
    ]
    for segment in splitter.split(text):
        if not segment.strip():
            continue
        previous = cached.get(segment)
        if previous is not None:
            bonus += previous
            continue
        normalized_segment = normalize_search(segment)
        ascii_segment = normalized_segment.isascii()
        labels = set()
        for item, expression in simple:
            rule = item.rule
            if (ascii_segment and expression.isascii() and rule.kind != "regex"
                    and not rule.case_sensitive and not rule.whole_word):
                matched = expression in normalized_segment
            else:
                matched = item.pattern.search(segment if rule.case_sensitive else normalized_segment)
            if matched:
                labels.add(rule.label)
        value = len(labels) * (len(labels) - 1)
        bonus += value
        if len(cached) < 256 and len(segment) <= 4096:
            cached[segment] = value
    return bonus


def _proximity_bonus(
    text: str, patterns: list[CompiledRule], window_words: int = 25,
    *, normalized: str | None = None,
) -> tuple[int, dict]:
    if normalized is None:
        normalized = normalize_search(text)
    starts = [span.start() for span in WORD_PATTERN.finditer(normalized)]
    if not starts:
        return 0, {"window_words": window_words, "pairs": 0}
    positions: dict[str, list[int]] = {}
    ascii_body = normalized.isascii()
    for item in patterns:
        if item.rule.kind == "excluded":
            continue
        bucket = positions.setdefault(item.rule.label, [])
        expression = normalize_search(item.rule.expression)
        if (ascii_body and expression and expression.isascii() and item.rule.kind != "regex"
                and not item.rule.case_sensitive and not item.rule.whole_word):
            offsets = _literal_starts(normalized, expression)
        else:
            offsets = (match.start() for match in item.pattern.finditer(normalized))
        for offset in offsets:
            position = max(0, bisect.bisect_right(starts, offset) - 1)
            # Proximity depends on word positions, not on how often one rule
            # matches within a word (e.g. overlapping or zero-width regexes).
            if not bucket or bucket[-1] != position:
                bucket.append(position)
    minimum_distance: int | None = None
    # One sliding word window replaces a whole-document two-pointer walk for
    # every label pair. Integer masks record which pairs were ever neighbors;
    # repeated occurrences cannot inflate the pair count.
    values = list(positions.values())
    label_count = len(values)
    ordered = sorted((position, index) for index, bucket in enumerate(values) for position in bucket)
    neighbors = [0] * label_count
    active_counts = [0] * label_count
    active = 0
    window: deque[tuple[int, int]] = deque()
    previous: tuple[int, int] | None = None
    if label_count >= 2:
        minimum_distance = len(starts)
    for position, index in ordered:
        if previous is not None and previous[1] != index:
            minimum_distance = min(minimum_distance, position - previous[0])
        previous = (position, index)
        while window and window[0][0] < position - window_words:
            _, expired = window.popleft()
            active_counts[expired] -= 1
            if not active_counts[expired]:
                active &= ~(1 << expired)
        bit = 1 << index
        neighbors[index] |= active & ~bit
        active |= bit
        active_counts[index] += 1
        window.append((position, index))
    close_pairs = 0
    for index, mask in enumerate(neighbors):
        # Count each unordered pair once, regardless of which label occurred
        # first at different places in the document.
        close_pairs += (mask & ((1 << index) - 1)).bit_count()
        higher = mask >> (index + 1)
        while higher:
            low_bit = higher & -higher
            other = index + low_bit.bit_length()
            if not neighbors[other] & (1 << index):
                close_pairs += 1
            higher ^= low_bit
    # Preserve the established scoring rule for labels matched only in other
    # fields: their empty-body pair distance is the body word count.
    if len(starts) <= window_words:
        empty = sum(not bucket for bucket in values)
        close_pairs += empty * (label_count - empty) + empty * (empty - 1) // 2
    return close_pairs * 6, {
        "window_words": window_words,
        "pairs": close_pairs,
        "minimum_distance": minimum_distance,
    }


def prepare_analysis_fields(
    original: str,
    title: str,
    visible: str,
    raw: str,
    links: list[str],
) -> tuple[dict[str, str], dict[str, str]]:
    fields = {
        "url": original,
        "title": title,
        "body": visible,
        # An earlier development scanner truncated source at 500,000 characters. Maximum-recall scans
        # must search the complete archived response.
        "source": raw,
        "compact": _compact_markup_text(raw),
        "links": "\n".join(links),
    }
    normalized_fields: dict[str, str] = {}
    by_text: dict[str, str] = {}
    for name, value in fields.items():
        normalized = by_text.get(value)
        if normalized is None:
            normalized = normalize_search(value)
            by_text[value] = normalized
        normalized_fields[name] = normalized
    return fields, normalized_fields

def analyze_content(
    original: str,
    title: str,
    visible: str,
    raw: str,
    links: list[str],
    patterns: list[CompiledRule],
    prefilter: KeywordPrefilter | None = None,
    prepared_fields: dict[str, str] | None = None,
    prepared_normalized_fields: dict[str, str] | None = None,
    *,
    include_hit_fields: bool = True,
    include_snippets: bool = True,
    include_interesting_links: bool = True,
) -> dict:
    if prepared_fields is None or prepared_normalized_fields is None:
        fields, normalized_fields = prepare_analysis_fields(original, title, visible, raw, links)
    else:
        fields = prepared_fields
        normalized_fields = prepared_normalized_fields
    multipliers = {"url": 6.0, "title": 5.0, "body": 1.0, "source": 0.75, "compact": 0.9, "links": 2.5}
    hits: Counter[str] = Counter()
    hit_fields: dict[str, set[str]] = {}
    score = 0.0
    matched_rules: dict[str, CompiledRule] = {}
    excluded_labels: set[str] = set()
    required_labels = {item.rule.label for item in patterns if item.rule.kind == "required"}

    # Ordinary normalized literals are counted directly from one Aho-Corasick
    # traversal per field.  Regex/case-sensitive/whole-word rules retain the
    # exact legacy regex path.
    literal_map = prefilter.literal_rules if prefilter is not None else {}
    literal_auto = prefilter.candidate_automaton if prefilter is not None else None
    slow_patterns = prefilter.slow_patterns if prefilter is not None else patterns
    positive_found = not any(item.rule.kind != "excluded" for item in patterns)
    literal_counts_cache: dict[str, dict[str, int]] = {}
    slow_counts_cache: dict[tuple[int, str], int] = {}

    for field_name, value in fields.items():
        normalized_value = normalized_fields[field_name]
        if literal_auto is not None:
            literal_counts = literal_counts_cache.get(normalized_value)
            if literal_counts is None:
                literal_counts = literal_auto.count_non_overlapping(normalized_value)
                literal_counts_cache[normalized_value] = literal_counts
            for expression, count in literal_counts.items():
                for item in literal_map.get(expression, ()):
                    label = item.rule.label
                    hits[label] += count
                    if include_hit_fields:
                        hit_fields.setdefault(label, set()).add(field_name)
                    matched_rules[label] = item
                    if item.rule.kind == "excluded":
                        excluded_labels.add(label)
                        continue
                    positive_found = True
                    exact_bonus = 2.0 if item.rule.kind == "exact" else 1.0
                    score += min(count, 10) * multipliers[field_name] * item.rule.weight * exact_bonus
        for index, item in enumerate(slow_patterns):
            key = (index, value if item.rule.case_sensitive else normalized_value)
            count = slow_counts_cache.get(key)
            if count is None:
                count = _match_count(item, value, normalized_value)
                slow_counts_cache[key] = count
            if not count:
                continue
            label = item.rule.label
            hits[label] += count
            if include_hit_fields:
                hit_fields.setdefault(label, set()).add(field_name)
            matched_rules[label] = item
            if item.rule.kind == "excluded":
                excluded_labels.add(label)
                continue
            positive_found = True
            exact_bonus = 2.0 if item.rule.kind == "exact" else 1.0
            score += min(count, 10) * multipliers[field_name] * item.rule.weight * exact_bonus

    if not positive_found and any(item.rule.kind != "excluded" for item in patterns):
        return {
            "score": 0, "hits": dict(sorted(hits.items())),
            "hit_fields": {key: sorted(value) for key, value in hit_fields.items()},
            "snippets": [], "interesting_links": [],
            "excluded": bool(excluded_labels), "excluded_labels": sorted(excluded_labels),
            "required_missing": bool(required_labels),
            "missing_required_labels": sorted(required_labels),
            "proximity": {"window_words": 25, "pairs": 0, "minimum_distance": None, "sentence_bonus": 0, "paragraph_bonus": 0, "score_bonus": 0},
        }

    missing_required = sorted(required_labels - set(hits))
    excluded = bool(excluded_labels)
    distinct = len([label for label in hits if label not in excluded_labels])
    if distinct >= 2:
        score += distinct * 3

    positive_matched_rules = [item for item in matched_rules.values() if item.rule.kind != "excluded"]
    # Sentence, paragraph, and word-distance bonuses require at least two
    # positive labels by definition. Most archive hits contain only one keyword;
    # skipping three whole-document passes in that common case is a substantial
    # scan-speed win with identical scores.
    if len(positive_matched_rules) >= 2:
        sentence_bonus = _matched_labels_in_segments(visible, positive_matched_rules, SENTENCE_SPLIT) * 4
        paragraph_bonus = _matched_labels_in_segments(raw, positive_matched_rules, PARAGRAPH_SPLIT) * 2
        proximity_bonus, proximity = _proximity_bonus(
            visible, positive_matched_rules, normalized=normalized_fields["body"]
        )
    else:
        sentence_bonus = paragraph_bonus = proximity_bonus = 0
        proximity = {"window_words": 25, "pairs": 0, "minimum_distance": None}
    score += sentence_bonus + paragraph_bonus + proximity_bonus
    proximity.update({"sentence_bonus": sentence_bonus, "paragraph_bonus": paragraph_bonus, "score_bonus": proximity_bonus})

    if excluded or missing_required:
        score = 0
    interesting_links = (
        sorted({link for link in links if link_is_interesting(link, patterns, prefilter)})
        if include_interesting_links
        else []
    )
    snippets = (
        make_snippets(
            visible or raw, positive_matched_rules,
            normalized=normalized_fields["body" if visible else "source"],
        )
        if include_snippets and positive_matched_rules
        else []
    )
    return {
        "score": int(round(score)),
        "hits": dict(sorted(hits.items())),
        "hit_fields": {key: sorted(value) for key, value in hit_fields.items()},
        "snippets": snippets,
        "interesting_links": interesting_links,
        "excluded": excluded,
        "excluded_labels": sorted(excluded_labels),
        "required_missing": bool(missing_required),
        "missing_required_labels": missing_required,
        "proximity": proximity,
    }
