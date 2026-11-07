from __future__ import annotations

from dataclasses import dataclass

from .keywords import CompiledRule, KeywordPrefilter, compile_keywords, compile_prefilter


@dataclass(slots=True)
class ScanJob:
    scan_run_id: int
    keyword_set_name: str
    rules: list[str]
    patterns: list[CompiledRule]
    prefilter: KeywordPrefilter
    store_snippets: bool = True

    @classmethod
    def create(cls, scan_run_id: int, keyword_set_name: str, rules: list[str], *, store_snippets: bool = True) -> "ScanJob":
        patterns = compile_keywords(rules)
        return cls(scan_run_id, keyword_set_name, list(rules), patterns, compile_prefilter(patterns), store_snippets)
