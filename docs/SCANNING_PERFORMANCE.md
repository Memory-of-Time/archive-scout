# Scanning performance

Archive Scout 1.0.4 keeps deterministic evidence semantics while reducing avoidable local CPU and I/O work. The scanner reuses normalized/count data, bounds proximity-segment work, avoids repeated literal-count allocation, reuses already-saved files for local rescans, and handles malformed-markup tails without reintroducing a source-length truncation.

Scanning is local after acquisition. Increasing scanner workers can improve CPU-bound throughput on suitable hardware without increasing Wayback request volume. Network pacing is controlled independently by the index and replay request-attempt pools described in `NETWORK_PERFORMANCE.md`.

The regression suite includes mixed literal/regex/whole-word/case-sensitive rules, required and excluded terms, Unicode and escaped content, duplicate labels, report enrichment, malformed markup, retained-source rescans, and deterministic result comparisons.

Use `scripts/benchmark_scanning.py` and `scripts/benchmark_scan_pipeline.py` for repeatable local measurements. Compare equivalent inputs and evidence settings; a faster result obtained by scanning less source is not an equivalent-recall optimization.
