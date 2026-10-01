# Downloader design notes

Archive Scout's execution engine has been informed by comparison with other Wayback downloaders, especially designs that use persistent work tables, resume keys, keyset pagination, bounded concurrency, bulk SQLite operations, size-aware scheduling, compiled multi-pattern matching, and streamed output.

Archive Scout intentionally adapts those useful fundamentals to its own project architecture rather than copying process-global state, destructive queue resets, subprocess-driven UI control, unbounded task creation, or complete-response buffering.

The result remains an Archive Scout project workflow: deterministic scanning, review history, analysis, recovery, media state, reports, site diagnostics, and optional AI relevance all share one persistent project model.
