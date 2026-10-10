# v1.1.3 — selective performance and GUI restoration

- Acquire-first as the default for new retained scans, with compatible explicit overlap and discard handling.
- Restored workable sidebar width, horizontally accessible pages, wheel/focus handling and visible input borders; tabular copy support.
- Measured-window post-commit fresh-save metrics and periodic return to pooled backend after a fallback.
- Spawn-safe scanner pipeline benchmark and expanded regression/CI checks.
- No schema migration or adaptive rate limiter; existing v1.1.2 projects and settings remain compatible.

# v1.1.3 — stable engine rollback

- Restore the v1.0.0 interface and the tagged v1.0.2 indexing/download engines.
- Remove adaptive pacing, escalating headerless cooldowns and recent global connection recovery machinery.
- Honor server Retry-After; use fixed short retries and preserve pending work.
- Drain healthy replay transfers after recoverable pauses. Continue the same operation automatically.
- Keep current schema13, encoding, routing, backup/restore and bounded local-scanner corrections.
- Replace tests for retired experimental behavior with fixed-policy and retained-evidence regressions.

# Changelog

## 1.0.0 — Initial public release

- Introduces the complete Archive Scout desktop and CLI workspace.
- Adds resumable CDX/Timemap indexing, capture/media acquisition, deterministic scanning, reports, review state, Research Intelligence, optional AI relevance, archive analysis, and recovery tooling.
- Uses separate shared Wayback pacing pools: 2.5 seconds between index attempts and 0.125 seconds between replay attempts.
- Counts and paces actual redirect hops, transport fallbacks, and retry attempts rather than only logical operations.
- Adds coordinated 429/503 cooldowns with a 60-second missing-header minimum, Retry-After minimum-deadline semantics, one recovery probe, and gradual rate recovery.
- Distinguishes rate-limit pauses from connectivity pauses and preserves exact resume state for both.
- Avoids interpreting historical archived-origin 429/503 responses as live Wayback throttling.
- Adds Windows Per-Monitor V2 DPI declaration, system/high-contrast theme awareness, named-font scaling, screen-clamped geometry, responsive scroll containers, and independently scrollable data tables.
- Moves expensive Results/FTS, History, Errors, and site-issue reads off the Tk UI thread.
- Ships the scanner optimization baseline and preserves schema 11 for compatibility with pre-release project data.
