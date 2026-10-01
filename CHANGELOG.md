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
