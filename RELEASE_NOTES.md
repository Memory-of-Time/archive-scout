# Archive Scout 1.0.2 release notes

Archive Scout 1.0.2 is a focused interface and per-target correctness release. The project schema remains **11** and existing v1.0.0/v1.0.1 projects remain compatible.

## What changed

- **Clearly outlined text-entry areas on every OS.** Multiline Tk text editors now receive an explicit theme-aware border/focus outline instead of depending on platform-native defaults that could make editable regions blend into the page.
- **Media editors are visibly grouped.** Media sites/paths, Include extensions, and Exclude extensions now use labeled outlined groups so it is immediately clear where each one-per-line list is entered.
- **Per-target overrides remain attached to the target through the full text pipeline.** A target-specific date range, match type, collapse/filter identity, or other query-defining setting no longer indexes under one CDX signature and then disappears when replay/scanning uses the global signature.
- **Per-target replay settings now actually apply.** Target-specific Download workers and Replay delay settings run in bounded target phases while still respecting Archive Scout's process-wide Wayback pacing floor and shared cooldown behavior.
- **Simple mode explicitly honors target overrides.** Configure current target stores overrides under the same normalized target identity used by the backend, and Sites and paths shows a live summary of the active override on the current line.
- **No schema migration.** Project schema remains 11.

## Compatibility

The release keeps the v1.0.1 rate-limit, scrolling, recovery, scanner, report, media, and project-safety behavior. Existing project configuration and SQLite data remain compatible.
