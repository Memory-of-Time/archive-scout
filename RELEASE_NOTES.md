# Archive Scout v1.1.3 — selective restoration patch

Patch **only** the named files onto the audited complete **v1.1.2** repository. The database schema remains **13**. Existing projects, explicit scan-overlap preferences, classification, media routing, CLI, archive selectors and fixed request pacing are retained.

## Changes

- New retained operations default to acquisition-first, then draining durable unscanned rows. Explicitly configured overlap continues to work; discard-after-scan still overlaps within its spool budget. Old projects with saved `scan_overlap=true` keep that choice.
- Repair the sidebar's actual requested width, add horizontal access to oversized pages, enable whole-page scrolling on History and Errors, normalize wheel events and prevent focus-induced jumps when controls are already visible.
- Give text fields, text areas and comboboxes consistent borders and focus indication. Select rows and press Ctrl+C (Command+C on macOS) to copy full error/history/results rows as tab-separated text.
- Keep pooled Python transports preferred when healthy; after 32 successful fallback requests periodically try the original primary on a real request. If it fails, do not probe on every attempt. No new request, delay or global cooldown is added.
- Publish rolling 10/60/300-second fresh-save rates and committed bytes **only after SQLite manifest transactions succeed**; don't count adopted files as new traffic. A rolling window does not guarantee sustained service throughput.
- Make `benchmark_scan_pipeline.py` spawn-safe on Windows/macOS with a proper guarded entry point and add the small smoke test to CI.
- Bump runtime, package, Windows executable metadata and workflow identity consistently to v1.1.3; existing full test matrix remains.

## Deliberately unchanged

The separate **2.5-second CDX / 0.125-second replay** request floors, actual server Retry-After, the existing fixed headerless-429/503 fallback, transient retry policy, persistent manifests, atomic `.part` files, schema 13, scanner Aho/regex semantics, text/media classification, FTS, project settings and CLI interface. Adaptive pacing, escalating cooldowns and bulk pool replacement are **not** introduced.

## Limitations

This is a targeted patch, not a complete replay of all historical releases or proof of near-eight saves/s on live Wayback over multiple days. The progress label now accurately calls the mixed counter "request failures (HTTP/transport)"; the existing `transport_failures` detail key remains for API/diagnostic compatibility and is still mixed. Separate HTTP-vs-transport telemetry and resource-scale optional analysis were not redesigned. Other pages with large fixed form layouts now remain accessible through horizontal scrolling, but not every form has been reflowed vertically. Running the GitHub Actions matrix on hosted Windows/macOS runners is still required before calling a release universally verified.
