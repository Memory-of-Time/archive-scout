# Changelog

## 1.0.3 — Recovery correctness, project identity, and network integrity

- Tagged GUI operations, asynchronous views, row maps, and mutations with canonical project identity so stale data from one project cannot alter another.
- Preserved failed operation/scan lineage through Resume, including download-only recovery without keywords and acquisition-only error retry.
- Added header-first 429/503 handling, typed transport/storage diagnostics, coordinated replay/media outage pauses, backend cooldown enforcement, and release of inactive-client pacing floors.
- Added the **Download external redirect destinations** policy, blocked live replay escapes, and protected partial Range downloads from cross-representation redirects.
- Separated deterministic snippets from editable Notes and added bounded, read-only, paginated/copyable detail views across Errors, Results, AI relevance, Research Intelligence, and Scan history.
- Made Dashboard auto refresh honor the selected interval during active work, use consistent bounded read snapshots, and report query failures as unavailable instead of zero.
- Made moved project manifests resolve to the folder selected by the user.
- Keeps project schema 11 and preserves existing v1.0.2 acquisition/scanning semantics outside the audited fixes.

## 1.0.2 — Interface outlines and per-target correctness

- Added explicit cross-platform outlines for multiline text-entry areas and labeled Media input groups.
- Fixed target-specific CDX signatures/date scopes being lost when the text replay and scan selectors used only the global project signature.
- Made per-target replay worker and delay overrides effective during text acquisition.
- Normalized Configure current target keys and surfaced the active override summary in Sites and paths, including Simple mode.
- Project schema remains 11.

## 1.0.1 — Rate-limit and scrolling corrective release

- Propagates an exhausted Wayback service pause unchanged to the operation boundary instead of shrinking, splitting, rotating, or retrying the pending request.
- Stops paged text/media admission on the first service-wide deferral, cancels queued sibling work, commits already-validated successes, and leaves unfinished pages pending without ordinary failure inflation.
- Enforces one shared absolute recovery deadline, including time spent behind another worker's cooldown, and preserves server Retry-After eligibility across restart.
- Applies adaptive pacing once per coalesced service incident and keeps healthy automatic indexing on resume-key traversal rather than re-fetching a dense prefix through paged mode.
- Persists typed rate-limit state and operation progress so GUI/CLI consumers can distinguish cooldown, resumable service pause, connectivity failure, and user cancellation.
- Prevents focus changes on already-visible controls from moving scrollable pages and reveals off-screen keyboard focus only by the minimum necessary amount.
- Uses one interpreter-wide wheel router, pointer-target routing, stable high-resolution residuals, and prevents the same wheel event from scrolling both a native child and its parent at a boundary.
- Keeps schema 11 and all v1.0.0 project data compatible.

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
