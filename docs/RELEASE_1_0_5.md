# Archive Scout 1.0.5

This complete source release starts from the supplied 1.0.4 repository (schema 11). It applies the release audit with the user's explicit override: remove current-operation accounting and provide a compact explanation of miscellaneous outcomes instead. All original repository files are included.

## What changed

- Recovery distinguishes service eligibility from the recovery-cycle budget. An old incident can admit its next legitimate probe after the cooldown; renewing the cycle never shortens Retry-After. Temporary connection outages use shared increasing waits. Persistent recovery is on by default; Pause & save remains available.
- Replay waits inside its existing worker/client lifecycle. It stops new admissions, cancels queued tasks, drains active attempts and commits completed files before retrying. Transport health, cumulative metrics, batches and atomic-file adoption are preserved.
- Curl remains a validated fallback for text after eligible Python connection methods fail. Prefix/post-transfer checks, replay redirect restrictions, Range checks, byte limits and request admission remain in force.
- The Dashboard removes the current-operation accounting panel and additional inventory aggregate. Other outcomes reports non-text/media exclusions, URL-filter skips, real media handoffs, other skips, failed captures and recovered incidents. Open error records can outnumber failed captures; skips and handoffs are intentional outcomes. The existing manual/interval refresh controller remains in use.
- Reports groups use the shared wheel/focus router and responsive field columns. Horizontal bars sit outside the content viewport, with extra bottom clearance for the last controls. Every report setting and preset remains available.
- Disposition readers recognize the actual `deferred_to_media` writer value. Metadata-only binary exclusion does not claim a media queue exists. Retained textual descriptor bodies are identified as available for local search.
- Hitlist checkpoints track body revisions and compact per-run coverage. Resuming preserves unaffected hits, revisits changed earlier captures and reconciles availability counts. It freezes its capture-ID boundary when the run begins; newly indexed higher IDs require a new search. Legacy interrupted Hitlist runs restart once to establish reliable coverage.
- A crash-released OS lock excludes overlapping project operations/restores. GUI restore waits for active view reads and blocks same-project starts/edits during replacement. Backup validation and replacement run off the GUI thread; the existing safety snapshot is retained.

## What remains the same

CDX query construction, indexing strategies, paging/resume algorithms, date/collapse semantics, request-rate settings, media indexing, content-byte classification, replay-file validation and deterministic scanning are retained from the supplied 1.0.4 code. Source comparison verified unchanged executable syntax for those modules and the core replay/local-scan functions. The release does not raise request quotas or promise a particular Internet Archive save rate.

Capture files continue to hold original replay bytes; SQLite holds manifest, queue, review and coverage state. Text captures keep `.txt` names with original encodings and markup. Binary media follows the standard media pipeline. No duplicate capture bodies or acquisition-time full-file hashing were added.

## Existing projects

Schema 11 migrates forward to schema 12 automatically when opened for writing, with the existing migration safety backup. New state consists of a capture body-revision field and compact Hitlist coverage records. Capture/media files, notes, matches and scan history are preserved. Hitlist coverage records are created by Hitlist searches, not by acquisition-only runs. A schema-12 project should not subsequently be opened for writing with 1.0.4.

## Validation

The full offline test suite, compilation and 100,000-row offline benchmark were run. `validation-1.0.5.json` contains the exact results. Regression coverage exercises real expired-gate admission, server-deadline preservation, one shared probe, cancellation, settled replay workers, validated curl fallback, outcome counts, late-body Hitlist resume, metadata-only edits, project locking, restore reader draining and migrations preserving evidence.

Native Windows DPI/scrollbar pixel checks and packaged Windows/macOS/Linux executable smoke tests were not available in this environment. No live Internet Archive throughput or outage-cause claim is made. GitHub Actions build/release and the GUI/CLI packaging files are included.
