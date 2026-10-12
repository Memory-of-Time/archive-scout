# Scout v1.2.1 — Patch-only installation

This ZIP is **not** a complete repository. Extract its paths directly over the root of the
`Scout-v1.2.0-Complete-Repository.zip` repository, replacing matching files.

This is a **single consolidated v1.2.1 patch**, not a secondary add-on. It
includes the prior Dashboard refresh hotfix and the intended 1.5-second CDX
default, plus the new CDX outage-recovery and endpoint-diagnostics changes.

`archive_scout/cdx/indexer.py` and `archive_scout/cdx/client.py` now preserve
saved resume-key/date windows during HTTP 5xx/429 and host connection failures
instead of treating all generic transient exceptions as reasons to split into
smaller and smaller date ranges. Explicit response-size errors still qualify
for subdivision. Read timeouts and malformed responses may subdivide **broad**
windows but not repeatedly fragment narrow ones into seconds. An outage no
longer reduces the saved CDX page size or blindly rotates through a fragmented
queue. Indexing errors remain retryable and checkpoints are not discarded.

This patch **does not** change `archive_scout/cdx/parameters.py`. The original
v1.2.0 automatic strategy remains if you did not separately install the
Detective Ra Timemap add-on; if you did, its automatic Timemap preference is
preserved. Tests in the consolidated patch support either profile. Explicit
Resume and Paged settings are unchanged, and already-saved incomplete windows
retain their selected strategy and boundaries. Pre-existing highly fragmented
windows are **not automatically merged**, since the saved state cannot prove
whether they were split for a legitimate oversized response; merging without
that information could cause a repeat of the original failure.

## Installation

1. Back up all project directories and project databases, especially before
   using maintenance/restore features or moving a project.
2. Open the Scout GitHub repository root (the folder containing pyproject.toml).
3. Upload/extract the included files with directory structure preserved, replacing
   matching files. Do **not** nest them in an extra folder.
4. Run `python scripts/verify_release.py`, `python scripts/run_tests.py`, then
   restart Scout. CI should run on Linux, Windows and macOS before public release.
5. Keep your `project.json` backups. The optional host recovery gate is **off**
   unless explicitly enabled in Settings → Network/recovery.

## Performance-preservation policy

Replay request-start spacing remains **0.125 seconds**; default workers remain
**10**. Intended CDX spacing is **1.5 seconds** (configurable). Text-vs-media
classification, CDX healthy request pacing, scanner concurrency defaults and
acquisition file layout are unchanged. CDX failure-only recovery policy and
endpoint diagnostics are improved. Monitoring adds no extra network requests or database
polling, and emits stall diagnostics only when no task has finished for 30s.

## Scope and outstanding compatibility risks

Delivered: index-and-download-only, acquisition-only retries, index-only scope,
bounded retry sweeps, operation locks, WAL-safe backup restoration, imported-file
ownership, project-file path rebase, media inclusion filter consistency,
on-demand media/hash integrity, Media page layout, passive stalled-acquisition
messages, **default-off** experimental shared connection-failure recovery,
CDX outage-aware subdivision, unchanged page size on server failures and
endpoint-cause/timing diagnostics.

**Not delivered:** automatic schema-13→9 migration (unsafe without a real legacy
fixture), historical scan-and-discard, overlap, scanner memory/workers controls,
and separate storage compaction. These require further data-integrity or
performance validation before restoring. Full live Wayback and hosted Windows/macOS
CI are not verified by this patch. The optional network gate is experimental;
benchmark before enabling it on large jobs.
