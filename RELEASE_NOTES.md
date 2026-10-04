# Archive Scout 1.0.3 release notes

Archive Scout 1.0.3 is a focused recovery-correctness, project-identity, networking, and GUI reliability release based on the October 3 deep audit. The project schema remains **11** and existing v1.0.2 projects remain compatible.

## Recovery and project identity

- Progress, completion events, asynchronous view queries, visible row maps, and state-changing actions now carry/check canonical project identity so stale data from project A cannot mutate project B.
- Switching projects clears incompatible visible rows/details immediately while allowing an already-running operation to continue against its original frozen project configuration.
- Resume now considers recoverable failed operation snapshots in addition to interrupted/paused work and restores the saved operation contract instead of falling into an unrelated scan path.
- Compatible failed/interrupted scan runs can retain their existing lineage and previously committed matches/reviews during recovery.
- A moved `project.json` uses the selected manifest directory as the active project root instead of silently preferring an old absolute `output_dir`.

## Network recovery and diagnostics

- Live Wayback 429/503 responses are recognized from headers before response-body reading or content/media validation. Archived-origin Memento responses remain distinguishable from current Wayback service throttling.
- Replay and media acquisition now coordinate bursts of genuine connection failures so a common outage can pause the operation and preserve untouched rows as pending instead of consuming the entire inventory's retry budgets.
- Transport backend cooldowns are honored. When every backend is cooling, the request waits rather than immediately recycling the same backends.
- Local filesystem failures such as disk-full, read-only, and permission errors remain local-storage failures and are not misclassified as HTTP backend problems.
- Timeout and transport diagnostics preserve more specific cause/status information.
- Shared pacing now tracks active client registrations. Closing a slower operation releases its requested pacing floor while genuine service-driven adaptive recovery remains shared.

## Redirect integrity

- Added **Download external redirect destinations**, disabled by default.
- External archived redirect destinations are checked before contacting the redirected archive target. Blocked destinations are recorded as policy errors and can be retried after enabling the option.
- Direct live-web escapes remain blocked rather than being silently stored as historical capture content.
- Resuming a partial Range download across a redirect restarts the individual representation instead of appending bytes from a different destination.

## Error retry and scan continuity

- Added acquisition-only text error retry for download-only projects. It does not require a keyword set and leaves successfully retried captures in `downloaded_unscanned` state without creating scan/document/match rows.
- Scan failures can leave the operation/scan lineage resumable instead of being finalized as fully complete when retryable scan work remains.

## Interface and read safety

- Results no longer mix deterministic snippets into the editable Notes field. Notes, tags, and review state remain human-authored data.
- Read-only GUI views use a query-only database helper with bounded waits instead of opening a writable connection that can run migrations/recovery as a side effect.
- Results and FTS share one coalesced view identity so an older request cannot overwrite a newer search. Similar project/generation guards cover Errors, AI relevance, Research Intelligence, and Scan history.
- Errors now have explicit status filtering, pagination, readable details, copy actions, and category scope that also applies to grouped site issues.
- Scan history is paginated and has a selected-run detail view and copy support.
- Results, AI relevance, Research Intelligence, Errors, and Scan history use dedicated scrollable detail areas for long evidence/URLs/messages.

## Dashboard

- Automatic dashboard refresh can run during active work and honors the selected interval instead of being disabled for the duration of the operation.
- Refresh requests are coalesced and stale project tokens are discarded.
- Dashboard count reads use a bounded read-only snapshot so cards come from one consistent view of the database. Query failures become unavailable/unknown rather than exact zero.
- Manual mode remains a true no-automatic-recount mode; live operation status remains separate from database recount semantics.

## Compatibility and validation

- Public version: **1.0.3**.
- Project schema: **11**; no schema migration required.
- Existing v1.0.2 projects remain compatible.
- Full local suite: **373 tests run, 370 passed, 3 expected environment/display skips, 0 failures/errors**.
- The GitHub Actions workflow keeps Linux, Windows, Intel macOS, and manual ARM64 macOS coverage on Python 3.11 and 3.12 and asserts package metadata version 1.0.3.
