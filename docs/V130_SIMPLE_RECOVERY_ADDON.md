# v1.3.0 simple indexing and download-tail recovery

Incremental base: `5d3688a15055065f96634b3719899031619aacd6`.
Version remains **1.3.0**, SQLite schema **12**. No interface, setting, CLI
argument, dependency requirement or database-schema changes.

## Indexing

Healthy direct enumeration keeps its existing fast path. In automatic mode,
an initial resume query that stalls with HTTP 504, a read/request timeout,
an incomplete response, or explicit response-size pressure can now save a
numbered-page plan and use the existing parallel page pool. This is one
targeted fallback, not a new scheduler or asynchronous rewrite.

The selected date range, filters, collapse and endpoint preferences remain
unchanged. No date partitioning is introduced. A committed resume key, numbered
page cursor/ledger, explicitly selected resume strategy, or recorded lack of
pagination support prevents this replan. Connection/pool failures, HTTP
429/502/503 and other service-control responses do not justify it. A bad host
or invalid status/date is still a validation failure, not permission to bypass
scope checks.

The fallback is saved transactionally before requests use it, with a durable
recovery event retaining the failure reason. It survives Stop/reopen. Explicit
unsupported-pagination responses use the existing resume fallback and disable
further automatic switching for that window. Already successful numbered pages
are retained; a failed page alone is requeued. Completion still requires the
existing durable page ledger reconciliation or verified resume exhaustion.

The same helper is used by text and direct-media indexing. Existing parsed-body
limits, configured worker/memory limits, pacing, status handling and evidence
validation remain active. There is no new concurrency across independent
targets in this focused patch; it reuses parallelism within a numbered scope.

## Download tail

The source previously reset an exhausted recovery budget whenever any scheduled
failure had a network category. Repeatedly timing-out captures could therefore
keep the final recovery phase running indefinitely, even without a successful
save. The same exemption existed in media recovery.

Both tail recovery phases now honor the existing no-progress sweep limit and
elapsed recovery budget. The elapsed allowance comes from the existing retry
maximum setting, bounded to 30–300 seconds by `NoProgressBudget`. It is checked
between recovery work, not an absolute wall-clock cap on an entire sweep or an
already admitted transfer. Productive sweeps reset the budget. Fresh acquisition
and its interleaved retries are unchanged, as are individual transfer deadlines.

A future saved cooldown exceeding the remaining allowance yields immediately
instead of waiting for a long final retry. Stop is checked before text recovery
queries/cooldown decisions. Failed captures remain errors, their future due
times and evidence remain saved, and operation completion still reports these
gaps. Nothing is newly marked skipped, unavailable or successful. Resume or a
later explicit retry can continue those captures; delays are never bypassed.

This deliberately changes the old indefinite post-download recovery policy.
An extended service outage can now leave the download pass explicitly
incomplete. Temporary failures still recover automatically within a running
pass when they clear within its budget. This is not a guarantee that every
remote failure can be resolved without a future retry.

## Scope and validation

The user's supplied log showed 377 failed indexing turns and about 8 hours 20
minutes inside those failures. Most were HTTP 504; smaller groups were read
timeouts, active deadlines and incomplete transfers. Its retry tail completed
only seven targets over roughly 6.5 hours. This supports improving request
planning; it does not prove a global Wayback outage or connection-pool defect.

The supplied reference downloader inspired reuse of numbered pages and bounded
parallel requests. Its weaker cursor-before-write and completion-flag behavior
was not copied. This patch is independently implemented in Scout's existing
flow and does not add reference-downloader code or dependencies.

The delivered validation report records full test results, a 300,000-row
fault-injected local SQLite workload with early/middle/late resource samples,
release checks, an offline benchmark, clean patch application and wheel checks.
No live Wayback, original user database, native Windows/macOS build, million-
capture or overnight validation is claimed. The workload tests indexing and
recovery, not eight validated snapshot saves per second.

Apply the ZIP's `files/` contents over the matching repository and rebuild.
Delete nothing. Open the same project to continue its saved state. Applying
source files does not update a previously built executable by itself.
