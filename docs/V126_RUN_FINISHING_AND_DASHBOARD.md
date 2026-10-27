# v1.2.6 run finishing and dashboard patch

This patch applies to the supplied archive-scout-main(20261027-213011).zip. Version 1.2.6 and SQLite schema 11 are unchanged. Existing projects, settings, capture identities, payload validation and checkpoints are preserved.

## Run-ending policy

The selected policy is **finish incomplete after bounded recovery without progress**. Unresolved work is never relabeled complete, silently skipped, or deleted to make a run finish. Resume starts a fresh recovery budget while retaining saved cursors, page ledgers, acquisition attempts, and eligibility schedules.

The shared budget uses the existing max_attempts setting (normally 4) for consecutive unsuccessful indexing turns or recovery sweeps. A quiet recovery period is also bounded to 30–300 seconds, derived from retry_max_seconds. Useful committed progress resets the budget. There is no total duration limit on a productive run. These are recovery-turn limits, not an exact count of physical HTTP requests. Existing per-request retries, endpoint handling, and currently active transfer timeouts still apply.

Individual HTTP turns no longer interpret rate_limit_attempts=0 as infinite: zero selects a finite default of four rate-limit responses. Explicit positive settings remain honored. Shared cooldown waits have a request deadline (rate_limit_max_wait, or 300 seconds by default). If a known server cooldown extends beyond that deadline, the request defers immediately. The server's cooldown is retained; no early recovery probe is sent.

Healthy acquisition pacing and worker settings are unchanged. Recovery uses fair bounded batches. A failed item cannot continually reenter the same media recovery sweep ahead of other due items. Attempt counters, error timestamps, repeated raw-result counts, and an unproven terminal page are not useful progress. Short cursor cycles are detected using a bounded history of 64 cursor hashes in text, direct media and all-snapshot embedded traversal.

## Operations and dependencies

The policy covers text indexing/acquisition, direct media indexing, embedded media lookups, automatic recovery and explicit retry operations. Their shared paths also cover All, Resume, acquisition-only and external-media-after-scan. Local rescanning/reporting does not acquire a network retry loop.

Already committed text captures can be acquired even when another index page is unresolved. Available saved pages can be scanned and used for media discovery while other text inputs remain unresolved. The final operation remains incomplete if any required inventory, acquisition, or scan work is unfinished.

During embedded lookup recovery, ready media is downloaded between lookup sweeps. A retrying URL no longer holds every resolved URL behind a whole-inventory barrier. Index-only operations retain their indexing-only behavior. All-snapshot acquisition may use already committed pages. Earliest/latest acquisition waits where a remaining inventory could change that URL's choice; a completed exact embedded lookup or complete direct inventory permits selection. Exact lookup provenance also handles archived HTTP/HTTPS spelling variants. A complete inventory still receives the normal duplicate/snapshot policy. Uncertain captures and existing downloaded files remain intact.

An explicitly oversized all-snapshot embedded page retries at a smaller page size using the same cursor. A temporary connection or server error does not cause arbitrary query-size changes.

## Dashboard accounting

The dashboard now labels pending **captures**, because pending includes records not yet classified. Its project-wide equation is:

Indexed = pending + in flight + saved + failed captures + non-text skips + URL-filter skips + media handoffs + unsupported media + other skips + other states.

Saved unscanned is a subset of saved, not another additive category. Each indexed capture occupies exactly one accounting bucket. Routing labels are only treated as skip outcomes when the capture's actual state is skipped, preventing stale routing from double-counting retried items. Unknown/legacy states remain visible. Other skips is computed rather than fixed at zero.

All dashboard figures are read within one SQLite snapshot, preventing a concurrent commit from making the equation disagree. The existing refresh controls and query deadline remain in effect. The display explains that these are snapshot records across the entire project, not distinct URLs or only the current selection. Media has a separate inventory; open error records are incidents rather than additional captures.

## Validation and limitations

The accompanying validation files record the full test results, local acquisition/scanning smoke workload, and synthetic large-project costs. Regression coverage includes bounded failure tails across all network operation modes, productive resets, resumed cursors, fair media sweeps, partial admission, earliest selection, archived scheme variants, cyclic cursors, oversized embedded pages, and concurrent dashboard reads.

The localhost workload verifies 240 payload hashes, saved documents, scan results, and domain extraction. It is a short controlled check, not proof of day-long live Wayback consistency. Synthetic SQL checks use 100,000, 500,000 and 1,000,000 text records plus equally sized media inventories. They do not measure network speed. Linux/Python 3.12 was exercised here; native Windows/macOS and display-dependent GUI tests were not run in this environment.

Returning incomplete is intentional when coverage cannot be established. Reports and diagnostics explain what remains; Resume can retry it. This patch cannot make an unavailable archive capture recoverable or promise a fixed finishing time under arbitrary server behavior.
