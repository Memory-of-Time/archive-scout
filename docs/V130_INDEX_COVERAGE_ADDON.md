# v1.3.0 indexing coverage add-on

Version remains **1.3.0**, database schema **12**. This is an incremental add-on
to repository commit `57868fc836f8628f606fb2101127b1c41d86f859`, including the
previous retry/CDX fairness add-on.

## Verified findings and changes

The supplied log shows the 2005 `*.ogrish.com/*` response repeatedly rejected
because an original URL has host `ogrish..com`. Rejecting the entire mixed page
also prevents all its otherwise valid captures from being committed. This is a
local scope-validation failure; that error does not establish a Wayback outage
or prove that half the site's captures were lost.

For wildcard domain indexing, a mixed, parsed page can now exclude a host with
empty DNS labels when removing only those empty labels would place it in the
exact selected domain/subdomain boundary. The excluded original is **not
rewritten**, stored as another capture, or passed to acquisition. Exclusions
use the existing durable recovery events and counts. Valid rows retain their
original URLs, timestamps and evidence. This applies to text and direct-media,
both resume and numbered-page paths, through their shared scope validator.
Spelling already inside the requested domain boundary retains its existing
behavior.

Unrelated domains, wrong dates/status/path/port, structurally corrupt responses,
and nonempty pages containing only excluded hosts still fail verification.
They cannot establish terminal coverage. The existing full-page missing-token
confirmation and numbered-page completion ledger remain in force.

The source also allowed a progressed resume window to be split after repeated
timeouts or size failures. Splitting dates discards its opaque URL-ordered
cursor and can replay the beginning of the range. A committed cursor now
prevents this automatic split or strategy restart regardless of failure count.
Temporary failures retry that exact request; explicit response-size pressure
can reduce the page limit at that same cursor. The configured attempt limit
can still leave a scope **explicitly incomplete**, with its checkpoint retained;
it cannot turn a failed response into complete coverage. The optional target
time budget remains opt-in.

The log's switch from a full-range traversal to annual traversal also switches
project directories (`ogrish` to `ogrishscan`). That alone does not prove a
same-project reset. Tests separately verify that reopening the same project
uses its committed range/year cursor, skips completed years, and processes
later years. No saved state, project setting, date-scope choice, schema, or
capture is reset by this add-on.

## Applying and recovering the affected run

Stop Scout, overlay the ZIP's `files/` contents onto the current repository,
and rebuild or run from that source. Alternatively apply the included unified
patch; use one method. **Delete no files.** Reopen the same existing project
directory and use Index or Resume. A still-uncommitted 2005 page must be fetched
once more; previously committed years and resume pages are retained. A patched
source tree does not update an already-built executable by itself.

The change cannot repair an absent checkpoint using log messages or guess an
expired cursor's replacement. If a service continues rejecting a cursor, its
scope remains unfinished rather than silently restarting or claiming success.

## Validation

The accompanying validation report records exact commands, results and local
resource measurements. The canonical suite ran **832 tests**, with **zero
failures, errors or skips**, including 12 new coverage regression tests and the
updated repeated-timeout regression. New tests exercise mixed first and later
pages, 10,000 valid rows, numbered-page ledgers, direct-media routing, annual
and range Stop/Resume, whole-page rollback after an injected SQLite write
failure, unrelated-host rejection, and repeated minimum-page-size failures
without cursor loss.

A separate real-SQLite local workload processed three 100,000-row pages,
retained all **299,997 selected captures**, excluded three known anomalies,
proved terminal coverage, and made no request on a subsequent completed run.
Early/middle/late database time was 2.64/2.28/2.29 seconds per page. Resident
memory was approximately 114/180/228 MiB while the database and its mapped/cache
working set grew; these three samples do not establish a memory plateau or
rule out long-run growth. No queue, worker, cache or database tuning is claimed.

Release metadata verification, the required offline benchmark, wheel build,
wheel payload comparison, CLI smoke check and clean patch-application check
are reported separately in the delivered validation evidence.

No live Wayback reindex, original user database, Windows/macOS build, million-
capture soak, or overnight run was performed. Local mocked-CDX timings measure
index validation and SQLite writes, **not** network acquisition or eight
validated snapshot saves per second. The patch addresses the demonstrated
scope poison and cursor rewind mechanisms; it does not certify a site's entire
remote index or sustained live-service throughput.
