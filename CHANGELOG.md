# Changelog

## v1.3.0 — large-project sustainability
- Prevent a recognized empty-label host anomaly in a mixed CDX page from blocking the selected site's valid captures; keep strict validation, audited exclusions and exact pagination checkpoints.
- Never discard a committed resume key to split or restart a date range after repeated failures; retain same-cursor retry and page-size reduction for explicit size pressure.
- Stop CDX transport-truncation failures from multiplying JSON/text and endpoint fallbacks; retain genuine format fallback and the exact index checkpoint.
- Reuse operation-owned text worker pools across retry batches and select automatic retries from due schedules and unresolved legacy errors, preserving saved scope and retry eligibility.
- Keep last-successful-save age and recent-save rate across retry batches, distinguish selected retry batches from full inventory progress, and throttle batch-end health output within an operation.
- Recover nonempty, CDX-digest-proven payloads when a contradictory gzip header and broken HTTP framing otherwise reject the exact selected body; preserve strict rejection of wrong digests, genuine gzip truncation, empty bodies, limits and cancellation.
- Remove the hidden elapsed retry cutoff from text, direct-media and embedded-media indexing. The explicit target time budget remains opt-in; failed-attempt limits and durable incomplete checkpoints remain enforced.
- Space repeated text representation failures while fresh work proceeds, and retain bounded recovery proof samples in acquisition health history.
- Capture bounded owner-stall thread locations when diagnostics are enabled; no thread variables, page bodies, database access or network probes are collected by the watchdog.
- Maintain exact dashboard summaries in the same SQLite transactions as capture, routing, match, error, network-event and media changes; construct them once during backed-up schema-12 upgrade.
- Replace repeated dashboard history scans with compact summary reads while preserving manual/automatic refresh settings and WAL snapshot consistency.
- Add a pending-scan keyset index and a document-error lookup index to remove measured history-dependent lookup costs.
- Stop new acquisition promptly after an unexpected local scanner-supervisor failure, preserving retained bodies and unstarted work.
- Keep bounded, rotating five-minute acquisition-health history and include summary/schema code in build fingerprints.
- Rebuild dashboard summaries during explicit project repair; retain source routing, integrity checks, retry budgets and GUI/CLI contracts.

## v1.2.9 — acquisition recovery
- Give active text replays one shared deadline across connect, headers, body, fallback and redirects. Preserve expired captures as retryable and give the later recovery sweep a longer budget.
- Suspend text dispatch during shared host and backend cooldowns while local scans continue. Restore unstarted captures without charging an attempt if a cooldown begins after queue admission. Persist server retry deadlines and include rejected HTTP responses in diagnostics.


- Recheck backend availability before each physical request. Late results from older requests cannot overwrite a newer recovery decision, and a healthy backend can take work while another backend is recovering.
- Bound urllib3 pool checkout, separate pool waits from connection failures, and keep redirect pacing and host permits scoped to each request.
- Reopen recovered Python backends on successful response headers while continuing to validate every body before saving. Stall diagnostics report backend probes, waiters and recent validated saves.

## v1.2.8 — scan coverage and final interface changes

- Replace the Media exclusion editor with site-level abuse reporting guidance and remove Download scope from Settings. New GUI runs scan eligible text across the selected sites and dates. Use **Index URLs only** when you only need the inventory.
- Keep confirmed low-weight hits above zero and make the fast literal filter agree with Unicode case-insensitive matching. Collect regular-expression evidence from the original and normalized field. Use the larger per-field occurrence count for scoring so the two representations do not double-count the same hits.
- Scan plain text consistently in background acquisition and Rescan. Record every distinct matching passage across body, title, links, source and URL. Merge overlapping contexts and preserve explicit report limits.
- Report counts for score thresholds, exclusions and required terms. To refresh prior results, run **Rescan existing downloads with selected keyword sets**. Earlier scans and reviews remain available under their original scan runs.

## v1.2.7 — pipeline audit and release refresh

- Make automatic Research Intelligence indexing consistently optional and off by default; retain manual build/search and explicitly saved opt-in.
- Restore useful all-snapshot embedded lookup concurrency with smaller exact-URL pages and bounded response reservations; reduce duplicate discovery work and unchanged queue writes.
- Bound active CDX response work, return incomplete bodies to the checkpoint scheduler, and close failed urllib3 bodies without draining them.
- Add text owner-stage timing evidence and retain public SQLite schema 11, existing capture identities and saved checkpoints.
- Update application, package, default client identification, Windows and macOS version metadata and release checks to 1.2.7. Preserve deliberately saved 1.2.6 pacing and custom client identifiers.
- See docs/V127_PIPELINE_AUDIT.md for measurements, regression coverage and unresolved limits.

## v1.2.5 — text consistency follow-through (same public version)

- Share one text-representation policy across acquisition, imported local text, rescan and Hitlist: preserve literal plain-text angle brackets in FTS while still rendering verifiable HTML and honoring historical HTML suffixes.
- Import the nine audited text suffixes with the normal BOM/charset decoder and bounded directory traversal. Preserve original local files and bytes; refuse recognizable binary content instead of inventing a text interpretation.
- Repair stale persisted body text and FTS entries when a local rescan reinterprets unchanged retained bytes. Document database upserts now compare `body_text` as well as normalized fingerprints.
- Enforce shared physical-request pacing and renewed host admission on every redirect hop for httpx, urllib3 and curl, including automatic backend fallback. Revoked host permits do not cause another-backend retries.
- Permit curl to identify and return complete HTTP 429/503 response headers without waiting through a stalled control-response body.
- Tighten playlist descriptor acceptance to positive format-root evidence and preserve authentic unknown media filename extensions instead of inventing `.mp4`/`.png` during text-to-media routing.
- Improve stall messages with transport cooldown/waiter evidence. Keep version 1.2.5, SQLite schema 10, existing project state, and unchanged healthy replay pacing.
- The new targeted regression tests cover nine historical imports, rescan-to-FTS repair, all backend redirect hops, host permit revocation, curl delayed control bodies, and positive/negative media-descriptor cases.

## Archive Scout v1.2.5 — text acquisition consistency corrections (20 October 2026)

- Verify complete standalone HTTP 206 ranges and compressed-stream checksums/end markers before publishing text payloads across httpx, urllib3 and curl; reject truncated gzip instead of saving its valid-looking prefix.
- Handle HTTP 429/503 control status before decoding its potentially malformed response body in pooled Python backends.
- Prevent capture-specific decompression and local pool failures from cooling unrelated backend routes; pace physical request admission after backend recovery and on fallback.
- Keep text retries eligible through interrupted reservation/attempt counts, and interleave bounded due retries with fresh work without rebuilding the long-lived thread pool.
- Include overlapping target memberships in saved-body scanning, retry due-time selection and operation-completeness checks; resolve download versus scan errors at the successful operation boundary.
- Preserve all 76 explicitly selected historical image/video-group extensions in media indexing; correctly parse `.dvr-ms`, accommodate percent-encoded dots in the CDX suffix prefilter, and recognize supported playlist text as a media descriptor rather than an HTML error.
- Improve declared wide-character text admission, preview-boundary handling, XML encoding declarations, historical `.phtm` Hitlist fallback, and nested ENOSPC/EDQUOT failure propagation.
- Preserve version 1.2.5 and schema 10. Benchmarks and unresolved limitations are provided in the accompanying patch validation ledger; offline throughput is not a live Wayback guarantee.

## Archive Scout v1.2.5 — strict acquisition and media integrity

- Require completed selected index evidence before new download requests at operation and downloader entry points.
- Add schema-10 indexed active media retry scheduling with a mandatory pre-upgrade safety backup.
- Fix misleading MIME/binary phrase and short `206` media validation errors; preserve stream-first acquisition.
- Correct media query identity for target overrides; avoid collapsed earliest/latest selection without timestamp proof.
- Replace SQLite OR-expression rescan scope expansion with indexed temporary membership.
- Keep direct versus external embedded retries separated, and adopt large legacy media error histories in bounded batches.
- Preserve existing report snapshots, scanning, CLI/GUI packaging and healthy replay pacing.


## Archive Scout v1.2.4 — official operations/reports integration

- Resume now carries forward the original direct or external media phase and its selected settings.
- GUI/CLI expose durable incomplete operation outcomes; successful reports do not imply complete acquisition.
- Scan completeness is checked before media handoff, and report failures cannot invalidate committed scan evidence.
- Report snapshots may be written during a live operation with an independent read-only worker and unique completed-set publication.
- Media outputs honor configured files and columns. Obsolete patch-baseline artifacts removed from the official source tree.

### Indexing and acquisition stabilization retained in v1.2.4

- Fair text/media target scheduling with saved incomplete cursors, retry eligibility and optional per-target indexing time budget.
- Reject malformed, wrong-scope and unexpectedly continued CDX responses instead of marking incomplete inventories complete.
- Protect media index cursor/paging/collapse identities across HTTP outages and record committed media page evidence.
- Fix cold-start media indexing exceptions and policy-blocked media false completion.
- Skip `robots.txt` filenames at text acquisition admission; preserve the existing 76-suffix text/media routing policy.
- Preserve overlapping target membership without duplicating capture bodies, prevent migrations without a successful backup and bound estimated concurrent CDX page response bytes.
- Preserve existing schema-9 project compatibility and healthy local acquisition pacing; see `docs/V124_INDEXING_RECOVERY.md` for unresolved protocol limits and test scope.

## Archive Scout v1.2.3 — repository organization and public rebrand

- Retains the later v1.2.4 implementation supplied as the baseline, under the requested public version 1.2.3.
- Consolidates release instructions, removes obsolete hotfix/patch files, and aligns app metadata, packaging, installers, release assets, and CI checks.
- Preserves schema-9 databases, indexing checkpoints, acquired files, prior scans, operation behavior, and legacy `scout` CLI aliases.

## Earlier implementation lineage (retained)

The supplied repository incorporated an earlier internal v1.2.4 development line: persisted acquisition retry scheduling, streamed validated bodies, independent scanning, SQLite concurrency recovery, repeatable WAL-safe merges, CDX recovery, media routing, and reporting fixes. These features remain present in the rebranded release. This history is not a promise of verified multi-day throughput.
