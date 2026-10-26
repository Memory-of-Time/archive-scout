# Archive Scout v1.2.5 completion, acquisition and report audit

Date: 25 October 2026. Version remains 1.2.5; database schema remains 10.

## Baseline and evidence

The authoritative baseline is `archive-scout-main(20261025-200106).zip`, supplied in this chat after the audit began. SHA-256:

`6684086dd714e9b31071c09251b70bd74f60cc68ad83cdaca876efda99c83d32`

No code from the earlier October 24 source archive was used as the patch baseline. The accompanying manifest identifies every replacement/addition and both original and patched file hashes.

Evidence reviewed: that complete source archive, `Pasted text(20261025-021926).txt`, the report export screenshot, the supplied historical `errors.txt`, and the supplied media `project.json`. The error report spans releases. Its historical counters and messages do not prove that a defect remains in this baseline. The activity log's `frozen-unfingerprinted` identifier also cannot establish which executable revision produced every entry. Source defects below were reproduced separately.

## What the activity actually establishes

Counter differences, rather than the displayed cumulative average, give these successful-save rates:

| Activity window | Additional saves | Saves/s | New errors |
| --- | ---: | ---: | ---: |
| 21:19–21:24 | 1,790 | 5.97 | 120 |
| 21:34–21:39 | 1,607 | 5.36 | 71 |
| 21:48–21:53 | 1,244 | 4.15 | 43 |

The approximately 31% decline occurs while error frequency falls. Increasing errors alone cannot explain it. Captures are ordered partly by recorded length, so later payload sizes and recent request/commit latency must also be examined before attributing the decline to resource growth.

At 21:53:05, `793,777/795,349` captures were satisfied, leaving 1,572 obligations. All attempt outcomes being processed did not mean that every capture was acquired. A later 600-capture sweep saved zero: health counters reported 552 digest mismatches, 46 connection failures and two incomplete representations. This is evidence of unproductive recovery, not permission to waive validation.

The historical error report contains connection refusals that were labelled timeouts by older code. The current baseline already distinguishes those types. This patch does not rewrite historical errors or claim that their old labels were reproduced in the current release.

## Reproduced defects and changes

### Report export can destroy the generated export after a denied rename

The screenshot shows access denied while renaming `.partial-...` to a final manual-report folder. The source unconditionally removed staging after any exception, including a failure after reports and the manifest were fully written.

Publication now retries only access/sharing failures, up to five attempts with a total scheduled delay of 1.85 seconds. Healthy exports have no added wait. If finalization still fails, the complete staging tree is preserved and the exception gives its location. Render failures still clean up unfinished staging. The exporter does not mark an unpublished export successful or overwrite an existing destination.

The test injects permission failures into the real export path and verifies preservation, retry success and lock release. It does not identify whether the user's actual Windows denial came from ACLs, antivirus, Explorer or another process. A persistent external denial still requires resolving that access problem.

### A valid target can fail local CDX verification indefinitely

`validate_cdx_scope` interpreted `*.nothingtoxic.com*` as a literal hostname suffix containing an asterisk. A row for `www.nothingtoxic.com` was rejected every time. The supplied media project contains that target pattern. The local reproduction establishes a concrete rejection path, rather than proving that every user's unverified response has this cause.

Wildcard host expressions now use wildcard matching, including the root form of a leading `*.` pattern. Literal checks allow the bare/`www.` spelling that can share an archive canonical key. Unrelated hosts, out-of-range timestamps and contradictions of an explicit `statuscode:200` filter still fail. The server retains ownership of its regex and canonicalization semantics; local validation is not a second, incompatible query engine.

### The legacy text-response parser could reject its own requested layout

The fallback requested `urlkey` plus six fields, while its legacy object parser expected six fields starting with the timestamp. That API could repeatedly reject valid line-oriented responses. Both object and compact APIs now use the same parser, including gzip, continuation tokens and original URLs containing spaces. Compatible six-column responses that omit the requested urlkey remain supported; malformed rows still fail the whole page.

### A recovered continuation token was treated as a verification failure

The text indexer re-requested a full-sized page with no resume token, then rejected an alternate response precisely when that response supplied the missing token. It could repeat this page indefinitely.

The valid alternate rows and token are now returned together for the owner to commit atomically. The terminal probe requests one extra row. Equal rows under the larger limit can establish exhaustion; a still-full, contradictory response cannot. Comparisons cover every row, not just the first and last three.

Direct media resume indexing previously lacked this terminal-page check. It now uses the same rule, including the embedded all-snapshots page path. Healthy pages with continuation tokens, and genuinely short terminal pages, need no extra request. The added request applies only to an ambiguous full terminal page.

### Missing media page evidence could leave an empty scheduling loop

A media page cursor could reach its advertised page count without all page-ledger entries. The scheduler then returned an empty finished batch, while the outer loop refused completion and requested the same empty batch again.

The media loop now reconciles the expected page identities and explicitly requeues missing evidence, using the same completion safeguard as text indexing. Revision adoption also carries compatible media page-ledger entries forward, rather than discarding their usefulness.

### Earliest/latest media cannot be proved from a collapsed text index

The baseline could reuse a completed text inventory containing digest/urlkey representatives, then declare its media timestamp selection complete. A representative need not be the requested timestamp extreme.

That shortcut is refused for earliest/latest when the text inventory was collapsed. Proven compatible uncollapsed inventories remain reusable. Completed range coverage, including a single-year range, can now be reused locally; overlapping target membership is included. This avoids a redundant CDX traversal where the existing evidence is actually sufficient.

Media coverage revision 4 preserves compatible unfinished cursors, existing media IDs/files and error history. Uncertain older complete states are reverified instead of relabelled as proven. Compatible terminal histories below the smallest supported page length (100 rows) can be adopted when there is no collapsed earliest/latest risk. Larger old histories lack a retained terminal-page length, so their coverage is reverified. This is a conservative one-time cost; no capture payload is deleted. Historical conflicting rows are retained instead of being deleted during page-size adoption.

### All-snapshot embedded lookup used unbounded per-URL accumulation

The old worker accumulated every timestamp for an embedded URL before returning to the database owner. A prolific URL could consume growing memory, and an interruption late in that traversal required starting that URL again.

`media/embedded_pages.py` now fetches bounded pages in a memory-limited worker pool. Only the owner connection writes inventory rows and their continuation checkpoint, in one transaction. An interrupted lookup resumes at the committed page after reopening the project. No project-sized Python list or sleeping thread per URL is introduced. Accepted rows are inserted in bounded batches.

Older completed all-snapshot embedded lookups receive one-time revalidation. They did not store enough terminal-page evidence to distinguish a real short accepted inventory from an accepted subset of a truncated full page. The scope-specific initialization marker and state changes commit together. Existing media captures/files and lookup attempt history remain intact. Subsequent passes do not restart successful lookups. Direct acquisition admission rejects old unverified all-snapshot lookup coverage until that revalidation occurs.

Earliest/latest still use their inexpensive exact first/last queries, with scope validation. Configured CDX filters and collapses remain in force for the all-snapshot policy.

### Page-pool cancellation was advertised but missing from HttpClient

The existing parallel CDX code looked for `client.cancellation_scope`; the baseline client did not implement it. Pool cancellation therefore could not interrupt sibling gate waits through that mechanism.

A thread-local cancellation scope now composes the pool token with the user's Stop event. A service pause can release sibling waiters without setting global Stop. In-progress socket reads still depend on their configured network timeout; this is not a claim of instantaneous socket cancellation.

### Recovery and queue work repeated unnecessarily

Each text acquisition recovery batch creates a new background scanner connection. Its supposedly once-per-connection legacy reconciliation could therefore revisit historical documents on every batch. Reconciliation now belongs to the persistent owner connection/scope; the scanner consumes the resulting explicit pending states. Standalone scanner calls keep their existing reconciliation default.

Selected text retries are driven from their bounded ID table and do not change unrelated redirect-error states. Persistent replay acquisition delegates failed request turns to its durable retry owner, avoiding multiplication by another inner retry loop. Independent backend fallback, server Retry-After, fixed request spacing and user Stop remain active. With persistent recovery disabled, the existing finite client retry settings remain applicable.

Media acquisition previously repeatedly selected and sorted the shrinking inventory by a `COALESCE(length)` ordering that its persistent index could not directly provide. It now builds one ordered TEMP queue and traverses its index. Python retains one batch and bounded futures. This is temporary disk space proportional to selected work, not a RAM list proportional to the inventory.

Media recovery now honors the persistent-retry toggle. Explicit selected 404 rechecks are allowed once, while policy errors stay closed; automatic recovery does not endlessly reopen 404s. Category selection is respected. Explicitly oversized resume responses reduce their saved page length while preserving date/collapse scope and the cursor, rather than repeating the same oversized media page.

### Media publication and progress needed stronger accounting

Media payloads are synced before rename, then the directory is synced where supported, before the database records success. This closes a durability gap relative to text acquisition. Storage-full errors block new media work and preserve restartable state instead of becoming ordinary URL failures. The localhost media comparison measured no material throughput regression from this publication ordering.

Media progress now distinguishes processed outcomes, successful saves and errors. An error is not counted as a successful save. Media has its own bounded acquisition diagnostics. Representation rejection now records the observed HTTP status instead of misleadingly leaving the response counter empty. Missing timing samples are not invented as zero-duration transfers.

Health diagnostics include recent response timing/payload-size averages, process CPU time, thread count and available process-memory metrics. Their storage/rings remain bounded. Packaged-code hashing is attempted when source files are unavailable, improving build attribution; native frozen bundles still need platform validation.

`media_completeness.json`, emitted with the media summary, reports the current semantic inventory, state counts, index/discovery gaps and unfinished captures. Existing historical media reports remain available and their summaries identify that broader scope. This separates current obligations from errors left by other runs/releases. It reports committed acquisition evidence; it is not a new full-disk hash audit.

## Completeness versus speed

No validation is disabled to improve the counter. Digest/header repair still requires the existing proof, partial transfers stay unfinished, media acquisition keeps its positive coverage admission, and text/media routing remains intact. Existing project databases, saved captures, user settings, flat media naming and GUI/CLI entry points are preserved. There is no new adaptive pacing layer.

Added costs have specific evidence benefits:

| Cost | When incurred | Completeness benefit |
| --- | --- | --- |
| One larger-limit terminal query | Full page without continuation | Avoids silently accepting an unproven prefix |
| Reindex where a collapsed text index cannot prove extrema | Earliest/latest media in that case | Establishes the actual eligible timestamp universe |
| One-time older all-embedded lookup revalidation | Existing all-snapshot lookup scope without page proof | Repairs the earlier possible terminal-page omission |
| File/directory sync | Successful new media retention | Publishes durable payload bytes before success state |
| Ordered queue construction | Once per media download batch | Removes repeated sorts; does not change selected membership |

The benchmark with one million historical captures found that selecting just 20 retry identities already took approximately 1–2 milliseconds in the baseline. That query is not a credible explanation for an hours-long tail in this workload. Persistent network failure, repeatedly rejected representations, old-loop defects and real request timeout/backoff remain distinct possible causes.

At 0.125 seconds between physical request starts, the nominal budget is about eight starts per second. Retries and redirect hops consume that same budget. Eight validated saves per second cannot also be guaranteed during throttling, connection failures, invalid payloads, insufficient bandwidth or a final handful of genuinely unavailable captures. Large media files additionally depend on bytes/second. Making the operation finish by silently discarding those captures would violate completeness.

## Validation

See the package's `validation/` JSON and logs for exact results and commands. The baseline and patched suites were run with inherited proxy environment variables cleared for fixtures; Scout's user proxy behavior/default was not changed. An initial baseline run with the host's SOCKS proxy environment had three missing-optional-dependency errors; the clean-environment baseline subsequently passed.

The final suite has 551 tests: 0 failures, 0 errors, 4 skips. The baseline has 523 tests with the same four display-dependent skips. The 28 added tests cover permission-error preservation; CDX scope, layout and continuation recovery; page-ledger repair; media-state revision adoption; ordered queue membership; media disk-full handling; current-versus-historical completeness; selected media 404 recovery; local cancellation; and embedded-page rollback/reopen/revalidation. A real localhost media 500 failure is scheduled and then recovered after database reopen, with saved bytes/hash/state checked.

Healthy localhost text acquisition: 480 captures, 480 retained/scanned documents and 480 persisted scores. Every retained file was SHA-256 checked. Baseline acquisition was 7.997 saves/s; patched acquisition was 7.999 saves/s. Patched early/middle/late segments were 8.019, 7.989 and 7.989 saves/s. Peak process memory sampled at those boundaries was approximately 39.2–39.8 MiB; process CPU time at the last boundary was 3.42 seconds over 60 seconds elapsed. This fixture has small bodies and includes its local HTTP server.

Healthy localhost media acquisition: 240 payloads of 65,536 bytes, nine configured workers, 0.4-second fixture latency and 0.125-second request spacing. Every saved file/hash/size/state was checked. Baseline and patch both averaged about 7.91 successful saves/s including startup/drain; middle/late windows were about 7.99/s. The patch synced files before recording success.

The synthetic SQL benchmark contains one million text-history captures, 999,980 document records, a selected 20-capture tail and 100,000 media rows. It checks complete ordered queue membership and reports early/middle/late CPU/memory measurements. This is SQL scheduling evidence, not one million completed network downloads. The new queue replaces repeated sorting with a single disk-backed order; its JSON includes the measured setup cost as well as traversal cost.

The required offline benchmark also ran with 1,000 CDX rows, 100 report rows and 1,024-byte bodies, including 5,000 keyword patterns. Release metadata/installer/workflow verification, compilation and patch applicability checks are included in the validation record.

## Unverified and remaining limits

- No day-long/overnight live Wayback run was performed. The localhost tests do not establish long-run live-service consistency or prove that the user's observed 6-to-4/s decline has been eliminated.
- The user's actual failed payloads, expected/calculated digests and response headers were not supplied as diagnostic samples. The repeated digest mismatches are not explained away or marked successful. The new diagnostics make that investigation more concrete.
- No native Windows filesystem denial, macOS/Windows GUI session or newly frozen executable was exercised. The four skipped tests require a desktop display. Permission failures were injected on Linux.
- The new embedded page pipeline has transaction rollback, failure/reopen and one-time revalidation tests. It was not exercised against a multi-million-timestamp live embedded URL.
- Existing completed text checkpoints are not blanket-reset or silently re-certified by this patch. Historical source provenance and service evidence still matter when investigating a suspected old incomplete index.
- Media signatures still include some transport layout settings. Changing those settings may require extra indexing/adoption; this patch avoids a broad semantic identity migration.
- No new full-file rehash is imposed on every existing media asset at startup. The existing explicit project integrity operation is the tool for detecting subsequent deletion or bit corruption. Acquisition completeness and an exhaustive later filesystem audit are different claims.
- Permanent exclusions and unavailable captures remain visible incomplete obligations. A network failure cannot be repaired by changing progress accounting. Persistently invalid representations may still keep recovery active until the service returns valid evidence or the user stops the run.

## Applying the patch

Close Scout. Extract the ZIP. Merge the contents of `PATCH_FILES` into the exact repository above, preserving the relative directories and replacing the listed files. Include the new `archive_scout/media/embedded_pages.py`; copying only replacement files would omit the new all-snapshot pipeline. Restart or rebuild using the repository's usual process. Do not delete project databases, captures or media directories.

The root README and file manifest give the same instructions. `PATCH.diff` is the equivalent unified patch. Validation checks its applicability against the exact clean baseline and byte-compares the resulting files with `PATCH_FILES`.
