> Historical v1.2.5 checkpoint. For current cumulative behavior and audit disposition, see V126_AUDIT_IMPLEMENTATION.md.

# v1.2.5 selected scope, earliest media and acquisition follow-up

This follow-up implements the user's selected-sites-and-dates policy for Rescan and embedded-media discovery. It also closes related false-completion paths and removes unnecessary acquisition dependencies. Public version remains **1.2.5**, database schema **10**.

## Behavior

- Rescan, saved-body backfill, embedded discovery and their completion checks use the selected target membership, semantic CDX signature and explicit date bounds. Uncollapsed range signatures intentionally omit dates, so the date predicate is essential. Per-target overrides and overlapping target memberships are respected.
- A Rescan with no selected sites retains the existing explicit whole-project local behavior. Hitlist remains a whole-project search. Standalone analysis/research behavior is unchanged.
- Saved-page sites define the embedded source corpus. Separate direct-media targets do not redefine those sites. If the saved-page site list is empty, explicitly selected media sites provide the discovery scope.
- Compatible historical scans are reused only after their actual scored corpus passes the updated scope check. A contaminated historical run remains intact; it is not silently reused for a narrower corpus.
- Embedded discovery reads and hashes complete admitted source bytes. Missing, changed, unreadable, or oversized files remain explicit unresolved obligations instead of receiving successful empty discovery checkpoints. Increasing the text read limit permits a previously oversized saved source to be inspected without redownloading it.
- Discovery uses at most eight local workers and a 64 MiB estimated input working budget. Only bounded metadata batches and admitted futures are retained; exceptionally large single admitted files can exceed that estimate. This is not a hard process-RSS cap.
- A versioned discovery identity prevents old global-corpus/truncated-read checkpoints from certifying the new policy. Selected saved pages need local revalidation once. Compatible proven direct-media index checkpoints and their captures are adopted, avoiding a blanket new CDX pass. Historical files, captures, errors and attempts are preserved.
- Optional direct-media indexing runs after text acquisition/scanning. Its failure leaves media unfinished without preventing otherwise admitted text work.
- One operation-owned text client survives recovery batches, preserving its connection pools and backend health. Batch response diagnostics are rebound to the current batch. Direct downloader callers retain self-contained client ownership. Cleanup happens after the operation and on failure/Stop.
- Legacy saved-without-document reconciliation runs alongside acquisition on the local scanner. It walks capture IDs without repeatedly sorting history, materializes only actual gaps, is cancellable through SQLite's progress handler, and shares its completed scope proof back to the owner for subsequent retry batches. A cancelled pass cannot certify completion.

## Earliest-media duplicate rule

**Earliest means the earliest eligible capture of each original media URL within the requested scope.**

Direct and embedded discoveries of the same URL/timestamp share a capture identity. Across timestamps, only the earliest is eligible for a new download. Later candidates are retained as `skipped_strategy`; an explicit retry cannot revive them. Previously downloaded files remain intact.

Different original URLs remain distinct even when CDX digests match. This preserves URL provenance and avoids using a digest collapse to discard an earlier timestamp or another source URL. This patch does not introduce cross-URL payload storage deduplication or merge HTTP/HTTPS aliases by guessing canonical equivalence.

The policy runs both after proven indexing and at download admission, including explicit retries and Resume. User-supplied extra `collapse` parameters are excluded from earliest/latest inventory queries as well as ordinary configured collapses. Older extrema checkpoints affected by an extra collapse require proof revalidation; other proven direct inventories remain reusable. If an exact lookup returns more rows than requested, selection uses their timestamp minimum/maximum rather than assuming returned row order.

## Tests and limits

The package's `validation` directory contains the actual results. The complete suite passed **565 tests**, with **0 failures, 0 errors and 4 display-dependent skips**. Fourteen new regressions cover selected corpora, date bounds, target overrides/membership, old discovery-proof adoption, missing or oversized evidence, changed bytes, client reuse, cancellation, direct/embedded deduplication, explicit retry exclusions and real local HTTP transfer/Resume of earliest media.

Release verification, compile checks and the offline CDX/scanning/report benchmark also passed. Local text and media benchmarks validate every saved body; they do not establish live Wayback throughput or codec playability. Apply verification compares every packaged replacement with the tested source.

The million-row startup fixture contains synthetic saved document records, not a million real historical files. Only its 24 selected new captures are downloaded, hashed and scanned. It measures startup/drain behavior and cannot certify the historical bodies. The revised query was checked after an initial candidate repeatedly sorted history; that candidate is not included.

The startup measurement improved time to first save and slightly reduced total elapsed time, with a lower reported peak RSS, but used more process CPU during asynchronous reconciliation. See the exact before/after JSON. Do not infer a universal CPU improvement or an explanation for all previously observed long preparations from this fixture.

No live overnight/multi-day test, native Windows/macOS GUI test, frozen-bundle test, or production project integrity certification was performed.

## Scope of this follow-up

The previous Completion and Throughput patch is included in the cumulative package. This follow-up addresses selected corpora, media discovery proof, earliest-media admission, text-client lifetime and reconciliation scheduling. It does **not** claim to fix every item in the separate Operations Audit. Research invalidation, analysis publication/decoding, import version identity, merge payload verification, and broader maintenance scaling findings remain recorded there and require their own implementation and validation. Their behavior has not been silently changed in this patch.
