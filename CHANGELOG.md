# Changelog

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
