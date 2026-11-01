# Archive Scout v1.2.7 pipeline audit

Audit date: 1 November 2026. Public version is now **1.2.7**; SQLite schema remains **11**.

The changes address demonstrated embedded-media overhead, prolonged failed CDX responses, unnecessary discovery writes, and confusing automatic Research Intelligence work. The capture upsert slowdown and the long text completion tail remain partially unresolved: the supplied logs identify them, but do not establish a single cause. The patch adds useful timing evidence for the tail instead of claiming a speculative fix.

## Source and evidence

The base is [Memory-of-Time/archive-scout commit 5f5a02c71f8277eccdb7877a520f7707179eb9f4](https://github.com/Memory-of-Time/archive-scout/tree/5f5a02c71f8277eccdb7877a520f7707179eb9f4). Every one of its 309 tracked file contents was compared against the last delivered single-pass media restoration. They matched. The remote head was checked again before packaging and was unchanged.

The current source therefore retains the requested 1.2.6.3 media restoration and its focused indexing-completeness exception. This patch is an addition to that base. It does not restore the rejected per-URL media admission redesign or a mandatory second numbered traversal.

Inputs were reviewed separately:

| Input | What it establishes |
| --- | --- |
| `Pasted text(20261101-192021).txt`, 4,544 lines | Latest text tail, optional research costs, successful Mediabum media acquisition, later broad CDX failures and increasing Sohu batch-write times |
| `Pasted text(20261029-213118).txt`, 4,834 lines | Older media acquisition: saves continue through recovery; final status remains incomplete; connection and local-processing failures occur |
| `Pasted text(20261029-213424).txt`, 4,679 lines | Separate older text run with approximately 6.9 saves/s invocation average, connection failures and an explicit user Stop |
| Two 28 October logs | Earlier retry-budget behavior and another substantial post-acquisition Research Intelligence phase |
| Supplied Ra downloader ZIP | Its CDX resume traversal and ten-way Timemap overlap, local filtering, fixed admission and explicit failed-page accounting |

The logs have different build fingerprints and projects. Their counters are not combined into one run. No current project database or disk trace was supplied. Fingerprints from source and frozen builds are not assumed to be interchangeable.

## Findings and implemented changes

### 1. Research Intelligence must be an explicit optional cost

In the latest log, line 771 starts research indexing at **00:13:28** for 46,627 documents. Vector/entity indexing finishes at 00:27:09, duplicate clustering lasts until 00:32:48, and relationships finish before reports at 00:33:36. That is approximately **20 minutes 8 seconds** of optional post-scan work. It is separate from downloading.

The dataclass, omitted-config loader, CLI initialization and GUI variable already defaulted automatic indexing to false. However, the example project explicitly set `auto_build: true`. Existing saved projects could also explicitly enable it. Treating all of this as a missing default would misdiagnose the source.

Implemented:

- The example project now uses public version 1.2.7 and `research.auto_build: false`.
- The checkbox remains in **Research intelligence**, now clearly labelled as optional, with an explanation of vectors, entities, duplicate clustering and relationships.
- Normal scan completion explicitly reports whether automatic research indexing is enabled or skipped.
- Manual **Build / refresh index** and CLI `research-index` continue to work while automatic indexing is off. Research search uses the existing built index; it does not silently build one.
- An explicitly saved `auto_build: true` remains enabled. To disable it in an existing project, uncheck the box and save or run the project. New and example projects are off by default.

Regression coverage runs an ordinary rescan with automatic building forbidden, then manually builds and searches the same corpus. A separate case verifies that explicit opt-in still builds the index.

### 2. Long failed CDX bodies should return to the scheduler

The latest log's Sina request spends **936.1 + 194.9 + 928.8 seconds**, approximately 34 minutes 19 seconds, crossing three endpoint paths after incomplete chunked bodies (lines 4499–4501). These are transfer failures. There is no demonstrated HTTP 429 in this latest log, so rate limiting is not an established explanation.

Read timeouts limit inactivity during a read; a trickling body can continue much longer. The original large read aggregation also made cancellation checks infrequent. In addition, incomplete representations were returned as generic transient failures that could repeat the same expensive query through more endpoints.

Implemented:

- Built-in CDX transport requests check an active response deadline based on the existing client read timeout. Production CDX clients currently clamp that timeout to 30–120 seconds.
- The deadline starts after backend admission and request pacing. It applies to known CDX/Timemap request paths, without imposing a total-duration deadline on replay file downloads.
- HTTPX exposes incremental reads for these requests. urllib3 uses its public `read1` API, available at the project's supported minimum urllib3 2.2 version. Curl uses its total-time limit.
- Partial deadline bodies are discarded. The owner retains its saved page/cursor and decides whether to retry, subdivide eligible work or yield to other targets.
- An actual incomplete transport body returns directly to the CDX owner instead of trying another format and every endpoint. Native JSON, compact rows and legacy JSON paths follow the same rule.
- An interrupted urllib3 request is closed and released without draining its remaining body. This also fixes needless waiting on rejected redirect bodies and failed replay transfers. Fully consumed successful responses still release their reusable connection.

The deadline is cooperative for Python reads. An already blocking read can extend the observed turn, and compressed decoding can delay the next check. This is not a hard real-time cancellation guarantee. A functioning but very slow CDX query may now need smaller windows or another recovery turn; the patch never calls it an empty or completed inventory. Shared 429/503 controls and configured fixed pacing remain in force.

The real localhost trickle regression passes on HTTPX, urllib3 and available Curl. It deliberately sends bytes frequently enough to avoid an ordinary inactivity timeout, and verifies that the CDX turn fails promptly instead of accepting its partial body.

### 3. Embedded all-snapshot lookups were using domain-sized memory reservations

`index_all_embedded_pages` used the global page size to calculate memory-limited concurrency. With the normal 100,000-row page and 512 MiB allowance, three times the response allowance reserves roughly 439 MiB per worker. The effective pool becomes **one worker**, even when ten CDX workers are selected and each exact URL returns only a few rows.

Implemented:

- Exact all-snapshot embedded lookups start with at most **2,000 rows per page**.
- Their separate body budget has an 8 MiB floor and 64 MiB ceiling. Concurrency reserves three body budgets per worker, including the terminal confirmation allowance.
- With default settings this permits up to ten overlapping exact lookups, within the configured concurrency and memory caps.
- Saved resume keys remain in force. Changing a page limit does not discard previously committed rows. Response-size recovery can still shrink an individual page.
- Each page's media rows and continuation checkpoint commit together. Terminal JSON confirmation, cursor-cycle checks, scope validation, local media policy and prior acquired files remain intact.

This isolates exact-URL work from ordinary domain-media indexing, whose existing larger pages remain unchanged. A single embedded URL with many thousands of captures will require more requests; that is the tradeoff for bounded memory and useful concurrency across many URLs. Earliest/latest embedded lookup already uses limit 1 / reverse limit 1 and is retained.

The reference downloader's useful lesson is bounded overlap and simple local filtering. Its page-count/resume approach is not itself proof that a differently scoped response includes every historical media file. This patch preserves Scout's stronger checkpoint and terminal checks.

### 4. Discovery repeated normalization, classification and unchanged writes

Profiling a duplicate-heavy HTML fixture found repeated URL normalization and media-policy evaluation across links, attributes, player extraction and CSS. Identical candidates were only deduplicated after those costs. Repeated discovery also issued an UPDATE for every known queue entry, even when no field could change.

Implemented:

- A bounded 2,048-entry normalization cache exists only for one document discovery call. Relative URLs remain tied to that document's base URL; there is no cross-document URL cache.
- Duplicate `(URL, hint)` candidates are filtered before media-policy work. A later image/video hint can still upgrade an unhinted candidate.
- Queue enrichment only writes when it fills a missing document, source type or hint. It does not reset state, result counts, attempt history, errors or timestamps for unchanged entries.

Integrity-checked saved-page reads, content-hash discovery checkpoints, allowed extensions, external-host policy and selected saved-page scope remain in force. No parser shortcuts skip the latter half of a saved page.

### 5. Capture upsert growth is real in the log, but not reproduced locally

Three equal 100,000-row Sohu batches report database times of **15.14, 15.66 and 30.59 seconds**. The last batch attributes 28.72 seconds to upsert and 1.88 seconds to commit. This is a concrete investigation target, but three observations do not establish quadratic SQL or identify storage contention.

The existing capture upsert is batched and already suppresses unchanged metadata writes. Target-membership triggers preserve overlapping target ownership. Removing indexes or changing transaction semantics without the actual database could damage other operations or obscure the cause.

A synthetic growing 500,000-capture database did not reproduce the growth: baseline five-batch totals were **3.30, 2.95, 2.68, 2.67, 2.72 seconds**; patched totals were **3.29, 3.25, 2.67, 2.95, 2.69 seconds**. These include fixture construction and commit, unlike the log's isolated timer. The main capture SQL is unchanged. This result does not resolve performance on the user's Windows project, disk, indexes or WAL workload.

### 6. Text acquisition tail needs broader owner-stage evidence

At 00:06:53 the latest log reports zero running workers and 19 completed results awaiting the owner, yet mean commit/write wait is only 0.037 seconds. Other health lines show several minutes of slow drainage. Those averages do not establish database contention. The source already waits for the first completed future and processes ready results; a generic head-of-line rewrite would not address a demonstrated defect.

The separate 29 October text log ends immediately after an explicit Stop, also with completed results waiting. That final Stop snapshot is not evidence of a deadlock.

Implemented text health now reports mean and maximum durations for completed-result wait, owner outcome handling, queue refill and progress callbacks. The existing commit and publication measurements remain separate. Tracking is bounded by the existing in-flight work; it creates no observer thread or project-wide per-save query. A regression verifies that fast siblings commit before a slow first request.

The new local paced text fixture drains scanning in about 0.020 seconds after its last save. It does not reproduce or prove the cause of the multi-minute real-world tail. Payload digest failures remain retryable validation failures; repeated attempt counts are not unique permanently lost captures.

## Interconnection and completion review

| Path | Retained contract and evidence |
| --- | --- |
| Text indexing → acquisition | Selected target/date/query coverage remains distinct from payload state; failed pages do not become terminal proof |
| Text retention → local scan → reports | Complete bytes retain SHA-256 evidence; local scoring/domain extraction remains independent of network workers; the paced fixture saves and scans all 120 documents |
| Direct media + saved-page discovery → media acquisition | Existing single-pass direct-media path remains; embedded page commits are independent, bounded and restartable; existing ordinary media integration cases still pass |
| Download outcomes → Errors → completion | Confirmed unavailable captures remain visible without requiring Resume; retryable payload/indexing gaps remain incomplete |
| Resume → saved operation intent | Existing selected direct/external phase, scope, body identity and cursor tests still pass; no schema migration or checkpoint revision reset is introduced here |
| Research → reports/search | Automatic work is opt-in; explicit manual build/search and saved opt-in both pass integration tests |
| CLI and desktop | Installed CLI starts successfully; machine-readable output contracts remain covered by the suite; the Research-tab GUI test is added to Linux CI's virtual-display step |
| Maintenance and analysis | Existing migration, backup/restore, project merge, import, read-only report snapshot, scoring and derivation tests pass; this is regression coverage, not a live-service certification |

Latest media acquisition itself works: the Mediabum phase indexes 61,580 raw rows, queues 4,746 selected captures, saves **4,733**, and records **13 HTTP 404 unavailable captures**, then reports completion. Accepted/stored/selected counts differ by filtering, identity deduplication, prior state and snapshot policy; those differences alone do not prove missing URLs.

The older media log separately reports 20,883 saves, six unavailable outcomes and an incomplete final state, with connection and local-processing failures. Its attempt counters cannot identify the exact remaining capture set without its database. The later latest-log HTTP 400 has no server explanation sufficient to distinguish request length, filter syntax or another rejection. Neither issue is relabelled successful by this patch.

## Measurements and validation

All measurements below are synthetic local fixtures. They are not live Wayback throughput or completeness promises.

| Measurement | Before | Patched | Check |
| --- | --- | --- | --- |
| Three unprofiled 100-page discovery runs, median | 8.992 s | 3.989 s | About 2.25× faster; identical URL/hint membership |
| Same discovery with cProfile enabled | 35.283 s | 12.702 s | Profiled timings only; not the unprofiled speed claim |
| 40 rediscoveries of 2,000 existing queue URLs | 80,000 updates | 0 updates | Completed queue state and enrichment behavior preserved |
| 30 exact embedded URLs, three captures each, 60 HTTP requests | 3.723 s; peak 1 active request | 0.400 s; peak 10 | About 9.30× faster with pacing disabled; identical 90 URL/timestamp/digest identities and all 30 queues complete |
| Paced ordinary media replay, 120 × 64 KiB | — | 7.836 verified saves/s | 0.125 s starts; 400 ms response latency; every file SHA-256 checked |
| Paced text replay + scan + domains, 120 files | — | 8.025 verified saves/s | All 120 scores persisted and requested domain extraction complete |

The embedded benchmark intentionally disables request pacing to isolate concurrency. With default CDX pacing of 1.5 seconds, starts are limited to about 0.67 requests/s; the same 60 requests have a start-spacing floor of roughly 88.5 seconds. Extra workers chiefly help when latency exceeds that spacing and available concurrency. The patch does not remove terminal checks or promise the local 9.30× gain on live service.

Media replay resource samples rise from approximately 37.6 to 38.3 MiB peak RSS across the three segments; text samples rise from 47.3 to 49.0 MiB. These short high-water measurements do not prove overnight memory stability.

Validation completed:

- Canonical suite: **735 run, 730 passed, five skipped, zero failures/errors**. The skips require an unavailable desktop display.
- Fourteen pipeline regression cases include one display-dependent Research-tab case. Thirteen execute locally, plus three release-compatibility/metadata cases.
- Source compilation and release metadata/assets/workflow verification pass.
- Wheel builds and installs into a separate directory; all 126 application files match the source bytes.
- Installed CLI `--help` starts successfully. A GUI launch cannot be exercised without a display.
- Canonical offline smoke benchmark passes, including zero duplicate work and zero unchanged capture writes.
- Patched source build fingerprint: **`1.2.7:fbf6c6821b529fd6`**. A frozen application's fingerprint may differ because it hashes packaged code objects.

Evidence and reproducible drivers are in [`validation/pipeline-audit`](../validation/pipeline-audit/README.md). Native Windows/macOS builds, a fresh remote CI matrix, the actual project database, live archive exhaustiveness, and an overnight run were not exercised here.

## v1.2.7 release identity

The final release is labelled v1.2.7 in the application, Python distribution, example project, default client identification, Windows numeric/string version resources and macOS bundle metadata. SQLite remains schema 11. Historical v1.2.6 audit filenames and recorded logs remain historical references. Existing custom client identifiers are preserved; the known stock v1.2.6 identifier follows the new version. A regression prevents a deliberately saved v1.2.6 CDX delay from being mistaken for an old default merely because the public version changed. The macOS plist update is executed against a fixture during testing; native platform builds remain untested here.

The throughput measurements above were taken with the audited pipeline before the final release-label update. The same pipeline implementation is included in v1.2.7, and the final suite and installed-package checks are rerun for that release.

## Applying this delivery

Use either the patch archive against the base above or the complete repository archive. Do not combine the complete repository with older partial overlays. Stop Scout before replacing source or rebuilding; retain and back up project data.

The patch archive contains replacement/additional files under `files/`, a unified patch, hashes and an optional guarded installer. Copy `files/` contents into the repository root preserving directories, or run its installer as documented in the archive README. **No repository files need deletion for this base.** Do not delete project databases, captures, media, reports or checkpoints.

The full repository includes runtime source, tests, assets, dependency metadata, platform build scripts, workflows, documentation and audit evidence. It is source code, not a newly compiled native executable. Install the package or rebuild the desired platform so the executable actually contains the patch.
