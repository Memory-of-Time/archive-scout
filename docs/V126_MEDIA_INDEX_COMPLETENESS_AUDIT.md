# Archive Scout v1.2.6 — media indexing completeness audit

October 29, 2026. Public version 1.2.6; SQLite schema 11. Add-on to the previous download-pass patch and HTTP 403 add-on.

## Finding and scope

An omitted media URL never enters the download queue. Download retries and error accounting cannot recover a URL the indexer did not discover. This audit therefore follows media query construction, remote filtering, pagination, local acceptance, database insertion, coverage reuse, and restart checkpoints.

The final patch targets **media indexing only**. Normal text indexing retains its request path, completion rules, settings, and checkpoints. Two shared CDX files gain opt-in media verification and media checkpoint fields; those fields do not enable a text-index repair. A previous broader draft and its text-index timings were superseded after the scope clarification and are not validation of this add-on.

The code contains reproducible omission paths, but the supplied logs contain acquisition excerpts rather than CDX response transcripts. No project database or specific missing-URL list was supplied. Consequently the fixtures establish that these mechanisms are fixed; they do not establish which mechanism explains every user-observed omission.

## The two logs remain separate evidence

| Observation | Media log | Text log |
| --- | --- | --- |
| Attachment | Pasted text(20261029-213118).txt | Pasted text(20261029-213424).txt |
| Recorded build | 1.2.6:62cd5b3dd1df6ada | 1.2.6:8b1315cef8fb844e |
| Excerpt | 16:34:47–17:09:00 | 16:38:58–17:34:02 |
| Final new-save counter | 20,883 | 55,187 |
| Ending | Reports written; incomplete message, six unavailable media captures | Explicit Stop, progress saved |
| Connection-fallback notices in excerpt | 284 | 174 |

The fingerprints differ, and neither identifies this patch's assembled source. Recorded times are reproduced without assuming a log time zone. Accumulated failure events can represent repeated attempts, not distinct unresolved URLs.

The media run saved 19,903 before its recovery phase and ultimately reached 20,883: **980 additional successful saves**. Removing recovery would lose working downloads. Its final counters record 1,182 connection failures, 32 local-processing failures, and six missing-capture events. Those categories do not prove indexing coverage or identify the underlying local exceptions.

The initial media attempt batch contained 20,921 outcomes, whereas the final new-save counter is 20,883. That difference is not an index-missing list. The excerpt records six missing-capture events and 32 local-processing events, but events cannot be assigned to final distinct capture states without the database. These counters describe already discovered work and do not prove which additional URLs were never indexed.

The text excerpt is retained as comparison context. Its first/last health records report mean transfer/write time 0.748→0.730 seconds and mean owner commit/write wait 0.017→0.017 seconds. They do not establish database contention as the cause of missing media URLs. Its final 19 completed results awaiting the owner are observed at Stop, not proof of a sustained backlog. This patch does not change text acquisition or its queue preparation.

## Confirmed discovery defects and changes

### 1. The remote media filter could exclude locally eligible URLs

Media discovery used an original-URL extension regex. The local classifier also accepts MIME-identified media without a suffix and decodes extensions before testing them. The remote query could therefore hide an extensionless image or an encoded suffix such as `photo.%6a%70%67` before local classification ran.

The patch sends one full-record superset expression covering selected literal/encoded suffixes and supported image/video MIME families. Local include/exclude and media-kind rules remain authoritative. Explicit status, URL, target, and date filters are preserved. This does not turn the extension allow-list into an unrestricted download policy: a URL with a disallowed explicit suffix remains excluded by the existing local rules.

The generated expression deliberately contains no colon. The upstream CDX parser treats the first colon as a field-selector delimiter, including a colon inside a noncapturing regex group. Actual Java regex checks accompany Python policy regressions. A broader remote predicate can return more candidates; that is necessary where the previous predicate suppressed eligible media.

### 2. A short response without a cursor could falsely finish media coverage

The media resume path previously challenged missing continuation only when a batch filled the requested limit. A cleanly shortened or empty response could instead settle coverage. Future runs then trusted the checkpoint and never searched for omitted members.

Media requests now opt into terminal verification in the built-in compact CDX client. It repeats the same scope/input cursor as native JSON, retaining membership and any recovered continuation. Conflicting prefixes are rejected. A full batch uses an enlarged limit check. Productive cursor-bearing batches do not add confirmation requests.

This behavior defaults off in the shared client. Normal text requests continue through their previous path. Legacy integration clients returning already parsed CDXRows retain their adapter contract and are responsible for the evidence they supply.

### 3. Agreement between two formats is not an exhaustive traversal proof

In the upstream CDX implementation, scanned-input and written-output limits differ. Filtering/collapsing can exhaust scanning without filling the output count that triggers a continuation. The same query can then return the same short result in both formats. This is a reference-source mechanism, not a verified deployment fingerprint for every current Wayback endpoint.

Broad media resume traversal therefore also verifies the numbered-page ledger. Conversely, numbered-first media indexing reconciles against merged resume discovery. A page ledger alone cannot establish membership in the merged service view. Every numbered page is visited, including empty filtered pages; emptiness in an intermediate page does not end indexing.

The two phases and their transition are durable. Verification flags advance only in the transaction that commits the corresponding media rows. An injected database write failure cannot publish a verified phase or lose its input cursor. If numbered verification is blocked or unsupported, coverage stays incomplete instead of treating repeated short responses as proof.

This increases media indexing work. It does not change replay pacing, local scanning, network cooldown policy, or download validation. The two service views can select different historical representatives; the conservative union retains evidence rather than deleting previously indexed captures to force equal counts.

### 4. Completed text coverage could incorrectly certify media discovery

The previous shortcut could mark media indexing complete by locally filtering a completed text/site index. A collapsed source can omit a qualifying media capture before media filtering runs. Even an uncollapsed older text checkpoint does not establish the new media query's traversal guarantees.

Standalone media discovery now requires its own coverage. The old shortcut returns without certifying media completeness, and normal text checkpoints are neither reopened nor modified. This can cost another media CDX traversal where a previous run reused text results without network access. Already completed media coverage under the new protocol is still reused.

This is conservative: an independently proven uncollapsed source could support a future safe optimization, but importing new proof rules into normal text indexing would exceed this patch's scope.

### 5. Old media checkpoints cannot prove the broader query

Only changing future fetches would leave an already completed old media index untouched. The private media indexing revision therefore advances to 6 while public v1.2.6 and schema 11 remain unchanged.

Old media capture rows, saved files, errors, and historical checkpoints remain. An old cursor is not adopted as progress in the broader query: newly eligible media before that cursor would otherwise remain missing. Existing projects perform a fresh media discovery traversal under the new state revision. With unchanged media query identity, acquired rows retain their IDs, states, and paths; reindexing does not reset them to pending merely to download them again.

New revision-6 cursors and numbered ledgers remain restartable. Stop during the phase transition preserves the verification queue. Earliest/latest timestamp selection retains its existing requirement for complete media coverage, and previous saved evidence is not removed while coverage is incomplete.

### 6. Media traversal parameters and page counts need explicit ownership

Advanced offset/page/pageSize/showNumPages/showPagedIndex values can interfere with the traversal the application owns. The media parameter builder now rejects them with a clear error; use Connection and indexing controls for paging. Global text parameter parsing and settings files are unchanged.

A second metadata defect was exposed during validation: a capture-array response to a page-count request could have its numeric capture timestamp interpreted as an enormous number of pages. Media now rejects capture headers and boolean page counts before accepting any layout. No fabricated page count or empty completion is committed. The shared text page-count parser is unchanged.

## Comparison with Ra and another established downloader

**Ra's supplied program** provides useful comparisons for data-bearing requests and local filtering. Its CDX loop is controlled by a 50,000-result threshold, however, so a shorter response can end traversal despite a continuation. A successful empty response can also leave the previous result count in place before its repeated-query guard exits. These source paths are not exhaustive coverage proofs. Ra's status filter includes 2xx/3xx; Scout's configured statuscode:200 selects a different universe. This audit inspected the supplied code and did not run Ra live.

**Hartator's downloader** combines an initial nonpaged query with numbered prefix pages and acknowledges that the paged view can be less fresh. That supports consulting both views. Its default 100-page ceiling and stopping on an empty page are unsuitable guarantees for Scout's filtered research queries. Its path/timestamp curation also means saved-file counts are not comparable with Scout capture membership. This patch imports neither that page ceiling nor its empty-page stop rule.

**Older Scout versions** may finish sooner by trusting one view or reusing a text checkpoint. A completed progress bar does not demonstrate that omitted media were discovered. The revised checks deliberately measure unique media membership and preserve an unfinished result when traversal evidence is insufficient.

## Validation and measurements

The final scoped suite ran **732 tests: 728 passed, four display-dependent tests skipped, zero failures/errors**, on Linux/Python 3.12.14. The 19 new regression methods exercise missing media, false-empty responses, identical scan-limited replies, merged-only URLs, empty intermediate pages, encoded/MIME eligibility, old media checkpoint replacement, safe write rollback, unsupported verification, and media-specific parameter/page-count validation. Control regressions verify that normal text requests and text checkpoints retain their previous behavior.

Additional checks passed:

- Package wheel built and installed with existing dependencies; all installed application source files matched the tested source; installed version/schema were 1.2.6/11.
- scripts/verify_release.py and compileall.
- scripts/benchmark_offline.py --cdx-rows 1000 --result-rows 100 --body-bytes 1024, including zero repeated no-op capture writes.
- Java 17 regex verification: **214 accepted-media cases**; the tested expressions were checked against the current generated filter.
- Local media acquisition smoke: **90/90 files SHA-256 verified**, 64 KiB bodies, 0.4-second response delay, nine workers, explicit 0.125-second pacing. Overall rate 7.738 successful saves/s; early/middle/late segments 7.302 / 7.985 / 7.989 saves/s. Peak RSS observations were approximately 37.49 / 37.99 / 38.24 MiB. This short check ran alongside other validation and establishes functional behavior, not an isolated speed comparison or overnight consistency.

The media-indexing benchmark used index_media, media_captures, one million generated distinct media URLs and 25,000-row resume batches over localhost. The baseline and patched runs were sequential. Exact expected membership was verified after indexing, including matching URL/digest identities; a progress counter alone was insufficient.

| Measurement | Previous add-on | Media completeness add-on |
| --- | ---: | ---: |
| Distinct media URLs verified | 1,000,000 | 1,000,000 |
| HTTP requests | 41 | 82 |
| Total media indexing wall time | 100.244 s | 149.082 s |
| Distinct media URLs / total indexing second | 9,976 | 6,708 |
| CPU including server and subsequent membership verification | 100.071 s | 150.758 s |
| Peak process RSS including final SQL verification | 394.04 MiB | 445.28 MiB |
| Main SQLite file before close, excluding WAL/SHM | 472.88 MB | 472.88 MB |

The patched run took **48.7% longer** in this pair. The request count doubled because the additional view was traversed and committed without creating another million media captures. This is a material indexing cost, not a throughput improvement. One local pair does not establish a universal percentage, and the unchanged main-file size is not a measurement of total disk space including WAL/SHM.

Resume-stage early/middle/late rates were 10,685 / 7,461 / 13,410 rows/s before and 10,527 / 11,504 / 11,391 after. Corresponding peak RSS observations were 138.37 / 139.74 / 150.65 MiB before and 138.31 / 138.43 / 153.00 MiB after. Those samples describe the resume phase; total wall time also includes numbered verification. The process includes the synthetic HTTP server and final SQLite distinct-membership check. It is not a downloader memory measurement or evidence of a leak.

The second traversal can increase observed/seen counts while the distinct media inventory remains unchanged. Compact rows, existing no-op upserts, bounded page admission and durable ledgers remain in use. The benchmark does not establish a multi-million live-service or day-long resource envelope. The earlier broader draft's normal-text benchmark numbers are intentionally excluded.

The ZIP overlay and unified patch are checked against a fresh copy of the previous add-on. The included test log records every canonical case and skip; scope-check.json records normal-text and acquisition source comparisons. Verification evidence is included under validation/v126-media-index-completeness/.


## Application and remaining limits

Merge this add-on over the previous download-pass and HTTP 403 patches, rebuild, then run media indexing or the appropriate combined media operation on the existing project. The first run rechecks older media coverage. A reports-only/local scan operation does not start media network discovery. Normal text indexing is not reset by this patch.

Neither attachment contains the expected missing media URLs or project database. Whether these mechanisms explain each observed omission remains unverified; compare repaired membership against specific expected URLs under matching dates, target scope, explicit filters, and timestamp strategy.

No live Wayback probe, overnight/day-long run, native Windows/macOS build, or hardware-specific test was performed. Agreement between index views is not mathematical proof of every capture physically held by Internet Archive: access restrictions, changing indexes, shared service defects, and merged-only records beyond an unobserved service scan boundary remain possible. Exact-URL checks can supply additional evidence for known missing URLs. The patch does not fabricate that evidence or claim that every archive record is publicly discoverable.

Connection churn and the media log's local-processing failures still need their actual exceptions investigated. This media indexing patch does not claim to resolve those acquisition failures or establish sustained live eight validated saves per second.

## Primary references

- CDX API, resumption, filtering and pagination: https://github.com/internetarchive/wayback/blob/master/wayback-cdx-server/README.md
- CDX traversal reference implementation: https://github.com/internetarchive/wayback/blob/master/wayback-cdx-server/src/main/java/org/archive/cdxserver/CDXServer.java
- Field-filter parser: https://github.com/internetarchive/wayback/blob/master/wayback-cdx-server/src/main/java/org/archive/cdxserver/filter/FieldRegexFilter.java
- Hartator implementation: https://github.com/hartator/wayback-machine-downloader/blob/master/lib/wayback_machine_downloader.rb
- Ra: supplied wayback-machine-downloader-main(20261028-123139).zip, libs/api/cdx.py and libs/utils/utils.py.

Upstream references were retrieved October 29, 2026. They were inspected as reference code, not asserted to identify the current live deployment.
