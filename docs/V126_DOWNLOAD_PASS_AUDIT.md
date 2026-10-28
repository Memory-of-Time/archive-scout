# Archive Scout v1.2.6: download completion and retry fairness

28 October 2026. Against the attached `archive-scout-main(20261028-123128).zip`. Public version **1.2.6**, SQLite schema **11**, unchanged.

The middle ground is a finished download pass with explicit unavailable outcomes, automatic recovery for temporary failures, and an incomplete result for genuinely unresolved work. Resume solely because a capture returned a confirmed 404 does not help. Unanswered indexing and truncated bodies still require an honest incomplete result.

## What the runs establish

The latest acquisition excerpt begins partway through the run. Its final counters show **166,596 newly saved captures**, **15,019 excluded/routed captures**, and **22 remaining captures**, accounting for the selected **181,637**. The final health record lists 22 missing-capture failures and 11 connection failures; the final operation lists 22 unresolved captures. This strongly indicates that the remaining count consists of the recorded missing captures. Their database identities are absent from the logs, so this correspondence cannot be checked row by row.

The acquisition log identifies build `1.2.6:b8807a8b71d7a7e8`; the attached source computes `1.2.6:9266ab794024a109` in this environment. The fingerprint implementation hashes source bytes when available and compiled code in frozen builds, so these values cannot establish or refute cross-build identity. The attachment is the authoritative patch baseline, but an exact binary-to-source match for the logged executable is unverified. The patched source/clean installation computes `1.2.6:67d47888be1f70cc` here; packaged builds may differ.

The displayed acquisition average ends at 7.2 saves/s. The visible portion is not uniformly 7.8/s: median displayed recent rates across three equal save-count bands are **7.6, 7.3, and 6.7 saves/s**, with the last band including the shrinking tail. These bands cover saves 137,964–166,596, not the beginning of the run.

From 03:31:52 to 04:46:58, cumulative mean transfer/write time changes from 0.511 to 0.517 seconds, owner commit/write wait from 0.024 to 0.027 seconds, and publication time from 0.157 to 0.163 seconds. Queue preparation stays 14.781 seconds. These cumulative figures do not isolate why the recent rate falls. CPU, RSS, disk-queue traces, payload-size distribution and the database were not supplied. Pool exhaustion, DB contention, filesystem pressure and scanner saturation cannot be established as the cause. Raising workers or weakening validation would be speculative.

The last text save is **04:43:41**. Downloader/scanner drain continues to **04:46:58**, followed by saved-document coverage verification at **04:46:59**. Text reports are written around 04:47:00–04:47:18. Research indexing runs **04:47:24–05:34:00**, clustering **05:34:01–05:46:20**, and evidence relationships finish by **05:49:03**. These named stages explain much of the later wall time. This patch preserves optional user settings; it does not claim to accelerate Research, clustering, or the uninstrumented final interval before 05:52:09.

The earlier indexing run has **34 retry-budget deferrals**, including **33 at 20:34:55**. Source inspection confirms that a target's timer starts on its own request and continues while other targets run. Large selections can exhaust allowances before another attempt. The supplied HTTP 502/504 failures are server responses; they do not establish local pool failure or successful empty indexing.

## Confirmed defects and corrections

| Finding | Correction |
| --- | --- |
| Every capture error counts as unfinished acquisition, including confirmed missing captures. | Finish the pass when only current, explicitly confirmed missing/excluded/robots-blocked outcomes remain. Preserve their capture/error state and evidence. |
| A target spends its no-progress clock behind other targets. | Suspend text/direct-media indexing timers between scheduler turns. Persist actual exhaustion attempts, active time, previous failure and known HTTP status. |
| Payload recovery expires while waiting for a saved cooldown or completing a large sweep. | Limit consecutive wholly unsuccessful sweeps. Cooldowns do not consume attempts; every selected failure gets a fair sweep. Productive sweeps reset the allowance. |
| Media recovery freezes eligibility before an idle wait, leaving future-due work undiscoverable even after its cooldown. | Refresh eligibility at the start of an idle sweep; keep its cutoff fixed while processing admitted work. |
| Streamed downloads discard the archive-specific 403 reason recognized by in-memory downloads. | Share one rejection classifier. Explicit exclusions/robots evidence is retained; a plain 403 remains an unresolved refusal. |
| Index status lacks selected-target reconciliation and durable reasons. | Show remaining/retrying/cooling/deferred queue counts, selected unfinished-target totals/reasons, and `indexing_status.tsv` when Summary's index-coverage field is enabled. Preserve explanations in GUI dialogs and CLI JSON/JSONL. |

Latest-decision predicates require matching persisted outcome and error evidence. An old missing response cannot hide a new transient failure; a pending capture cannot be settled by stale history. Legacy failures without a proven current decision stay unfinished. Text scope still honors membership, query signature and dates. Media outcomes use the active signature.

## Completion contract

| Outcome | Behavior | Result if it is the only remaining issue |
| --- | --- | --- |
| Validated body | Retain bytes/evidence; scan locally when requested | Finished |
| Known non-text/filtered resource | Preserve configured skip/media routing | Finished for the text pass |
| Confirmed missing capture; explicit archive exclusion or robots block | Keep Errors; no automatic replay loop | Finished pass with unavailable count |
| Connection failure, timeout, 429/503 or retryable server response | Existing fixed pacing/shared gate and automatic retry schedule | Incomplete if recovery is exhausted |
| Truncated/digest-mismatched/unverified body | Reject publication; retry eligible work | Incomplete if unresolved |
| Plain 403, unknown/local error, size or redirect-policy refusal | Preserve held outcome and supported explicit recheck | Incomplete |
| Unanswered/malformed/foreign-scope/incomplete CDX coverage | Preserve exact checkpoints; never infer empty coverage | Incomplete |

Operation completion means the requested pass finished under this contract. The report snapshot's stricter completeness flag still stays false for unavailable failures lacking acquisition/scan evidence; its counts name those unavailable captures. No unavailable capture is counted as a successful save, no error is silently resolved, and no coverage proof is invented.

Indexing keeps the existing attempt ceiling and 30–300-second no-progress allowance for its own active turns. Its own request/cooldown time still counts; other targets' turns do not. Payload recovery permits up to `max_attempts` consecutive unsuccessful full sweeps after the initial pass: normally four recovery sweeps with the default allowance. Fresh-work interleaving and productive-sweep resets remain. This is bounded recovery, not a promise that every temporarily failing URL becomes available before completion.

## Comparison

The supplied Ra downloader uses fixed **480 requests/minute (8/s)**, **10 concurrent downloads**, and **five attempts** for exceptions/server failures. Its status handler skips inaccessible nonretryable responses and separately reports skipped archives versus other errors. This supports pass-with-unavailable-outcomes semantics. Its full-response `BytesIO` buffering, large task batches and blocking 429 sleep would be poor replacements for Scout's bounded streaming and shared gate.

The locally available v1.0.8 source counts failures among settled attempt outcomes and marks acquisition operations complete after the pass. It also has lifetime download-attempt selection ceilings. Its simpler completion display does not prove equal acquisition coverage. The locally available v1.2.5.4 loop continues through saved transient cooldowns automatically, without the current bounded unsuccessful-tail policy. This patch combines autonomous eligibility, bounded failed sweeps and modern integrity safeguards. Neither historical version was rerun against the live service.

Hartator's popular downloader documents latest-file selection by default, optional all-timestamp acquisition and configurable concurrency. Its default workload is not automatically equivalent to Scout's selected capture inventory. This comparison uses its [public documentation](https://github.com/hartator/wayback-machine-downloader), not a live benchmark of that program.

## Validation

The canonical suite and fresh patch application are checked: **705 tests run; 701 pass; four skip for missing desktop display; zero failures/errors**. The 22 added tests cover normal-pass completion, retained Errors, latest outcome authority, target/date scope, strict snapshots, GUI/CLI explanations, text/media fairness, inherited cooldown recovery, Stop, unverified indexing, real local HTTP 502-to-200 recovery with a permanent 404, byte identity, scan persistence and streamed 403 classification.

A deterministic before/after reproduction uses forty targets, ten-second virtual requests, one HTTP 504 failure per target and a succeeding second attempt. Baseline: **38 targets get only one attempt and two finish**, for both text and direct-media indexing. Patched: **all forty receive a second attempt and finish**, for both paths.

Two sequential local HTTP runs use real streaming acquisition and its independent scanner, 720 bodies of 4,173 bytes, and 0.125-second pacing. Both hash-verify all bodies and persist all scan outcomes.

| Measurement | Baseline | Patched |
| --- | ---: | ---: |
| Acquisition saves/s | 7.992 | 7.992 |
| Early / middle / late segment saves/s | 8.002 / 7.986 / 7.988 | 8.004 / 7.985 / 7.987 |
| Total elapsed seconds | 90.100 | 90.101 |
| CPU seconds by final save | 6.741 | 6.581 |
| Final observed peak RSS, decimal MB | 45.62 | 44.07 |
| Threads at each measured segment | 14 | 14 |

This shows no healthy-path regression in this workload. Small CPU/RSS differences do not establish a general improvement. Ninety-second loopback runs do not establish overnight live-service consistency.

A synthetic SQL benchmark grows a project through **100,000 / 500,000 / 1,000,000 captures**. Outcome accounting takes **0.051 / 0.243 / 0.582 seconds**; exact dashboard refresh **0.066 / 0.328 / 0.767 seconds**. Temporary scope storage stays **8,192 bytes**. Process peak RSS is **143.73 / 351.65 / 390.59 MB**, including SQLite caches/mappings and database construction; the final database is 459.49 MB. The code creates scalar counts and bounded scope rows, not a Python capture inventory. These are linear stage-boundary/explicit-refresh costs, not new per-save work. No persistent index or schema changes are introduced.

Checks: dependency installation, final clean-target package installation, release metadata/workflow verification, canonical suite, required 1,000-row/100-result offline benchmark, forty-target before/after reproduction, both 720-save local HTTP runs, million-capture accounting benchmark, compile checks, and clean-baseline unified-patch/overlay hash verification. Commands and raw results accompany the patch.

The first full suite found a report-selection regression, which was corrected. Three initial errors came from a host-injected SOCKS proxy without its optional client dependency; final fixture-only suites remove proxy environment variables. No product dependency or user proxy setting changed. Four real-display tests remain skipped; GUI event/dialog behavior is tested with mocks. Windows/macOS/frozen builds, power loss and a days-long live Internet Archive run remain unverified. The user's actual database/payloads were not provided, so the original runs cannot be replayed exactly.

## Apply

Stop Scout. Extract the patch ZIP over the attached source repository root, the folder containing `pyproject.toml`, and replace listed files. Reinstall with `python -m pip install .`, or rebuild through the existing platform workflow. Source changes do not alter an already built executable. Open the same project/settings. Query signatures and schema are unchanged; no full reindex is required.

The ZIP contains replacement/new files, not a complete repository. `PATCH_MANIFEST.json` gives original/replacement SHA-256 hashes. The equivalent unified patch is an alternative; use one method once. Preserve unrelated source edits when applying to another tree.

Source ZIP SHA-256: `e239e94126bcd5aa5420ebbefb7c2f196efbbdb6b74c7d1bacc3e7f7742265e3`.

Ra ZIP SHA-256: `42eb69d062f2b67c16d8506dc9c01885e9d12eda936bf0dc5ea285134ecb26cc`.

Run-log SHA-256: `16a455f9f4fb074db0aa80753f41f46006b03d144553db2707aa275748c74ab7` (latest acquisition); `d7b894b185437f5c4e58c46a4a35f0b808e632fd3f3c31b51347713c60249aa8` (indexing).
