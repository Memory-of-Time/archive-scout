# v1.0.9 validation scope

The validation archive contains `VALIDATION.json`, final logs, scale/fault results, checksums and runnable harnesses. Counts/durations describe executed local checks. Included workflows are configuration, not evidence of remote matrix execution.

Local checks install v1.0.9 into an isolated environment, verify dependencies/imports/metadata, compile source/tests/scripts, check synchronized canonical workflows, run full unittest discovery, execute complete CLI import/process scans with Unicode/JSONL checks, and run the 100,000-row offline benchmark. Delivery also verifies ZIP CRC/inventory/checksums, exact baseline hashes, failed-preflight preservation, apply/idempotence and tests from the applied checkout.

Final source discovery ran 537 tests: 536 passed and the one native-display theme test was skipped. Regressions cover strict/partial/legacy decoding, typed errors, actual stalled headers/bodies and reused sockets, cancellation ownership, drained-pool renewal, local/manual retries, immutable imports, backup faults/cancellation, SQLite/FTS, process/thread parity, oversize completeness, heavy regex Stop/resume, Hamming oracle recall beyond the former cap, transactional analysis cancellation, line-preview counts, bounded metrics/history and recovery clocks. Ten additional restore tests cover committed source WAL, pinned snapshots, active readers, file identity, safety data, lock waits, different page sizes, interrupted-copy rollback, validation and cleanup. Two additional delivery tests cover known earlier-candidate upgrades and rejection of edited candidates. Existing migration, read-only CLI, index/report, media, retention, review, research and provider-mock coverage stays in full discovery.

Scale tests use independent processes, closed seeds and actual payload files where stated. Scan results are rejected if checkpoint/reopen/FTS checks fail. Some tests run concurrently; filesystem caches are uncontrolled. Worker-owned shared peak measurements supply child memory because Linux namespace limits prevent reliable `/proc/<child-pid>` sampling. Conservative sums are labeled separately.

The final 33,000-file repeats took 35.34 seconds with automatic four-process scanning and 56.18 seconds with two processes. Both produced the exact expected digest and 66,000 match records, closed all validation connections, and verified a zero-byte/absent WAL. A separate delayed process checks ordinary database access with both SQLite 3.53.1 and 3.45.1, foreign keys, the complete result digest and FTS content integrity. The full local-hash Research Intelligence build indexed all 33,000 documents in 148.66 seconds; an unchanged refresh took 0.14 seconds. Existing deterministic matches remained byte-for-byte identical.

A separate run excludes both native parser and matcher accelerators in the parent and spawned workers. Its complete 33,000-file scan took 35.23 seconds, produced the same digest, and passed delayed ordinary-database checks with both SQLite runtimes. This exercises Linux source-runtime fallbacks, not a native or frozen-platform build.

Independent before/after probes reproduce v1.0.8 restore's stale-reader/file-replacement and omitted raw-source-WAL defects and verify the corrected behavior. Restoring the complete 33,000-document project took 6.32 seconds: full captures/documents/matches/reviews/notes digests matched the backup, active readers preserved their existing view until their next transaction, and the safety snapshot retained newer user edits. Integrity, foreign keys, FTS content, checkpoint and ordinary reopen passed. These confirmed restore issues do not establish the origin of the excluded conflicting-WAL fixture.

The complete-group duplicate oracle additionally covers later bridges and exact-identity links at radii 1/3/6/32. On a controlled dense fixture of 2,000 distinct two-bit SimHashes, skipping redundant edges after all inserted fingerprints become connected reduced clustering from 25.44 to 0.56 seconds. This times clustering of synthetic fingerprints, not full real-document fingerprinting; no candidate sampling is introduced.

An earlier 41.73-second scan repeat is **excluded**: ordinary access later found corruption with its existing WAL, while a forensic main-file-only view was intact. Both SQLite runtimes reproduce the disagreement. Its root cause is unproven; mapping off is not a complete corruption fix. The validation archive includes the read-only review and the unchanged main/WAL hashes. No project WAL is discarded to obtain a passing result. Fresh harnesses reject database overwrite, explicitly close readers, roll back FTS checks, assert complete checkpoints and independently check the closed ordinary database. Longer packaged-runtime validation remains necessary.

The healthy soak uses production pacing, local HTTP, unique complete payload files and exact byte/digest/manifest verification. Faults are real partial transfers and HTTP 429. The large-project acquisition fixture adds two million already-excluded historical metadata rows before real acquisitions; those historical rows are **not** downloaded payloads. The separate manifest benchmark likewise measures metadata, not two million network saves. Integrity's memory fixture has pending metadata and no retained payload corpus.

Environment: Linux x64, CPython 3.12.14, SQLite 3.53.1, installed native accelerators. One native-display GUI test is skipped. No live Internet Archive, overnight user session, native Windows/macOS build, signing, interactive GUI or remote GitHub success is claimed. Python 3.11 and frozen-runtime checks remain in the supplied platform workflows.

## Audit traceability

| Finding | Patch | Validation |
| --- | --- | --- |
| F01 encoding as network failure | Shared strict policy; typed payload errors | Charset/BOM/Cyrillic/prefix tests; healthy backend continues |
| F02 misleading speed | Fresh committed rolling rates; separate adoption/wire/errors | Virtual clocks, bounded history, paced files |
| F03 header cancellation | Request socket abort; drained pool renewal | Actual HTTPX/urllib3/curl stall/reuse tests |
| F04 retry worker mismatch | Shared scanner resolver | Selection and CLI process tests |
| F05 local retry cooldown | Local phase before network admission | Saved-body and saved-cooldown regressions |
| F06 unavailable recheck | Explicit manual flag/UI | Eligibility/history regression |
| F07 mutable imports | Immutable sources/version identity | Bytes/mtime/idempotence/review/FTS tests |
| F08 metadata memory | Streamed/spilled selections/integrity/maintenance | Million-row Integrity; two-million-row selectors |
| F09 missing duplicate candidates | Complete Hamming-radius tree; skip only redundant connected-group edges | Pair oracle over 2,003 values; full-group oracle, late bridges, exact links; dense fixture |
| F10 repetitive difference work | Named linear block measure; complete counts/change detection | Repetitive/tiny-edit and cancellation tests |
| F11 ETA reset/future phases | Telemetry exclusion; bounded phase plan/history | Virtual-clock waits and CLI progress |
| F12 partial backup publication | Validate/fsync/atomic publish before prune | Compression failure/cancel/restore/progress |
| F13 database validation failures | Mapping off; runtime diagnostics; durability gates; root cause not established | Fresh 33k checkpoint/reopen/content-FTS and delayed checks; two-million-row exercise; conflicting-WAL repeat excluded |
| F14 unsafe restore ownership | SQLite-owned restore; complete raw WAL snapshot; validated safety copy | Ten regressions, independent v1.0.8/v1.0.9 probes, full 33k table digests and live-reader checks |

## Reproduction

```bash
python scripts/validation/scale_scan.py --repository . --workspace scan-validation --count 33000 --workers 2,4,0 --checkpoint-before-close
python scripts/validation/scale_manifest.py --repository . --workspace manifest-validation --count 2000000 --output manifest-validation.json
python scripts/validation/acquisition_soak.py --repository . --workspace healthy-soak --count 28800 --scan-after --output healthy-soak.json
python scripts/validation/acquisition_soak.py --repository . --workspace fault-soak --count 1200 --faults --overlap --output fault-soak.json
python scripts/validation/acquisition_soak.py --repository . --workspace large-project-soak --count 1200 --prefill 2000000 --scan-after --output large-project-soak.json
python scripts/verify_packaged_scan.py --executable PATH_TO_ARCHIVE_SCOUT_CLI --encoding-stress
```

Use fresh fixture directories. The scan harness creates complete files and a closed seed; worker copies are independent. The healthy soak takes about an hour at the unchanged 8/s floor. No command contacts Internet Archive or an AI provider. Inspect Integrity runner `--help` for its runtime/output paths.
