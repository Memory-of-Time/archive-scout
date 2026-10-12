# Scout v1.2.1 — Completeness, Recovery, Indexing & Encoded-CDX Patch

## Apply order

This ZIP is a **unified patch overlay**. Apply it over your existing Scout v1.2.1 repository **after the consolidated v1.2.1 patch and the two cross-platform CI hotfixes**. It **already incorporates** the earlier Scout-v1.2.1-Completeness-First-Audit-Hotfix-Overlay; that earlier audit hotfix is **not required separately**. If it was already installed, this ZIP simply replaces matching files with their newest combined versions.

1. Exit every running Scout process. Backup the project with Scout's SQLite backup feature. Do not overwrite your project folders, archive_scout.sqlite3, project.json, captures or media files.
2. Extract this ZIP directly into the *repository root* and replace matching source and test files. No directory is wrapped around the files.
3. Commit the files and run GitHub Actions for the Windows/macOS/Linux and Python 3.11/3.12 matrix. This container was Linux / Python 3.13 and cannot certify hosted CI.
4. Test one saved project offline, then compare a multi-hour healthy and outage/recovery Wayback run against prior v1.2.1.

## Actual modifications (source only)

- CDX JSON and text parsing: tolerate still-gzip-wrapped bodies, native gzip JSON, and UTF-16 JSON; reject corrupt/oversized compression rather than treating it as an empty result. Normal HTTP body handling and CDX URL query encoding are unchanged; if the reported 'encoded request' refers to an encoded URL rather than a compressed body, a concrete failing request is still required to diagnose it.
- Text and media CDX resume cursors advance *only after* the corresponding SQLite rows and checkpoint commit. Errors cannot persist a cursor that skips uncommitted records.
- Timemap paged completion reconciles every numbered page against durable page records; missing pages remain queued.
- Existing media content is re-used only with matching SHA-256/size or CDX SHA-1 digest. Colliding filenames are saved separately and remain in flat images/videos directories.
- Incomplete streamed **media** may be preserved in `.part` plus `.part.resume.json` and continued only with valid response identity and aligned `206 Content-Range`. If Wayback ignores Range, a returned `200` is consumed as a clean full download. A mismatched validator, wrong range, corruption, or unknown identity never causes unsafe append. Partial bytes remain unresolved until a valid full body is obtained. Downloads with no usable identity/length fall back to full retry.
- Normal automatic retries continue to make one fair sweep; explicit retry now also selects indexed pending captures excluded by lifetime attempt caps, in bounded batches.
- Reports disclose attempt-exhausted text/media captures in `unfinished_over_attempts.tsv` and `media_unfinished_over_attempts.tsv`. Extra inventory queries occur at report time, not per saved capture.
- Transport messages distinguish incomplete transfer failures from connection-setup failures.

## Boundaries

There is **no** default rate, worker, CDX spacing, replay spacing, schema, or text scanning change. New HTTP Range requests only occur after a validated interrupted *streamed-media* download, not during healthy acquisition. This patch does **not** add HTTP Range continuation to ordinary text bodies kept in memory. A malformed unknown URL-encoding problem is not established by the supplied reports and cannot responsibly be declared fixed.

Validation: 286 tests passed, zero failures, six skipped (display limitations), on local Linux Python 3.13; compilation and scripts/verify_release.py passed. A deterministic MockTransport test proves interrupted-media prefix retention and byte-aligned Range recovery. No live Wayback throughput or hosted CI verification has been performed.

Use:
```
python -m compileall -q archive_scout tests scripts
python scripts/verify_release.py
python scripts/run_tests.py --output validation/test-results
```

This patch is not for a separate v1.2.2+ repository without reviewing the code differences first.
