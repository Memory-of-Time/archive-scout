# Scout 1.2.1 — Complete repository

Full source release for `Memory-of-Time/archive-scout`, assembled from the v1.2.0 repository and cumulative v1.2.1 operation/CI/completeness patches. The v1.2.0 project schema remains **9**; installed source version is **1.2.1**.

## Resumability and integrity

- Durable CDX cursor/SQLite write consistency in text and media indexing; committed Timemap page coverage remains verifiable.
- Encoded CDX response handling (including bounded gzip/UTF-16); malformed or truncated responses cannot silently count as complete inventory.
- Gateway 504 retry recovery keeps the saved checkpoint intact, with service-oriented subdivision rather than blindly multiplying requests.
- Interrupted streamed-media Range continuation validates range offsets, totals and identity before publishing bytes; unverified partial bodies are never treated as complete.
- Media basenames cannot overwrite or wrongly reuse other archived captures without digest/identity validation.
- One-pass recovery of temporary text failures; explicit retry of captures that have exhausted the ordinary attempt ceiling.
- Resume can reuse matching **interrupted scan runs** and preserve their existing match history; unrelated scopes create new scan runs.
- Safeguarded project backups, relocation of verifiably project-owned paths, crash recovery for in-flight states and per-operation diagnostics.
- A read-only project upgrade checker and a v1.2.0 schema-9 compatibility regression suite.

## Performance and compatibility

- No change to the standard 0.125-second replay start spacing, established worker concurrency or user-saved CDX pacing settings.
- No additional HTTP requests in the healthy download or CDX indexing path. Safety checks in this release run only when recovery, compatibility or an existing media file warrants them.
- Python source targets >=3.11. The GitHub Actions suite covers Python 3.11 and 3.12 across Ubuntu, Windows and Intel/Apple Silicon macOS. Local Linux CI-script test results accompany this artifact; hosted cross-platform results must be checked after uploading.

## Safety limitations

- Legacy schema-13 databases are **not** supported by Scout 1.2.1.
- Failed partial HTTP responses with no validator cannot always resume safely; affected captures remain retryable.
- Index completeness still depends on actual responses from the Wayback Machine. A successful local fixture test cannot guarantee remote archival availability or sustained 8/s throughput.
- Do not overwrite a running 650,000-URL project, change its indexing constraints during upgrade, or copy a live SQLite WAL database by copying only `.sqlite3`.

See `docs/UPGRADE_1_2_0_TO_1_2_1.md` for a safe upgrade procedure and preflight commands.
