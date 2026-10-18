# Operations and report snapshots

Archive Scout keeps indexing, text acquisition, local scanning, media acquisition, and exports as distinct durable operations. An operation may write successful reports **without** completing every requested download or index scope. The database, CLI and GUI retain the `incomplete` status and unfinished checkpoints in that case.

## Core operation handoffs

- **All:** Recover local retained bodies and missing scores; resume required CDX indexing; download and scan selected text; verify the saved-body scan; write text reports; optionally continue the selected media phase; optionally rebuild Research.
- **Resume:** Keep compatible scan evidence and the exact unfinished supplemental-media phase, including its saved direct/external media settings. Do not convert an external-embed task into broad direct-media indexing.
- **Download and scan:** Use existing indexed captures, score retained text, verify scan coverage, and write reports without fresh indexing.
- **Download only:** Index and acquire text without scoring; if enabled, process selected media; report unresolved work independently.
- **Index only:** Index and classify the inventory, write index reports, retain incomplete state for deferred CDX scopes.
- **Rescan:** Reinterpret retained local bodies with selected rules, without requesting archive bodies.
- **Retry:** Text retry and media retry keep their existing distinct scope and attempt policies. Pending/failed selected media is never labeled successfully acquired.
- **Media only:** Media-index, media-download and media-retry operate without requiring an ordinary text scan and report their own unfinished selection.
- **Report (idle):** Regenerates available scan/index and media reports. Does not index, download or rescan. If no index or scan exists, writes an honest empty inventory summary.

Other analysis, research, hitlist, import, repair, integrity, backup and maintenance operations remain independent. Completed scan evidence is **not revoked** by an export permission, storage or formatting failure.

## Write report now (Reports tab)

This action works while a normal operation is idle, indexing, downloading, scanning or stopping. It does not use the exclusive mutating-operation lock, invoke the normal operation runner, or alter the main Stop event. It reads a single committed SQLite WAL snapshot using a separate read-only connection, bounded per-connection cache, and private disk-backed TEMP tables for link aggregation. It streams report rows rather than materializing the project's matches in memory.

- The active worker's captured project path is used while it runs, even if an editable path field changes.
- The report worker has a separate cancellation token; it never starts scans, indexing, downloading, AI or analysis.
- Snapshots are written under `reports/manual/<UTC timestamp>-<unique id>/`, with rendered output files under `reports/` in that folder and an inspectable `manifest.json` at the top.
- A report is published by renaming the completed private staging folder. A cancelled or failed export leaves ordinary `reports/` outputs alone and never publishes a partial snapshot.
- A dedicated file lock permits one snapshot per project across application processes; it is **not** the mutating-operation lock, so downloads and scans can continue.
- The manifest captures scan/operation statuses, target/date scope, acquisition and inventory counters, and outstanding work. Export success alone never proves archive coverage.
- Very long report readers temporarily pin SQLite WAL history, which can increase temporary disk usage until the reader closes. No database or capture-payload copy is created. Avoid running repeated large manual exports on very low-storage projects.

Historical scan regeneration uses the same worker with an explicitly selected scan ID. A manual snapshot never overwrites newer normal `latest` reports. Normal report-output and column settings apply to media reports as well as text. Disabled outputs are not generated, apart from the ordinary indexing safety inventory when an index report is requested.

## Status and automation

`archive-scout run project.json --mode <mode> --format json` returns `status: complete` or `status: incomplete` (with an incomplete exit code) based on the actual operation row, not just whether a report file was written. JSONL similarly distinguishes an `incomplete` final result. Existing Python callers receive a dictionary-compatible mapping of paths with `.status` and `.message` properties; no stored schema migration is required.

Resumability preserves the schema-9 project, recorded scan scores including zero-hit documents, downloaded bytes, CDX cursors and unfinished media state. Backup projects before first opening them with new application code. The supported database schema remains **9**.
