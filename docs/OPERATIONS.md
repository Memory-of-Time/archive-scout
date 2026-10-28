# Operations and report snapshots

Archive Scout keeps indexing, text acquisition, local scanning, media acquisition, and exports as distinct durable operations. An operation may write successful reports **without** completing every requested download or index scope. The database, CLI and GUI retain the `incomplete` status and unfinished checkpoints in that case.

## Core operation handoffs

- **All:** Recover local retained bodies and missing scores; resume required CDX indexing; download and scan selected text; verify the saved-body scan; write text reports; optionally continue the selected media phase; optionally rebuild Research.
- **Resume:** Restore the latest unfinished acquisition, indexing, scanning or media request, including its saved targets, dates, keyword sets and media selection. Acquisition-only remains acquisition-only. Distinct earlier unfinished requests remain checkpointed; completing an intervening task makes the earlier request resumable again. Reports and maintenance do not replace acquisition intent. Current transport, pacing, report and redirect controls remain adjustable.
- **Download and scan:** Reuse complete inventory; finish any missing required index coverage before replay requests, score retained text, verify scan coverage, and write reports.
- **Download only:** Index and acquire text without scoring; if embedded media is enabled, parse retained bodies locally for discovery without creating keyword scan jobs. Finish required media indexing before acquiring selected media. Report unresolved work independently.
- **Index only:** Index and classify the inventory, write index reports, retain incomplete state for deferred CDX scopes.
- **Rescan:** Reinterpret retained local bodies with selected rules, without requesting archive bodies.
- **Retry:** Retry selected errors within the active inventory scope. Temporary failures continue through persistent scheduling when enabled; accumulated historical attempts never permanently hide eligible pending work. Text recovery and local scanning finish before dependent media acquisition. Pending/failed selected media is never labeled successfully acquired.
- **Media only:** Media-index, media-download and media-retry operate without requiring an ordinary text scan and report their own unfinished selection.
- **Report (idle):** Regenerates available scan/index and media reports. Does not index, download or rescan. If no index or scan exists, writes an honest empty inventory summary.

Other analysis, research, hitlist, import, repair, integrity, backup and maintenance operations remain independent. Completed scan evidence is **not revoked** by an export permission, storage or formatting failure.

## Redirect policy and phase admission

With **Follow archived redirects during text acquisition** off, the first HTTP
301/302/303/307/308 response excludes that capture immediately. No destination
request, redirect-body parsing, fallback replay or automatic retry follows.
This includes same-site, canonical and timestamp redirects. The capture remains
in the inventory as `skipped_redirect_policy`, with its destination recorded.
Turning the option on reopens those policy exclusions in the active scope.
Enabled text following permits archived destinations; live-web destinations
remain errors. Media retains archive-only host checks; the same setting permits
external archived media hosts, while ordinary same-site archived media redirects
remain followable. Legacy config key `download_external_redirects` is preserved.

Every acquisition mode requires positive coverage for its required inventory.
Combined operations admit and acquire proven text inventory first, then index
optional direct media and refresh embedded discovery after text is retained and parsed. Embedded
URLs unknowable before reading archived text must be resolved before **media**
acquisition. A failed required lookup or missing body blocks the dependent phase;
an authoritative zero-result lookup is recorded as unavailable. Index timeout,
an empty queue, an attempt ceiling, or a successful export is never completion
evidence. Explicit policy exclusions are recorded exclusions, not saved bodies.

User Stop and process termination preserve committed checkpoints and validated
bodies; use Resume after reopening. Persistent recovery handles temporary
failures while the process runs. Permanent archive unavailability, deleted local
files and disk failure remain explicit incomplete/error outcomes. Resumability
preserves obligations; it cannot make unavailable archive content exist.

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

A finished download pass may include confirmed missing captures, explicit Wayback exclusions, or robots-blocked captures. They retain their capture/error evidence and are named as **unavailable**, without requiring Resume. Unknown failures, plain HTTP 403, size-limit refusals, local errors, incomplete payloads and unproven indexing remain unfinished. GUI dialogs and CLI JSON/JSONL include the operation's explanation. The report snapshot's stricter `complete` flag still stays false when unavailable failures lack acquired/scanned evidence; its counts separately identify unavailable text/media captures.

With the Summary report and its index-coverage field enabled, `reports/indexing_status.tsv` lists every selected text target, its unfinished scopes, and last recorded reasons. The live indexing queue also shows remaining, retrying, cooling and deferred target counts. Retry time excludes turns behind other targets. Text/media payload recovery allows complete fair sweeps up to the configured no-progress attempt allowance; saved cooldowns do not expire that allowance before an attempt. Persistent 429/503 gate rules and explicit request pacing still apply.

Resumability preserves saved scan results including zero-hit documents, downloaded bytes, CDX cursors and unfinished media state. v1.2.6 uses **schema 11** and backs up supported schema-10 projects before adding independent domain coverage, current retry decisions and derivation revisions. The previous data remains in place; nevertheless make an independent project backup before upgrading and do not open the upgraded project in v1.2.5.

## Domains and corrected scan recipes

Enable **Extract domains** in Reports, or set `extract_domains: true` in project.json. CLI initialization accepts `--extract-domains`. Scan/Rescan processes all selected eligible bodies, including keyword nonmatches and exclusions. Report-only exports committed domain results without scanning or fetching. Acquisition-only runs process this requested local consumer without creating a keyword scan. Output is one `*.full.hostname/*` per line in `reports/domains.txt`, including separate apex, www and other subdomains when observed. No new target is enqueued.

First enablement backfills available eligible saved bodies; missing or changed inputs remain unresolved. Existing valid extraction checkpoints prevent repeated reads. Domain detection is high recall: a filename such as `version.py` is also domain-like because `.py` is a real ending. No offline recognizer can prove historical registration or recover arbitrary encrypted JavaScript.

Old single-rule scans unaffected by the scoring corrections can be adopted after their scope checks. Potentially affected multi-rule scans stay historical while a new scoring generation processes retained inputs; this one-time local work fixes demonstrated label/proximity errors without downloading them again. Explicit saved research choices remain intact; omitted auto-research now consistently defaults off.

After upgrade, use the usual dependency installation/update procedure or rebuild the executable. The build scripts include offline suffix assets in GUI and CLI bundles. Source tests are not native frozen-build certification.
