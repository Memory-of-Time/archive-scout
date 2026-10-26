# Architecture

Archive Scout is a project-oriented desktop application. The UI is deliberately separated from the indexing, downloading, scanning, media, AI, database, and analysis engines so long-running work can persist independently of what is currently displayed.

## Project storage

Each project contains `archive_scout.sqlite3`, downloaded captures, downloaded media, reports, backups, and derived exports. SQLite is the authoritative work-state store. The current schema version is 11.

Project connections use WAL mode, normal synchronous behavior, disk-backed temporary storage, a bounded cache, memory mapping, busy timeouts, and foreign keys. Large queues are read with keyset pagination rather than large OFFSET scans.

## Indexing

The CDX layer builds request signatures from target/date/settings and persists work by target and time window. Broad requests can use numbered pages; resume-key requests are used where appropriate. The engine retains fixed shared request-start pacing while allowing bounded overlap so slow Wayback responses do not serialize otherwise independent work.

Transient failures can rotate transport backends/endpoints and subdivide the failed date window. Permanent archive-policy failures are recorded as site issues rather than retried indefinitely.

## Text downloading and scanning

Pending captures are selected from SQLite, with known smaller downloads scheduled first. Network workers retain validated raw bytes. A bounded local scanner independently decodes/parses them, persists score-zero coverage and evaluates the selected keyword set. Explicit rescans reconstruct parsed content from validated bytes. Optional domain extraction shares these inputs but owns separate source coverage and hostname tables.

Literal rule prefiltering uses an Aho-Corasick automaton. Full scoring retains required, excluded, exact, regular-expression, whole-word, field, weight, and proximity semantics.

## External media

External media is intentionally a second-stage pipeline after text download and scan. Page content is mined for direct, lazy, CSS, social-metadata, legacy-player, and other embedded image/video references. Candidates are persisted in `media_discovery_queue`; unchanged source documents are tracked by content hash in `media_discovery_documents`.

Exact Wayback lookups are bounded and rate-limited. Resolved captures enter the normal media capture table and are streamed directly to disk. Excluded/robots-blocked hosts can be short-circuited at discovery/index/download stages.

## AI relevance

AI relevance is a separate analysis layer over existing deterministic matches. `ai_runs` stores the prompt/model/run metadata and `ai_results` stores one relevance result per match. The normal match score, review status, notes, tags, and scan data are not modified.

Only a bounded candidate set is sent to the configured AI provider. OpenAI and OpenRouter providers use structured output where supported. Evidence IDs and input hashes are recorded; provider output remains an AI draft, and ID validation does not establish entailment.

## UI threading

Tk owns the UI thread. Operations run in worker threads and communicate using a bounded/coalescing event queue. Database connections used by workers are opened inside their owning thread. The UI never relies on subprocesses for its core research workflow.

## Safety

Project merge, repair, backup, migration, export, and diagnostic operations use path containment and atomic-write patterns where applicable. The program never intentionally bypasses Wayback exclusions or robots restrictions.

## Derived work and operation ownership

Selected saved-page scopes use one shared site/signature/membership/date predicate. Optional media indexing follows admitted text work. Replay clients and health observations live for the operation, surviving recovery batches. Retry decisions are current per-capture state; older category history cannot revive a newer non-retryable failure.

Analysis builds connection-local disk TEMP generations, promoting valid derived rows in one main transaction. Research stores independent vector/entity recipes and document revisions; unchanged heavy inputs are not transferred into Python. Duplicate fingerprints persist across refreshes and clustering uses indexed disk state and bounded candidate buckets. Near matching remains approximate; exhaustive capture coverage is a separate contract.

Domain extraction uses bundled, pinned IANA active/retired endings and public-suffix rules, with full-hostname IDNA normalization. It does not reduce hosts to site roots, query DNS, contact discovered domains or create match rows. `reports/domains.txt` is a sorted unique export of the current saved-input selection; its status file discloses missing processing independently of acquisition.
