# Archive Scout

**Archive Scout 1.0.1** is the first corrective release after the initial public release of a cross-platform desktop research workspace for indexing, downloading, searching, reviewing, reconstructing, and analyzing public captures from the Internet Archive's Wayback Machine. The GUI and automation CLI share the same durable SQLite project engine. The current internal project schema is **11**.

## Downloads

- [Windows x64](https://github.com/DearStrike4940/archive-scout/releases/download/v1.0.1/ArchiveScout-Windows-x64.zip)
- [Linux x64](https://github.com/DearStrike4940/archive-scout/releases/download/v1.0.1/ArchiveScout-Linux-x64.tar.gz)
- [macOS Universal (Intel + Apple Silicon)](https://github.com/DearStrike4940/archive-scout/releases/download/v1.0.1/ArchiveScout-macOS-Universal.zip)

The repository can also be run from source on Python 3.11+. Tagged releases are built by the cross-platform GitHub Actions workflow.

## 1.0.1 corrective-release highlights

- Resume-first CDX/Timemap indexing with durable page/window checkpoints and a conservative shared **2.5-second index request-attempt interval (24/minute)**.
- Independent replay pacing at **0.125 seconds per actual request attempt (8/second)**. Redirect hops, retries, and transport-backend fallbacks are paced and counted at the wire-attempt boundary.
- Coordinated Wayback 429/503 handling: Retry-After is treated as a minimum, missing headers start with at least a 60-second cooldown, recovery is gradual, and rate-limit pauses remain distinct from connectivity failures.
- Historical archived-origin 429/503 responses are not misclassified as current Wayback throttling.
- Durable text/media classification, replay resume, local scanning, reports, review state, Research Intelligence, optional AI relevance, and media workflows.
- Windows DPI-aware startup, system/dark/high-contrast theme handling, named-font scaling rather than global Tk DPI overrides, screen-clamped geometry, reusable scrolling, and independently scrollable tables.
- Results/FTS, scan history, error, and site-issue reads execute away from the Tk UI thread with stale-result guards.
- Optimized deterministic scanner behavior with bounded proximity work, normalization/count reuse, local-rescan file reuse, and malformed-markup handling.

The source still recognizes serialized project identifiers from pre-release development builds so existing project folders can be migrated safely. Those compatibility identifiers are not separate public releases.

## Core workflow

A typical research project follows this sequence:

For bulk acquisition where scanning can wait, choose **Index and download only (no scanning)**. That mode requires no keyword set, writes only capture/resume state to SQLite, leaves saved captures as `downloaded_unscanned`, and is designed to be followed by **Search with Hitlist** or a later local rescan.

1. Add one or more sites, URL prefixes, or exact URLs.
2. Set the archive date range and optional CDX parameters.
3. Define one or more keyword sets.
4. Index archived captures from Wayback.
5. Download textual captures with bounded, rate-limited workers.
6. Parse and scan downloaded pages locally.
7. Review, search, filter, tag, and export matching pages.
8. Build or refresh the local Research Intelligence index and search the whole project with natural-language research questions.
9. Optionally ask the AI relevance/deep-review layer to rank or synthesize a bounded set of evidence with document-ID citations.
10. Optionally discover, resolve, and download archived images and videos embedded by the scanned pages.
11. Use recovery, comparison, forum reconstruction, provenance, diagnostics, and project-management tools as needed.

Archive Scout stores project state in SQLite so long jobs can be stopped and resumed without starting over.

## Report control

**Reports** appears between **Media** and **Archive analysis** in both Simple and Advanced workspaces. Choose every standard text-scan, index/error, media and archive-analysis output and its individual fields. Scrollable groups keep controls accessible on smaller screens. Set the minimum score, match order/limit, snippet count/length and links per match; use presets for all reports, matched URLs only or no generated reports. Hitlist, AI and manual export formats remain separate operation outputs.

Report visibility never deletes historical match details or human review state. Full scan detail is retained by default so reports can be regenerated without rescanning. Uncheck **Retain full scan details** for lean future scans that omit enrichment unused by enabled reports; changing that choice later may require a local rescan. Download-only creates no scan enrichment either way. If an older pre-release project lacks retained enrichment, regenerate it by rescanning the saved captures.

## Text and media

Text includes HTML source, plain text, JSON, XML, JavaScript and legacy CGI/PHP/ASP pages. Newly acquired text goes under `captures/YYYY/MM/` with recognizable URL-derived `.txt` names. The raw bytes are retained: `.txt` does not mean stripped markup or lossy transcoding. Existing capture paths remain usable without renaming or downloading again. Hitlist uses SQLite for URL/path/coverage state and reads the capture files for the actual content.

Known binary captures are excluded from the text queue. Ambiguous metadata is checked against response bytes; familiar image/video/document signatures override an incorrect `text/html` header. SVG is treated as image-purpose content by the shared classifier. Media descriptors can be searched only through the explicit descriptor policy. Optional images/videos retain their file format in flat `media/images` and `media/videos` directories. HTML error pages are rejected as media. Text acquisition does not automatically download every non-media binary format (PDF/ZIP/executables are not plain text).

## Request preview and connection recovery

The CDX preview now shows the actual first request for the chosen strategy, readable parameters and a **Copy request URL** button. Copied CDX links and wholly encoded target URLs are normalized at input. Ordinary escaped paths are preserved. Query values are quoted once at the request boundary; nested `&`, `+`, `%` and fragment characters still need encoding to retain their meaning.

All HTTP backends now follow the environment/proxy toggle consistently. Automatic fallback survives an optional backend failing to initialize. Range resumes validate their offsets and total length; an invalid/rejected range restarts the affected file once rather than appending corrupt bytes. Rate limits still use the shared pause gate, and TLS verification remains enabled. See [Network performance](docs/NETWORK_PERFORMANCE.md) for pacing, recovery, and troubleshooting details.

When **Also download media during text and download-only runs** is enabled, the Media-page controls apply to both workflows, including embedded discovery and external hosts. Follow-up media uses an independent local snapshot policy keyed by the archive `urlkey` where available; `earliest` selects one eligible capture without changing the primary text query's collapse settings.

## Research Intelligence

Archive Scout treats a completed project as one evidence corpus. The local research index combines full-text evidence, compact vectors, extracted entities/identifiers, duplicate clusters, deterministic Archive Scout scores, hyperlinks, provenance relationships, reconstructed forum relationships, and capture timestamps.

The **Research intelligence** tab can:

- search concepts across the saved project rather than only one scan run;
- surface near-duplicate/reposted material without letting copies dominate the ranking;
- extract domains, URLs, filenames, dates, hashes, email addresses, and username-like identifiers;
- expose an evidence graph and chronological relationship timeline for each result;
- preserve the distinction between archive evidence, deterministic Archive Scout scoring, and AI interpretation;
- run an optional deep AI review over only the strongest bounded evidence set, rejecting model claims that cite document IDs not supplied to the model.

The built-in `local-hash` backend is dependency-free and private. Source installations can optionally install the `research` extra and select `fastembed` for a neural local embedding backend. AI is not required for ordinary Research Intelligence search.

## AI relevance review

The AI relevance page is an optional second-stage research tool. It does not replace keyword matching or alter Archive Scout's deterministic scores.

After a scan has produced a report, enter a natural-language description of what you are trying to find. Archive Scout:

- selects a bounded set of existing report matches;
- gives prompt-relevant full-text candidates first consideration and fills the remaining candidate budget from the normal Archive Scout ranking;
- sends compact page evidence rather than complete project archives;
- asks the selected provider for a structured relevance score, confidence value, category, short reason, and evidence summary;
- stores the AI ranking separately from the original match and review records;
- generates CSV, JSON, and Markdown AI relevance reports.

OpenAI (`gpt-5-mini` by default) and OpenRouter are supported through separate provider adapters behind the same internal interface. The provider/model, candidate count, batching, excerpt size, and minimum displayed relevance can be adjusted in the interface.

### API key and privacy

Archive Scout does not bundle an API key. Enter a session-only key in the AI page, or provide `OPENAI_API_KEY` / `OPENROUTER_API_KEY` in the launch environment. Source checkouts may use an ignored `.env` file. Keys are never written to `project.json`, SQLite, reports, diagnostics, or application settings.

AI review is opt-in. Normal indexing, downloading, scanning, reports, media workflows, and local Research Intelligence search do not require an AI provider. When AI is run, only the research prompt and bounded page excerpts are sent to the selected provider. The OpenAI adapter requests non-stored Responses; all adapters instruct the model to treat archived page content as hostile/untrusted source material rather than instructions.

## External embedded media

The dedicated operation **Index, download, scan, then download external embedded media** preserves the established sequence while making the media stage much more comprehensive and resumable:

1. The specified site is indexed.
2. Textual captures are downloaded and parsed.
3. Keyword scanning and normal reports finish first.
4. Archive Scout discovers media references from the downloaded pages.
5. External candidates are placed in a persistent discovery queue.
6. Exact Wayback lookups resolve archived captures for those media URLs.
7. Resolved media captures are downloaded directly to disk using bounded workers.

Discovery covers standard and legacy page structures including `img`, lazy-image attributes, `srcset`, `video`, `source`, posters, `object`, `embed`, legacy Flash/player configuration, CSS `url(...)`, social-preview image/video metadata, preload hints, background attributes, direct media links, and extensionless image/video endpoints identified by HTML context. Wayback replay URLs embedded inside archived HTML are unwrapped to their original URLs before exact lookup.

The discovery queue is persistent and keyed to document content, so unchanged pages are not repeatedly reparsed. Exact media lookups use bounded parallelism while retaining Archive Scout's shared request-start limiter. Known Wayback-excluded or robots-blocked hosts are short-circuited so thousands of media references from the same unavailable host do not generate thousands of pointless requests.

Media downloads are streamed directly to disk with incremental hashing and bounded validation previews. Known file sizes are scheduled smallest-first to improve visible progress and reduce long-tail stalls.

## Site-specific Wayback issues

Archive Scout distinguishes archive-policy and site-specific failures from ordinary transient network errors. The Errors page contains a dedicated **Site-specific Wayback issues** table and records repeated occurrences by host and workflow stage.

Examples include:

- material explicitly excluded from Wayback;
- archived replays blocked by robots.txt restrictions;
- unavailable captures;
- invalid replay redirects;
- access-restricted CDX requests;
- archived origin/server-unavailable pages;
- rate limits, timeouts, connection problems, TLS failures, and Wayback service errors.

Confirmed exclusion and robots restrictions are treated as host-level circuit conditions where appropriate, allowing Archive Scout to preserve useful work and continue other targets instead of repeatedly retrying the same unavailable host.

## Search and review

Archive Scout supports:

- multiple independent keyword sets;
- literal, required, excluded, exact, regex, whole-word, field-specific, weighted, and proximity-aware rules;
- a compiled Aho-Corasick literal prefilter for large rule sets;
- SQLite FTS search over downloaded documents;
- persistent scan history;
- review statuses, notes, and tags;
- local and Wayback opening from results;
- CSV, JSON, Markdown, and review-package exports;
- scan comparison and score-change reports.

AI relevance results remain linked to the same underlying match records, so human review and deterministic evidence remain authoritative.

## Archive analysis

The analysis workspace includes:

- generic and profile-aware forum reconstruction;
- legacy embedded-player extraction;
- custom identifier extraction;
- duplicate and near-duplicate analysis;
- snapshot differences;
- provenance and mirror relationships;
- first-appearance timelines;
- controlled external asset lookup;
- project merging with path-containment checks and automatic safety backup.

## Performance and resilience

The execution engine is designed around bounded work rather than project-sized in-memory lists. Important behaviors include:

- persistent SQLite work queues;
- WAL mode and tuned project connections;
- keyset pagination for large local tables;
- bulk capture and queue writes;
- resumable CDX pages and resume keys;
- high-throughput text replay defaults (10 workers, 0.125-second shared request-start spacing) with bounded overlap;
- endpoint/backend recovery and date-window subdivision for transient CDX failures;
- bounded local parallel rescanning;
- stored-parse reuse for unchanged documents;
- no-op database writes when CDX/document/match data is unchanged;
- size-aware text and media scheduling;
- direct-to-disk media streaming;
- bounded GUI progress queues;
- atomic report and export replacement.

The offline benchmark runner can exercise large CDX parsing, database insertion, result pagination, keyword matching, and no-op repeated indexing without contacting the Internet Archive.

Text-capture discovery explicitly recognizes legacy web/page formats including `.htm`, `.shtm`, `.dhtm`, `.xhtm`, `.phtm`, `.cgi`, `.php`, `.dat`, and `.txt` in addition to the existing HTML/XML/JSON/script formats.

### Sparse-first and dense-parallel indexing

Automatic indexing starts an unknown broad range with one useful data-bearing resume request. Sparse ranges can therefore finish without mandatory year-by-year or page-count passes. If that first response proves the inventory is dense and has continuation, Archive Scout switches once to bounded parallel Timemap paging (`pageSize=9`, up to ten CDX workers) behind the shared 2.5-second actual request-attempt limiter. Each successful page and its rows are committed atomically; failed pages remain individually resumable. Exact/narrow queries stay on resume-key traversal, and saved queue layouts are not reinterpreted as another pagination model.

## Automation and bot compatibility

The release packages a separate `ArchiveScoutCLI` console executable alongside the GUI. The CLI supports noninteractive project creation, run/resume, status, deterministic results, errors/site issues, Research Intelligence search/indexing, AI review, and grounded research. `text`, `json`, and streaming `jsonl` formats are available, with stable exit codes and graceful Ctrl+C handling. Read-only status/search/results/errors commands open SQLite in query-only mode. See `docs/AUTOMATION.md`.

Bot support is an interface layer over the same engine and is not executed by the normal GUI hot path.

## Project safety and recovery

Projects are self-contained folders. Archive Scout can:

- automatically back up a project before schema migration and high-risk operations;
- resume interrupted index/download queues;
- retry selected error categories or selected capture IDs;
- check missing/orphaned files;
- repair project indexes and derived state;
- export privacy-reduced diagnostic packages;
- merge another Archive Scout project while preventing source paths or symlinks from escaping the selected project root.

Existing supported project databases are upgraded in place to the current schema after a safety backup.

## Installation

### Windows

Download `ArchiveScout-Windows-x64.zip`, verify its checksum, extract it, and run `ArchiveScout.exe`. The archive also contains `ArchiveScoutCLI\ArchiveScoutCLI.exe` for automation. Tagged official releases are intended to use the repository's configured Windows Artifact Signing workflow.

### macOS

Download `ArchiveScout-macOS-Universal.zip`, extract it completely, move `Archive Scout.app` to Applications, and open it. The ZIP also contains the universal `ArchiveScoutCLI` executable. The packaging script preserves symlinks, verifies the frozen runtime, performs a startup probe, and validates the packaged application before publication.

### Linux

Download `ArchiveScout-Linux-x64.tar.gz`, extract it, and run the included application or install it with the provided user-local installer. The installer maps `archive-scout` to the packaged CLI and keeps the GUI in the desktop application menu.

## Source development

Archive Scout requires Python 3.11 or newer.

Install the package and run the test suite with the standard Python packaging workflow for your development environment. The repository's Tests workflow exercises Linux, Windows, Intel macOS, and Apple Silicon macOS on Python 3.11 and 3.12.

See the `docs` directory for architecture, AI review, external media, network recovery, migration, development, and release guidance.

## Responsible use

Archive Scout is intended for research involving publicly archived material. Respect applicable law, archive access restrictions, site policies, personal privacy, and the context of sensitive historical material. The software deliberately uses bounded request pacing and records exclusions rather than attempting to bypass Wayback restrictions.

## License

MIT. See `LICENSE`.
