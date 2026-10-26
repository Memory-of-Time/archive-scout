# v1.2.6 cumulative audit implementation

This release builds on the supplied 25 October repository and the Completion and Throughput work. It includes the selected-scope follow-up and the subsequent confirmed audit corrections. The accompanying package is cumulative: do not apply an older patch afterward. Public version is 1.2.6, SQLite schema 11.

## Preserved contracts

Archive inventory proof, validated payload bytes, local processing, requested media and published reports remain different completion conditions. Errors, exhausted attempts and empty scheduling rounds do not certify completion. Healthy request pacing remains explicit; no new adaptive pacing layer or unconditional acquisition sleep was added. Temporary network failures keep their durable recovery state. Storage-full preserves work and stops new retention.

Existing folders, payload files, target memberships, scores, notes, reviews, project settings, GUI/CLI aliases and resume intent are preserved. Schema migration requires a successful backup before modification. Never open a migrated project with v1.2.5 or edit schema_info to force a downgrade.

## Audit disposition

The C identifiers refer to the cumulative audit. Some entries were findings, some operational contracts, and some unverified causes. They are not 37 independent reproduced production bugs. The 126-feature register is a coverage inventory, not 126 claims of exhaustive validation.

| Audit references | v1.2.6 disposition |
| --- | --- |
| C01-C03 | Earlier CDX scope/layout/cursor/terminal-page and media page-ledger fixes are retained, including targeted older-proof revalidation and safe reuse of proven uncollapsed extrema inventory. |
| C04 | Complete admitted media-discovery source bytes are checked against their hash. Truncated, missing, changed or unreadable input remains unresolved. |
| C05-C06 | Rescan and embedded-media discovery share selected site/signature/membership/date scope, including per-target overrides and overlapping targets. No selected sites preserves explicit whole-project local Rescan behavior. |
| C07 | Optional direct-media indexing follows admitted text acquisition/scanning. Media failure does not prevent independently admitted text work. |
| C08-C09 | Operation-owned text clients survive batches. Legacy reconciliation runs locally alongside acquisition, is cancellable and shares its completed-scope proof. Disk TEMP queues retain bounded Python state. |
| C10 | Earliest media is the earliest eligible timestamp per original URL in scope, enforced at admission and Retry/Resume. Later timestamps stay strategy-skipped; historical files remain. Extra collapse parameters cannot prove extrema. Equal digests at different URLs do not erase provenance. |
| C11 | Dashboard selected/excluded media counts now exclude strategy-skipped and routed/skipped candidates consistently. Historical project totals still differ from a narrower run; they are not a new current-scope integrity audit. |
| C12 | Underlying connection causes remain empirical questions. Diagnostics retain bounded sanitized exception chains and response evidence. No blanket attribution to throttling, proxy, disk or database is made. |
| C13-C14 | Required rules use semantic identity rather than shared display labels. Sensitive proximity uses its matching source representation, sorts merged positions and does not award a pair for an empty body-position list. |
| C15 | FTS domain filtering parses canonical full hostnames with exact/dot-boundary subdomain matching, rejecting query-string mentions and lookalikes. |
| C16 | Report storage policy now reaches the shared match writer. Optional enrichment/default review creation can be omitted; score-zero coverage and existing human reviews remain. Existing stored details are retained when omitted by a later lean policy. |
| C17 | Read-only CLI SQLite URIs are absolute and correctly encoded. Paths containing # or ? no longer create/open an unintended database. |
| C18-C19 | Research vectors/entities have independent recipe identities and per-document revisions. Dimensions, excerpt and entity settings invalidate their applicable results. Missing vectors rebuild. Omitted auto-research consistently defaults off; explicit user choices remain. CLI has explicit --auto-research. |
| C20 | Unchanged research refresh does not load every heavy body into Python. Duplicate fingerprints persist; candidate buckets, union mappings and grouping use indexed disk state. Entity-only recipe changes do not force duplicate clustering. Near matching remains explicitly approximate, with the existing four-band/2,000-candidate policy. |
| C21-C22 | Analysis uses the shared byte decoder and rejects absent/changed raw evidence. Derived tables build in disk TEMP generations and publish transactionally; a failed/interrupted build keeps prior derived results. Contradictory forum-only settings are rejected before clearing results. External lookup retries remain incomplete. |
| C23 | Changed same-URI/same-second-mtime local imports get a content-version signature; historical document identities are not overwritten. Oversized import inputs fail explicitly under the admitted body budget, without truncation or false coverage. |
| C24 | Merge hashes during bounded temporary copy, validates existing destination reuse, and publishes verified bytes. Inconsistent recorded evidence is rejected. Cross-project ID maps use disk TEMP storage. |
| C25-C26 | Repair queues missing/changed downloaded media. Integrity streams issue output and uses disk-backed references, with per-file/chunk cancellation. Long merge/analysis loops also check cancellation. These explicit maintenance checks do not become acquisition startup requirements. |
| C27-C29 | Site issues are filtered before LIMIT; recovery-control grid collision is removed. Dead scan-control variables are removed and architecture/operation documentation is updated. No keep/delete retention mode is invented from a previously unused metadata fallback. |
| C30-C31 | Domain extraction is independent of capped research entities, Interesting Links and keyword matches. Its implementation and limitations are described below. |
| C32 | A durable current retry decision prevents legacy error adoption from reviving a later non-retryable failure. Historical categories stay available. Failure count survives category changes and reopen; explicit user retry remains possible. |
| C33 | Descriptive replay rejection messages retain their intended category, actual observed status and bounded response evidence. Empty representations are incomplete, not generic local success. |
| C34 | Media clients also live across recovery batches within the operation. Pools/backend health close at operation exit; direct downloader calls remain self-contained. |
| C35 | Repeated identical available representation evidence is fingerprinted. After three repeats, its existing exponential recovery schedule has a bounded slower recheck floor. It remains unresolved and eligible for later automatic recovery; no attempt count declares it complete. |
| C36 | Later throughput decline with flat errors remains a measured observation, not a diagnosed universal defect. Length ordering, occupied transfers and actual disk/CPU/network timings must be investigated in a live run. |
| C37 | Health observations survive recovery batches and retain phase plus batch save totals. No timing samples means null/unknown, not a claimed zero duration. Bounded first representative samples and sanitized exception chains improve diagnosis. |

Review-package exports now use one read snapshot across JSON/CSV/Markdown. AI deep review records the raw evidence identity and exact submitted evidence-packet hashes. AI Stop remains Stop rather than being wrapped as a provider failure. ID/hash validation is not factual entailment; AI output does not control capture completeness.

## Domain extraction

Enable **Extract domains** in Reports or set `extract_domains: true` in project.json. It defaults off. CLI `init --extract-domains` saves the same setting. Standard scanning and acquisition-only runs process it as a requested local consumer. Report-only exports committed domain results without fetching or rescanning.

Every eligible saved input in the selected site/date/query corpus is included, even if its keyword score is zero, it matches an excluded rule, or ranked reports omit it. The option does not override acquisition filters or add targets. It shares validated raw bytes, decoding, visible text and extracted links. No DNS or requests to discovered hosts are made, and no extra per-page document files or domain-derived match rows are created.

The recognizer uses pinned offline IANA active/retired endings and public-suffix rules, with IDNA normalization. Data source versions/hashes are recorded in assets/domains/SOURCES.json and the extraction recipe. Full observed hostnames remain intact:

```text
*.cdn.example.com/*
*.example.com/*
*.www.example.com/*
```

These are three distinct observed hosts, even though wildcard scopes overlap. Case, scheme, credentials, port and page path do not create duplicate hostname lines. Public/private suffix boundaries are checked without collapsing to a parent root. Ordinary HTML entities, escaped URL separators, Unicode/IDNA, recognizable inline formatting and email domain components are supported. Known replay wrappers contribute their original hostname.

`reports/domains.txt` is a sorted unique current-selection export with no header or metadata. `reports/domains_status.json` separately discloses recipe and unresolved saved inputs. Domain additions and a successful empty/nonempty source checkpoint commit together. A rollback can safely replay the input. Missing/changed input cannot receive complete coverage or contribute stale current output. Enabling on an existing project backfills eligible available saved sources once; valid checkpoints avoid subsequent rereads.

Normalization/recognition uses one candidate pass per distinct source view and bounded 256-host insert batches, rather than one full-page regex per suffix or a project-sized Python set. A large-token benchmark caught an early quadratic cleanup pattern; that pattern was removed before release and has a regression using one million unbroken characters. The feature still costs CPU and database space. It is not claimed to be free.

High recall has limits: `version.py` can be a filename or a domain-like token because .py is a real ending. Offline extraction cannot prove historical registration, recover arbitrary encrypted JavaScript, or inspect unavailable bytes. IP addresses, bare public suffixes and single-label names are excluded. The saved-input report is not itself a certificate of complete CDX acquisition.

## Compatibility and justified one-time work

Old single-rule scoring unaffected by these corrections can be reused after its scope proof passes. Potentially affected multi-rule scans keep their historical results while a new scoring generation processes retained bodies. This local work corrects demonstrated evidence errors and does not redownload valid bodies. Changing pinned recognition data invalidates the domain recipe; new endings require actual backfill, not only output reformatting.

Generation staging uses additional temporary disk proportional to derived work. Interrupted analysis retains its previous main generation; a later rebuild currently recomputes that derived generation rather than resuming an on-disk partial generation. Raw capture/download checkpoints stay intact. Analysis report publication uses atomic files, but the set of report files is not one filesystem transaction; an access failure leaves the analysis run failed and preserves remaining staging for explicit regeneration.

Merge hashing, complete discovery reads and explicit integrity verification each prevent a concrete evidence failure. They are not repeated unconditionally at each replay batch. The retry scheduler never turns bad bytes into valid captures to improve a finishing counter.

## Verification and remaining uncertainty

The package's validation directory contains final applied-source test results, fixture JSON, commands and exact scope. All retained files in the local HTTP text/media fixtures are hashed and their state/score coverage checked. Million-row fixtures contain synthetic historical database records, not a million physical archived bodies. They validate scheduling/startup/membership, not historical integrity.

No live overnight/multi-day Wayback run, production replay of the 780 generic errors, native Windows/macOS GUI or filesystem session, frozen executable execution, FastEmbed download, or AI-provider request is certified. The supplied media trace is not proof that every generic local error was an invalid archived response. Four desktop-display tests remain skipped in this Linux environment.

Successful healthy-service throughput remains the target, not a guarantee during connection failures, service throttling, invalid representations, insufficient bandwidth or unresolved final captures. The new diagnostics distinguish evidence needed to investigate those causes; arbitrary pacing changes do not substitute for diagnosis.
