# Archive Scout 1.0.8

This patch improves local scanning and resource use while preserving the existing network recovery policy, rate ceilings and evidence validation. It applies to the supplied v1.0.7 source; the lost older fast version is unavailable, and v1.0.4 was not used as a performance reference.

## Scanning, queues and memory

Retained scans and rescans persist completed results in batches of at most 16 items, an estimated 1 MiB of result objects or 250 ms. Individual oversized results flush immediately. Transactions stay short and do not wait on worker futures. Progress advances after commit. Cancellation flushes received work and preserves unfinished durable queues; a failed write rolls back its batch for resumption. Discard-after-scan retains its stricter commit-before-file-delete path.

Identical positive/candidate literal prefilters share their immutable native automaton. Text whitespace cleanup uses the equivalent built-in split/join operation. SQLite's mapped-file allowance falls from 256 MiB to 64 MiB; its 64 MiB cache and WAL/synchronous policy are retained. These are budgets, not a cap on total process memory, which also depends on bodies, worker count and optional analysis models.

Schema initialization reuses correctly shaped classification indexes. Media's positive-length queue follows indexed `(length,id)` keysets, with a separate bounded stream for unknown/zero/negative lengths. Acquisition-only error retry selection stays in SQLite temporary tables instead of a large Python ID list and a single over-limit SQL parameter list. Equal SimHash groups are coalesced before near-duplicate comparisons; unique-hash bucket limits remain approximate, as before.

## Content and resumability

Schema 13 adds compact current-FTS mappings/signatures and Hitlist coverage fingerprints. Full-text replacement inserts a new token version and atomically switches its document mapping. All public FTS search paths join the current mapping, so overwritten files cannot leave obsolete terms visible after replacement. Unchanged signatures avoid repeated postings. Old inactive postings remain on disk until explicit **Repair** or **Compact** rebuilds them; full document bodies are not duplicated to support this mechanism.

Migration preserves the existing FTS index and evidence. It cannot infer already-stale legacy terms without reading the corpus. Rescan affected documents or rebuild the full-text index with Repair/Compact to refresh those terms. Repair, Compact and Merge share the portable rebuild, prefer available saved bytes and respect the recorded encoding. Human review data remains separate.

Hitlist resume checks the actual saved content behind its checkpoint once, hashing files in bounded chunks. External edits with unchanged size and modification time, URL changes and newly available bodies are reconciled. This additional read is necessary for completeness; skipping it would silently retain stale coverage. The original saved capture boundary is preserved, and old coverage is refreshed once without discarding human review data. Scan/rescan content hashes are computed from the bytes actually parsed.

Supported older databases receive a backup before schema migration; a failed backup prevents migration. Schema 12 becomes schema 13 without deleting captures, notes, reviews or deterministic scores. Restore the older backup to use an older program again.

## Estimated time remaining

On Dashboard, enable **Show estimated time remaining**. The option is saved per project and defaults to off. It uses completed work and a recent bounded history; it does not assume a fixed 8/s rate, query SQLite, create a worker thread or add per-item persistence. Disabled estimates collect no samples.

The label estimates the **current phase**, including download, scan/rescan, Hitlist, media and existing analysis progress where a usable total is available. Backup copying/compression, report rows and full-text rebuilds provide progress too. Discovery, unknown future phases, growing work plans and phases without a reliable count show **Estimating**. This is not an invented whole-run deadline. Known recovery deadlines are included once; unknown outages remain unknown. Stalls, phase changes, cancellation and project/operation changes reset or qualify the estimate.

## Measured results and limits

All comparisons use the supplied v1.0.7 code on the same Linux/Python 3.12 environment. These are offline fixtures, not Internet Archive capacity measurements.

| Fixture | Supplied v1.0.7 | v1.0.8 | Completeness check |
| --- | --- | --- | --- |
| Initial scan, 33,000 unique saved files | 58.0–65.6 s in two runs | 51.7 s | Same 99,000 match records and evidence hash |
| Rescan of that corpus | 41.5–47.8 s | 39.8 s | Same complete match evidence |
| Hitlist on that corpus | 19.8–20.7 s | 19.0 s | Exact fixture assertions passed |
| Peak RSS across that scan/rescan/Hitlist process | 364.8–365.5 MiB | 202.1 MiB | Full retained scan details enabled |
| Scan phases across 300,000 growing logical captures | 214.2 s | 185.9 s | 300,000 documents/matches, zero pending |
| Peak RSS in that growing-project fixture | 358.5 MiB | 175.1 MiB | Bounded queues, 3 threads and at most 11 handles observed |

The growing fixture reuses 16 physical bodies and performs real reads, parsing, scoring and database persistence for each logical capture. It is not 300,000 unique files or a multi-day network run. v1.0.8's first 75,000 averaged 1,665 scans/s and its last 75,000 averaged 1,455 scans/s, including a slower 250,001–275,000 window. This demonstrates lower total time and bounded process resources, not perfectly flat throughput. Project databases necessarily grow with retained evidence.

Across the 33,000-file scan/rescan/Hitlist run, measured CPU fell about 15–22% and block-write bytes about 11–12%. Initial scan write bytes rose about 4–5% because the new correctness records are persisted; later phases reduced the overall write cost. Across the 300,000-capture scan phases, CPU fell about 14% and write bytes about 20%. These figures are workload-specific.

Production acquisition on pooled loopback HTTP completed healthy and discard-after-scan fixtures at approximately 8 successful saves/s; full five-second windows held 40 successes. A reset/redirect/500/429 fixture recovered all 96 captures with zero terminal errors, averaging 7.22 saves/s including necessary retries/waits and returning to 40 successes in the next full window. A deliberately two-second response fixture was limited to about 4.34 saves/s by its worker/latency budget. No live archive load test was performed.

Eight successful saves per second is a target where service latency and workload permit, not a guaranteed minimum. Wire retries and redirect hops consume the existing paced request budget. Retry-After, response/body/Range validation, partial-file safety, CDX completeness and byte-level text/media classification remain authoritative. No shorter timeout, higher rate ceiling or extra runtime dependency was introduced.

## Release validation

The patch includes migration, stop/resume, failed-write, content replacement, media keyset, retry-limit and virtual-clock ETA regressions. See [validation scope](VALIDATION_1_0_8.md) and the accompanying validation archive for actual logs. Local checks do not claim a remote GitHub matrix, native GUI interaction or native installer build succeeded. Follow [apply instructions](../APPLY_PATCH.md) and run Tests on the uploaded commit before tagging v1.0.8.
