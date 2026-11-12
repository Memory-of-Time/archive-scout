# Archive Scout v1.3.0 — large-project sustainability

This release targets measured local costs that grow with project history. It does not establish that every reported slowdown has the same cause, or that overnight live Wayback throughput is fixed.

## Dashboard

Previously, refresh counted captures, positive matches, open errors, media and classification groups from the complete database. A refresh exceeding its deadline discarded the new snapshot and retained old totals, producing the heavy-disk-activity message.

Schema 12 builds compact summaries once. SQLite triggers update those summaries in the same transactions as their authoritative capture, routing, match, error, network-event and media changes. A refresh reads those summaries inside one WAL snapshot. Rolled-back operations do not change committed totals; positive pages are counted once across scan runs. Confirmed unavailable captures remain separately accounted for. Read-only dashboard calls do not initiate migration or write to the database.

Manual/automatic settings and refresh intervals are preserved. A genuine lock or query interruption still retains the previous display; automatic refresh can retry. Summary reads greatly reduce work, but do not guarantee a response deadline under arbitrary storage failure. Explicit Repair rebuilds summaries from source tables after its safety backup.

## Local scanning and errors

Pending scans now traverse `(timestamp, id)` within a partial index of `downloaded_unscanned` captures. The signature and date range remain enforced, and tied timestamps retain an ID tie-breaker. This removes repeated sorting of historical captures and also bounds narrow-date queries. Unprocessed rows remain pending for subsequent drains.

A document-error lookup index avoids scanning unrelated error history when resolving a document's errors. These are database changes; no saved-body integrity or MIME/routing rules were relaxed.

An unexpected scanner-supervisor failure now stops further acquisition promptly. Completed bodies and queued captures remain recoverable. Existing temporary SQLite-lock handling remains in effect; network pacing, replay deadlines, server cooldown policy, and independent local scanning are preserved.

## Diagnostic history

When acquisition diagnostics are enabled, `latest.json` remains the current health snapshot. A five-minute `history.jsonl` sample retains bounded longitudinal evidence; it rotates into two backups at 4 MiB per file (12 MiB maximum per text/media stream). Error-Activity visibility remains optional and on by default; hiding those messages does not suppress database evidence or diagnostic records.

## Upgrade

Stop the old application and keep an independent project backup. The supported database upgrade creates its required pre-upgrade backup, adds indexes, and builds summaries without reindexing or acquiring captures. The initial database pass and backup require time and disk space. Existing files, settings, scan history, retry schedules and checkpoints remain authoritative. Use v1.3.0 for a schema-12 project; do not force a downgrade by changing `schema_info`.

## Validation performed

Linux, Python 3.12.14; 796 full-suite tests passed with no failures, errors or skips under Xvfb. A final focused run passed all 12 new sustainability tests, including a 1,229-record scan traversal spanning multiple 500-row batches. Release verification, compilation, wheel source/import checks, installed CLI help and the installed GUI dashboard snapshot were checked separately. Native Windows/macOS builds remain unverified locally.

The database fixture contains 1,000,000 captures, 800,000 document rows, 800,000 match rows, 800,000 FTS entries, and 100,000 errors. Historical source files are not physically present; the transfer tests acquire and verify actual new bodies. This tests populated database history, not one million real NTFS files.

Measured local results:

| Check | v1.2.9 baseline | v1.3.0 |
|---|---:|---:|
| Warm dashboard read at 1M captures | 800.8 ms | 1.2 ms mean |
| First 500-row scan batch, full metadata join | 622.0 ms | 5.2 ms |
| Document-error lookup with 100K errors | 8.35 ms mean | 0.006 ms mean |
| 960 real loopback saves + scans against populated history | 7.938/s | 7.942/s |
| CPU seconds at the 960-save milestone | 28.60 | 12.57 |

The healthy transfer rates are essentially equal: the fixture already reaches the configured request-start ceiling. CPU use fell by about 56%, but this does not reproduce or explain the full reported live regression. Peak RSS was approximately 359 MiB baseline and 374 MiB patched and did not increase between the three sampled transfer milestones; this short observation cannot establish multi-day memory stability.

Final patched early/middle/late rates were 7.898 / 7.988 / 7.990 saves/s. All 960 new files passed SHA-256 checks and had scan results. Twenty-five concurrent dashboard refreshes were exact; the slowest was 16.3 ms. The history rows use an earlier query signature; the separate scan-query test exercises a large matching historical scope.

The final fault run saved, scanned and independently hash-checked all 320 captures at 6.944/s, including 19 injected connection refusals, a 0.75-second service outage and two 0.35-second external SQLite writer holds. Early/middle/late rates were 7.333 / 6.149 / 7.618 saves/s (see the packaged JSON for exact measurements).

A transaction stress run attempted 10,000 mixed state/routing/error/match changes, rolled back 15 batches and observed 142 concurrent dashboard reads. All snapshots were exact with no invariant failures; the final summary matched a full recount. An additional deliberately aggressive 100 ms budget produced 14 deadline-limited snapshots during 151 reads; no counter inconsistency was observed. The normal two-second budget had no deadline failures in the recorded stress run. Initial uninstrumented 100 ms exploratory assertions were investigated through this instrumented repeat.

Maintaining summaries adds bounded write work. In a 50,000-capture insertion/classification probe, combined time increased from 2.45 s to 2.73 s (about 12%). The tradeoff replaces recurring whole-history refresh work with small transactional updates; indexing is not claimed to be universally faster.

Not established: live overnight consistency, eight validated saves/s against Wayback for days, the exact cause of the user's remaining Windows slowdown, antivirus effects, large month-directory NTFS costs, native release executables, or resource-driven CPU/storage worker adaptation. Existing memory-aware worker admission is preserved; no experimental rate controller or periodic process restart was added.
