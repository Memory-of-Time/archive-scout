# Archive Scout 1.0.9

This patch implements the v1.0.8 long-run audit against the supplied source. The lost historical version and v1.0.4 are not speed references. Schema remains 13 and GUI/CLI share the engine. Healthy service throughput, injected failures and large manifests are tested separately; no change guarantees a particular rate from Internet Archive.

## Acquisition and connections

The displayed download rate now counts fresh, validated, manifest-committed saves over the recent minute. Adoption, wire starts, retries, invocation averages, 10/60/300-second rates and saved bytes are separate. Encoding/validation errors, service responses, storage errors, cancellations and transport failures have distinct counters. An HTTP response alone is not a saved capture.

HTTPX/urllib3 have request-scoped socket cancellation for stalled headers/body reads, including reused connections. Curl disables output buffering and preserves validated identity prefixes. Workers drain before pool/file ownership changes. Initial connect/TLS setup follows configured timeouts when no interruptible socket is available; DNS resolution follows the operating system resolver behavior. HTTPX's isolated adapter is checked against its supported 0.28 series, now constrained in runtime requirements.

Two consecutive proven connection/protocol failures can request pool renewal after all active owners drain. Success clears that request. Renewal preserves backend/origin cooldowns and the service gate. HTTP 429/503, Retry-After, pacing floors, recovery probes and bounded retries remain authoritative. Pools are not renewed on a timer.

Shared strict decoding handles BOMs, byte order, NUL patterns, legacy declarations and incomplete multibyte previews. Contradictory wide declarations cannot garble ordinary ASCII keywords. Ambiguous/malformed wide bodies remain visible failures with complete source bytes retained. Such failures do not declare a healthy transport unavailable. Recorded encoding reaches full-text, research, analysis and AI excerpt reads. Valid HTML fallback parsing separates head/title from body.

## Local work and resources

Retained download-and-scan processes saved bodies during acquisition with a bounded overlap queue. Parent-owned SQLite and the durable manifest preserve overflow for the final drain. Discard mode retains its stronger commit-before-delete barrier. Pause, failed writes and worker failures preserve resumable sources/queue state.

One scanner resolver controls saved scans, rescans and local retries. Automatic local work uses up to four workers; overlap uses up to two. Large automatic work uses spawn processes, and regex work is isolated even when small. Explicit scanner counts can exceed three without becoming replay workers. Process/thread engines and worker counts are configurable. Frozen entry points call freeze support, and build workflows verify real frozen CLI scans.

The default 256 MiB reservation bounds estimated in-flight work, not total application RSS. It reserves room for decoded/parsed/result/IPC copies. One oversize file is admitted exclusively and processed completely. Worker interpreters, caches and unusual documents can use additional memory. No truncation, sampling or regex substitution is introduced. Explicit thread mode has lower interpreter overhead but heavy regex work can delay cancellation.

Local retry selections spill to SQLite and use saved capture paths even without document rows. Local work runs before network admission during a saved service cooldown. Unavailable-capture rechecks are explicit, manual-only choices; automatic permanent-error eligibility is unchanged. Media retry IDs also stay in SQLite.

Imports use immutable hash-addressed sources and version identities, including changed bytes with the same size/mtime. Earlier evidence, reviews and FTS remain available. Unstable reads/damaged destinations are explicit errors. Integrity streams manifests/issues and disk-backed path membership, checking SQLite, foreign keys and FTS. Stop preserves the previous complete report. Maintenance avoids project-sized Python lists.

Backup snapshots and compressed bytes are validated, then a complete fsynced archive is atomically published before pruning older valid backups. Failure/cancellation preserves the last valid snapshot. Writer `mmap_size=0` and disk-spilled temporary tables reduce mapping exposure and large metadata allocations. Mapping off is not claimed as a complete corruption fix. A later reuse check found a conflicting WAL alongside an intact main file in one validation workspace; that repeat is excluded. Its cause remains unproven. Validation explicitly closes every check connection, rolls back FTS checks and rechecks fresh fixtures after all connections close. Project WAL files are never discarded to force a passing result.

Restore validates an isolated, complete SQLite snapshot before changing the project. Raw SQLite backups include committed WAL data. Restoring an existing project uses SQLite's backup API, preserving its file identity and active readers' transactions; those readers see restored content in their next transaction. A validated safety snapshot preserves the current state. Busy writers or incompatible-page-size reader locks fail safely, and an interrupted copy rolls back. The project database and its WAL/SHM are never replaced or removed outside SQLite. These independently reproduced restore defects are separate from the unresolved validation-workspace WAL finding.

## Analysis, ETA and automation

Duplicates use a disk-backed exact Hamming-radius tree instead of approximate band candidates and the old 2,000-candidate cap. Complete connected groups, including later bridges and exact-identity links, remain. Once all inserted fingerprints belong to one connected group, additional matching edges are redundant and can be skipped; every distinct fingerprint stays available for later comparisons. SimHash is a fingerprint, not proof of byte equality; exact hashes remain separate. Worst-case completeness-preserving searches can still be quadratic.

Differences use `anchored_character_blocks_64_v1`: exact common prefix/suffix plus identical 64-character middle-block overlap. This replaces the old greedy character ratio, whose repetitive-input work could be quadratic. Change detection is independent of rounded similarity. Unique added/removed line counts are complete; 200 lines per side are labeled previews. The existing equal-normalized-hash shortcut still treats equivalent normalized text as unchanged. Cancelled analysis preserves earlier published groups/differences.

Optional dashboard ETA remains off by default. Measured phase progress, known recovery waits and bounded recent project rates can estimate known future work. Unknown discovery/report work is explicit. Retry telemetry does not reset phase samples. History has at most 32 rate records, written at operation completion, with no item history or extra worker. Structured CLI output is UTF-8 across locale/platform defaults.

## Measurements and limits

The unchanged 33,000-file corpus has 128,130,165 bytes, two rule sets and ten literal/regex/required/excluded rules. v1.0.8's fastest audit thread run took 115.18 seconds. The final v1.0.9 automatic four-process run took 35.34 seconds; two took 56.18 seconds. Earlier accepted repeats took 36.55 and 56.12 seconds respectively. The earlier 41.73-second repeat is excluded because its workspace later exposed the conflicting WAL. Accepted results require the exact result digest, 66,000 match records and checkpoint/reopen/FTS checks. Final repeats receive additional delayed ordinary-database checks with two SQLite runtimes.

Four-process conservative parent-plus-worker peak memory was approximately 439 MiB in the final repeat; two was approximately 300 MiB. Sums of individual process peaks can exceed simultaneous RSS. Four favors speed; two improved speed and this conservative memory total versus the audit's roughly 320–366 MiB thread runs. Parent-only memory is not total scanner memory.

A separate source-runtime test disabled both native parser and matcher accelerators in the parent and spawned workers. All 33,000 files scanned in 35.23 seconds with the same complete match digest and passed delayed checks. Its conservative peak sum was approximately 417 MiB. This tests the fallback code paths on Linux, not a frozen macOS or Windows build. A restore of the complete 33,000-document project took 6.32 seconds and preserved exact capture, document, match, review and note digests, including current edits in its safety snapshot.

A full local-hash Research Intelligence build over those 33,000 documents took 148.66 seconds; an unchanged refresh took 0.14 seconds, preserving all deterministic matches. A controlled dense 2,000-fingerprint duplicate fixture fell from 25.44 to 0.56 seconds after redundant-edge pruning, with every member retained. That synthetic result does not measure full document fingerprinting or arbitrary corpora.

Two million metadata rows passed selection/update/checkpoint/reopen with bounded memory. Million-row Integrity avoided the audit's large memory increase while adding SQL/FTS checks. A normal-paced local fault run acquired/scanned 1,200 unique complete files through eight partial-body drops and two HTTP 429 responses: all matched, no unresolved errors, approximately 7.50 saves/s including necessary waits. The validation artifact records the hour-long healthy run and real acquisitions in a two-million-row historical manifest.

These fixtures do not prove millions of live downloads or overnight behavior on the user's packaged runtime. Caches are uncontrolled; native Windows/macOS builds, GUI interaction and remote GitHub jobs are pending. No weaker validation or completeness-sensitive shortcut is used to meet a displayed rate. See [validation scope](VALIDATION_1_0_9.md).
