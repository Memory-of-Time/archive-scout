# Changelog

## Archive Scout v1.2.5 — strict acquisition and media integrity

- Require completed selected index evidence before new download requests at operation and downloader entry points.
- Add schema-10 indexed active media retry scheduling with a mandatory pre-upgrade safety backup.
- Fix misleading MIME/binary phrase and short `206` media validation errors; preserve stream-first acquisition.
- Correct media query identity for target overrides; avoid collapsed earliest/latest selection without timestamp proof.
- Replace SQLite OR-expression rescan scope expansion with indexed temporary membership.
- Keep direct versus external embedded retries separated, and adopt large legacy media error histories in bounded batches.
- Preserve existing report snapshots, scanning, CLI/GUI packaging and healthy replay pacing.


## Archive Scout v1.2.4 — official operations/reports integration

- Resume now carries forward the original direct or external media phase and its selected settings.
- GUI/CLI expose durable incomplete operation outcomes; successful reports do not imply complete acquisition.
- Scan completeness is checked before media handoff, and report failures cannot invalidate committed scan evidence.
- Report snapshots may be written during a live operation with an independent read-only worker and unique completed-set publication.
- Media outputs honor configured files and columns. Obsolete patch-baseline artifacts removed from the official source tree.

### Indexing and acquisition stabilization retained in v1.2.4

- Fair text/media target scheduling with saved incomplete cursors, retry eligibility and optional per-target indexing time budget.
- Reject malformed, wrong-scope and unexpectedly continued CDX responses instead of marking incomplete inventories complete.
- Protect media index cursor/paging/collapse identities across HTTP outages and record committed media page evidence.
- Fix cold-start media indexing exceptions and policy-blocked media false completion.
- Skip `robots.txt` filenames at text acquisition admission; preserve the existing 76-suffix text/media routing policy.
- Preserve overlapping target membership without duplicating capture bodies, prevent migrations without a successful backup and bound estimated concurrent CDX page response bytes.
- Preserve existing schema-9 project compatibility and healthy local acquisition pacing; see `docs/V124_INDEXING_RECOVERY.md` for unresolved protocol limits and test scope.

## Archive Scout v1.2.3 — repository organization and public rebrand

- Retains the later v1.2.4 implementation supplied as the baseline, under the requested public version 1.2.3.
- Consolidates release instructions, removes obsolete hotfix/patch files, and aligns app metadata, packaging, installers, release assets, and CI checks.
- Preserves schema-9 databases, indexing checkpoints, acquired files, prior scans, operation behavior, and legacy `scout` CLI aliases.

## Earlier implementation lineage (retained)

The supplied repository incorporated an earlier internal v1.2.4 development line: persisted acquisition retry scheduling, streamed validated bodies, independent scanning, SQLite concurrency recovery, repeatable WAL-safe merges, CDX recovery, media routing, and reporting fixes. These features remain present in the rebranded release. This history is not a promise of verified multi-day throughput.
