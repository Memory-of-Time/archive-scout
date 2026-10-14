# Changelog

## Archive Scout v1.2.3 — repository organization and public rebrand

- Retains the later v1.2.4 implementation supplied as the baseline, under the requested public version 1.2.3.
- Consolidates release instructions, removes obsolete hotfix/patch files, and aligns app metadata, packaging, installers, release assets, and CI checks.
- Preserves schema-9 databases, indexing checkpoints, acquired files, prior scans, operation behavior, and legacy `scout` CLI aliases.

## Earlier implementation lineage (retained)

The supplied repository incorporated an earlier internal v1.2.4 development line: persisted acquisition retry scheduling, streamed validated bodies, independent scanning, SQLite concurrency recovery, repeatable WAL-safe merges, CDX recovery, media routing, and reporting fixes. These features remain present in the rebranded release. This history is not a promise of verified multi-day throughput.
