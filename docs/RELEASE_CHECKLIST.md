# Official release checklist

- [x] Rebrand and public version identify Archive Scout v1.2.5 with schema 10.
- [x] Full local regression suite and offline release verifier pass.
- [x] CLI incomplete-status, media-Resume and report snapshot regressions added.
- [x] Report snapshots preserve existing reports and completed scan state on errors.
- [x] Healthy localhost acquisition/scan benchmark near the existing 8/s pacing.
- [ ] Official GitHub Windows, Linux, Intel macOS and Apple Silicon CI matrix passes after push.
- [ ] Native installable builds and GUI signing verified on the actual platforms.
- [ ] Large mixed-history/multi-day live Wayback acquisition and WAL/resource usage verified.
- [ ] Cross-page collapse-selection equivalence, active-scope discovery/selection completeness and the full indexing admission matrix certified.
- [ ] Sudden power-loss durable-save contract verified on physical storage.

An unchecked item is an explicit validation boundary. Do not call passing localhost tests proof of live Wayback availability or error-free 72-hour operation. Do not downgrade or hand-edit schema metadata to open a newer project. Opening a supported schema-9 project requires a safety backup before schema-10 migration.
