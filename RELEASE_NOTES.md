# Archive Scout v1.1.2

The interface is restored to v1.0.0 and indexing/replay transport to the tagged
v1.0.2 base, retaining schema13 compatibility and the selected evidence, routing,
encoding, backup, analysis and bounded local-scanning safeguards.

Adaptive pacing, escalating application cooldowns, shared connection recovery
cycles, backend retry heaps, newer pool renewal and ETA controls are removed.
Indexing uses a 2.5-second request interval; replay uses 0.125 seconds. Server
Retry-After deadlines remain authoritative and persist across stop/resume.
A live 429/503 without a usable Retry-After uses a fixed five-second wait;
connection/backend retries use brief fixed waits. Healthy sibling downloads can
finish while new admissions pause, and pending captures remain durable.

This complete repository removes obsolete feature/test modules, old patch and
cleanup machinery, generated validation logs and duplicate workflow copies.
Canonical workflows, issue forms, templates and dependency configuration are
under .github. Repository verification catches obsolete files before test
discovery; retained tests are not filtered or skipped. Existing project data is
not part of this source repository and must be kept separately.

See the separate delivery validation report for exact checks and limits.
Short local tests do not establish days-long live-service throughput or identify
the original connection-failure cause.
