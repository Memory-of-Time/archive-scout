# v1.0.8 validation scope

Final validation uses a fresh copy of the supplied v1.0.7 source with only the downloadable replacement files applied. It checks package integrity, the apply helper, before/after file hashes, repeat application, rejection of a changed base file and rejection of a tampered patch. Files are compared with the prepared v1.0.8 tree after overlay.

The local workflow checks install the v1.0.8 package, run dependency/import/metadata checks, compile source/tests/scripts, verify release identity and canonical workflow placement, run complete unittest discovery and execute the workflow's offline benchmark smoke command. Detailed results, exact counts, durations, archive hashes and environment versions are recorded in the separate validation archive's `VALIDATION.json` and logs.

New regressions cover transactional FTS replacement across all public search paths, unchanged-index growth, rebuild encoding, same-size/same-mtime Hitlist edits, stale-hit reconciliation, saved corpus limits, cancellation/resume, rollback on a failed batch, SQLite retry parameter limits, media keyset completeness, native automaton sharing, SimHash cluster membership, migration backup/failure/idempotence, backup integrity and bounded virtual-clock ETA behavior. Existing older-schema preservation and network/body/Range tests also remain in the full suite.

Performance fixtures and raw measurements accompany the logs. Scanning uses real parsing/scoring/persistence; acquisition uses a local pooled HTTP server and real production pacing/atomic saves. The only network mapping changes redirect fixture requests to loopback. External services and real API keys are not required.

Environment: Linux x64, Python 3.12.14, SQLite 3.53.1. One native-display GUI test is skipped because no display is available. No interactive GUI session, Python 3.11 runtime, Windows/macOS runner, signed installer build or remote GitHub Actions run was executed here. The supplied workflows retain their Python 3.11/3.12 Windows/Linux/macOS matrix; success must be confirmed on the actual uploaded commit.

The report setting that hid indexed URLs was located by the user. Its existing output and field behavior is preserved; this release does not force a report the user disabled.
