# v1.1.0 validation

Tested against the supplied v1.0.9 archive on Windows, Python 3.12.14, with the declared runtime dependencies installed in an isolated workspace environment.

- Full offline suite: **567 passed**, 0 skipped, 0 failures, 0 errors (54.875 seconds).
- Unmodified v1.0.9 baseline: 543 tests passed.
- Source compilation, installed v1.1.0 package metadata, schema 13, numeric/display Windows versions, canonical `.github/workflows` paths and matching `github/workflows` mirrors passed.
- Complete CLI/spawn smoke with UTF-8 content, encoding stress, all source bytes, matches, SQLite reopen and FTS integrity passed.
- Offline acquisition/scanning benchmark passed.
- The real fixed limiter completed two million virtual admissions at exactly 0.125-second spacing (eight scheduled starts/second), plus two million admissions with 19 injected 60-second server waits. Every server deadline and post-recovery no-burst check passed. Opt-in and disabled samples both honored server waits; disabled pacing accrued no adaptive debt.

New regressions cover adaptive default-off behavior, mixed clients and turnover, config/CLI/Resume, live cooldown preservation, connection-only recovery generations, unused probes, backend requalification, delayed retry wakeups, admission-only draining, unfinished healthy text/media siblings, and queued captures.

Evidence: [test summary](../_patch_meta/v1.1.0/test-summary.json), [full test log](../_patch_meta/v1.1.0/tests.log), [request-spacing benchmark](../_patch_meta/v1.1.0/request-spacing.json), [CLI/spawn smoke](../_patch_meta/v1.1.0/cli-spawn-smoke.json), [offline benchmark](../_patch_meta/v1.1.0/offline-benchmark.json), [release identity](../_patch_meta/v1.1.0/release-identity.json).

The GitHub Tests and release workflows use `scripts/run_tests.py` and upload logs plus JSON summaries as workflow artifacts. Workflow configuration was inspected locally; hosted Linux/macOS/Python 3.11 jobs have not been run for this unpublished patch. No remote repository changes or release builds were made.

These are offline correctness and virtual scheduling results. No Internet Archive downloads were made. Live eight-completions/second throughput, million-snapshot network stability, and the cause of the initial connection failures remain unverified.
