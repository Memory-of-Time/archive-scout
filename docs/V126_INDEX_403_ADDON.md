# Archive Scout v1.2.6: index HTTP 403 add-on

Apply on top of the previous v1.2.6 download-pass patch, before its cumulative 403 update. This is a delta/add-on, not a cumulative repository patch. Public version 1.2.6 and SQLite schema 11 remain unchanged.

The refusal itself comes from Wayback. Its exact cause is unverified. Scout previously saved the checkpoint and then raised the 403 out of the indexing loop, terminating the whole operation.

This correction records a refused text/direct-media index scope as unfinished, preserves its exact cursor/pages and seen count, and continues other scopes. Available captures can reach the existing acquisition/report phases. The final operation still reports incomplete coverage when a refused scope remains. A later operation retries its saved cursor; no false empty coverage is recorded. HTTP 400 configuration errors still surface. Pacing, filters, target dates, schema and settings are preserved.

Bare/wildcard targets display their real host rather than unknown. Query refusals do not become host-wide replay exclusions; existing replay-policy blocks remain respected.

Only four application files change: cdx/indexer.py, media/indexer.py, site_status.py and database/repositories.py. Two existing tests are updated to expect incomplete coverage without an escaping 403; eight new regressions cover cursor retention/recovery, continued targets and downloads/reports, classified 403 variants, media continuation, all-blocked selections, non-403 propagation, target labels and scope-safe policy.

The add-on was applied separately to a saved copy of the preceding download-pass source. Its full suite ran **713 tests: 709 passed, four desktop-display tests skipped, zero failures/errors**. Exact results are included in the validation folder. Native Windows/macOS/frozen execution and days-long live-service behavior remain unverified. No assertion is made that the remote 403 has been removed.

Extract over the repository root, merge matching folders, and rebuild/reinstall. Do not delete existing folders. Alternatively apply the included unified patch once to that prior patched baseline. The manifest supplies prerequisite and replacement hashes.
