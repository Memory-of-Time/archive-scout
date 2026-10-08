# Validation

- Linux Python 3.12.14: 543 tests run, 542 passed, one existing display-only skip.
- Same complete suite with Windows read polling forced on Linux: 543 tests, 542 passed, same skip.
- Focused platform/restore tests: 16 passed.
- Strict Windows-style fsync and reads unaffected by shutdown: three defects reproduced before; all three pass after.
- macOS-style symlink path aliases: both failures reproduced before; both tests pass after.
- Installed package, dependency check, compile check, and release metadata check passed (v1.0.9, schema 13).
- Installed CLI: 300 Unicode files imported/rescanned through process workers; complete bytes, matches, clean JSONL, reopened SQLite and FTS integrity passed.
- Offline benchmark: 100,000 CDX rows; result included.
- Only four existing project files replaced and one regression test added. All other original delivered source files checked byte-for-byte unchanged.

Reported GitHub failures were from Tests run 37706676635, commit c4ad2a4ce9be10d2129be063df74bb75ba9a69bf. Windows failed on read-only fsync handles and stalled-read cancellation; macOS failed on path-alias comparisons. Ubuntu jobs passed. Existing workflows remain unchanged.

Native Windows/macOS jobs and frozen executable builds have not been executed here. Rerun the repository's Tests workflow on the patched commit before building/publishing. This targeted fix does not establish long-run download throughput.
