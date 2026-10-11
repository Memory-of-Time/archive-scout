# Scout v1.2.0 — Dashboard refresh hotfix

Apply this small patch at the **repository root** of the Scout v1.2.0 complete repository. It also works if you previously applied the Detective Ra indexing add-on. Overwrite the two matching Python files.

## Files changed
- `archive_scout/ui/main_window.py` — removes a stale call to the nonexistent `project_identity()` method that prevented *both manual and automatic* dashboard refreshes before a SQLite query could even begin.
- `tests/unit/test_v124_index_dashboard.py` — adds a regression test that reproduces the bug without supplying an artificial `project_identity()` method.

No network, indexing, acquisition, scanner, database, configuration, or theme source files changed. No schema change. The dashboard still reads saved totals through a background thread.

## Validation
- Linux Python 3.13.5 full local test runner under Xvfb: 249 tests, 0 failures, 0 errors, 2 skips.
- `python scripts/verify_release.py` passed.
- `python -m compileall -q archive_scout/ui/main_window.py tests/unit/test_v124_index_dashboard.py` passed.
- Windows/macOS GitHub Actions runs have not been executed for this patch.

After applying, restart Scout. Click **Refresh dashboard** and allow time for the SQLite snapshot query. Auto mode, when enabled, only refreshes while the Dashboard page is visible.
