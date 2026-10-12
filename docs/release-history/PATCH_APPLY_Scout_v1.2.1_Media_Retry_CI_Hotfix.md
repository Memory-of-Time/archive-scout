# Scout v1.2.1 — Media Retry Selection CI Hotfix

Applies on top of the existing Scout v1.2.1 patch sequence, including the latest audit/recovery/CDX-encoding overlay. This is a **test-only** follow-up; it does not contain any production code changes.

## Failure addressed

GitHub Actions: https://github.com/Memory-of-Time/archive-scout-testing/actions/runs/38272474408
Commit: `50c98aa9957764aa5b2e238bcfa65a74ce07a44b`
Failing test: `test_large_media_retry_selection_avoids_sqlite_variable_limit`.

The implementation intentionally processes 1,200 retryable media captures as **two batches (1,000 + 200)**. The old test inspected only `mocked.call_args` (the final 200-item batch) and incorrectly required it to contain all 1,200 IDs. The test now checks the **flattened ID sequence across every call**, validating complete, exactly-once, ordered coverage with bounded batches.

The test also uses `try/finally` to close its SQLite connection even on failure; this avoids Windows temporary-file cleanup errors that masked the assertion failure.

## Files

- `tests/unit/test_large_project_stability.py` (replacement)
- `PATCH_APPLY_Scout_v1.2.1_Media_Retry_CI_Hotfix.md` (this guide)
- `SHA256SUMS.txt` (file integrity)

## Installation

Extract the ZIP into your existing **v1.2.1 repository root** and overwrite the matching test file. Do not remove any previous v1.2.1 patch.

## Validation

- Focused test: PASS (Python 3.13, local Linux)
- Full local suite: 286 tests, 0 failures, 0 errors, 6 skipped
- Source compilation: PASS
- `scripts/verify_release.py`: PASS
- Hosted Windows/macOS/Linux CI: NOT YET VERIFIED for this hotfix.

The local checkout has two more discovered tests than the 284-test failing GitHub run. The relevant production files (`archive_scout/media/downloader.py` and `archive_scout/operations.py`) have been verified to match the exact GitHub commit's blob SHAs. No performance-sensitive source was changed. Test selection itself exercises only local SQLite/mock behavior, no Wayback traffic.
