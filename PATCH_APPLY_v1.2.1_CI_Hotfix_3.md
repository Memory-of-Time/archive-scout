# Scout v1.2.1 — Cross-Platform Resume CI Hotfix 3

## Affected workflow
- https://github.com/Memory-of-Time/archive-scout/actions/runs/38283078391
- Commit: b2bb70871fd66833281f9fba2bea97c575fe91d0
- Ubuntu Python 3.11/3.12: successful.
- macOS Intel/ARM Python 3.11/3.12 and Windows Python 3.11/3.12: each failed one identical resume relocation test.

## Cause
The test used literal `Path` equality after moving an old v1.2.0 project.
macOS supplies path aliases /var and /private/var; Windows supplies 8.3 path aliases
(`RUNNER~1`) and expanded spelling (`runneradmin`). Both point to the same files.

## Change
Replace two test assertions with `Path.samefile()` identity checks to verify
that reopened project folder and referenced saved document are actually the
intended existing filesystem objects, independent of spelling and aliases.

This changes only tests/unit/test_v121_v120_upgrade_compat.py. It does NOT modify
application source, database schema, resume tokens, captures, downloads, or scan history.

## Apply
From this ZIP, copy tests/unit/test_v121_v120_upgrade_compat.py over the matching
file at the repository root. The changed test was based on the exact GitHub blob
at the failed commit (e9ce753b24aaae35503bec9b0bfb54aa383b9427).

## Validation
- `python -m unittest discover -s tests -p test_v121_v120_upgrade_compat.py -v`: 3 passed.
- `python scripts/run_tests.py --output validation/test-results`: 295 run, 0 failures, 0 errors, 6 skipped (Linux Python 3.13.5).
- `python scripts/verify_release.py`: passed.
- GitHub hosted Windows/macOS tests: NOT YET RERUN after applying this ZIP.

After applying, commit and push to trigger the complete 8-job GitHub Actions matrix.
Do not modify or delete any existing project data.
