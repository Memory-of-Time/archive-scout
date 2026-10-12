# Scout v1.2.1 — CI Hotfix 2 (patch-only)

**Apply to the existing v1.2.1 repository AFTER the previous Cross-Platform CI Hotfix Add-On.** This is not a standalone repository or a replacement for the consolidated v1.2.1 patch. Copy both file paths into the repository root and replace the existing files. Do not copy the ZIP as a single file into the repository.

Targets the failures of GitHub Actions run https://github.com/Memory-of-Time/archive-scout-testing/actions/runs/38195622970 at commit `66f598df257da23cd4ae1e34142f131bf897230c`.

## Exactly two code/test replacements

- `archive_scout/projects/paths.py`: Fix project-owned document/media path rebasing when the saved absolute file path and project-root marker use different filesystem aliases (`/var` vs `/private/var`, Windows 8.3 vs long names). Resolve aliases first; use a conservative parent-directory-suffix match when the old project folder no longer exists. Keep unrelated external paths untouched. This runs at project database opening / relocation maintenance, not per capture.
- `tests/unit/test_v121_safety_and_operations.py`: Add real symlink and synthetic Windows 8.3 alias regressions; replace the Windows-invalid `?` test filename; explicitly close SQLite read connections so Windows can clean up the temporary directory.

The previously fixed `archive_scout/projects/backups.py` is intentionally NOT included or changed. It must remain at the preceding hotfix revision.

## Validation

- Local Python 3.13 Linux suite: **266 tests, 0 failures, 0 errors, 6 skips**. The suite executes against the complete v1.2.1 source tree with the earlier hotfix already installed.
- Focused restoration tests: **11 passed**.
- Source compiles and release verification passed.
- **Hosted Windows/macOS GitHub Actions have NOT yet been rerun on these exact files**. Run `python scripts/run_tests.py --output validation/test-results` via the existing Actions workflow after committing the overlay.
- No changes to indexing, downloading, scanner logic, request pacing, retry timings, connection monitoring, worker counts, or database schema.
