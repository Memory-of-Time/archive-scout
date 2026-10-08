# Archive Scout v1.0.9 platform fixes

Apply this small follow-up **after the previous v1.0.9 patch**. It does not include that earlier patch. Version remains 1.0.9; database schema remains 13.

1. Close Archive Scout and keep a copy of your current source files.
2. Copy the five project files in this ZIP into the matching paths in your repository, replacing the existing files and adding `tests/unit/test_v109_platform_fixes.py`. Keep any unrelated local README edits when merging its download section.
3. Commit/upload those five files. Run the existing **Tests** GitHub Actions workflow on this updated commit. No workflow changes are needed.
4. After the platform matrix passes, build the application using the existing build workflow. Copying Python files alone does not update an already packaged executable.

Changed files:
- `archive_scout/projects/backups.py`: writable, non-truncating handles for Windows backup and restore-safety fsync.
- `archive_scout/network/cancellation.py`: bounded Windows read waits make Stop responsive while retaining the full original timeout and complete response buffers.
- `tests/unit/test_v109_reliability.py`: canonical path comparisons for macOS path aliases; rollback assertions remain intact.
- `tests/unit/test_v109_platform_fixes.py`: six regressions for these fixes.
- `README.md`: working installation/download links near the top, preserving the current repository text.

Local checks: 542 tests passed and one display-dependent test skipped, both normally and with the Windows read path enabled on Linux. Installed CLI smoke test passed for 300 Unicode files and process scanning. Native Windows/macOS GitHub jobs must be rerun after applying this patch; these local checks do not claim a native matrix pass.

The `followup-validation/` folder contains supporting test results and is optional to upload. The `FOLLOWUP_*` documents describe this follow-up only; they do not replace the earlier patch metadata.
