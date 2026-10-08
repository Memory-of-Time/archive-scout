# Apply Archive Scout v1.1.0

This is a replacement-file patch for the supplied `archive-scout-main.zip` (v1.0.9). It contains changed/new repository files and test evidence. It is source code, not a packaged desktop executable.

1. Close Archive Scout and extract this patch into its own folder.
2. From the extracted patch folder, run `python scripts/verify_patch.py --project "PATH/TO/archive-scout"` using Python 3.11 or newer. This checks package hashes and every replaced source file against the supplied base. Local changes are rejected before copying anything.
3. Run the same command with `--apply`. The helper makes a source-file backup, applies verified files, and rolls back completed copies if a copy fails. Reapplying the same patch is safe. It never opens project databases.
4. From your repository, run `python -m pip install .`, then `python scripts/run_tests.py`, `python scripts/verify_release.py`, and `python scripts/verify_packaged_scan.py --encoding-stress`. Existing saved projects keep schema 13.

For a GitHub upload, copy the replacements to their exact repository paths, including the hidden **`.github/workflows/`** directory. The `github/` mirror alone does not activate Actions. Upload to a branch/PR to run Tests. Application installers are produced separately by the existing build workflow.

Adaptive rate limiting is off by default and labeled **experimental — still in testing**. Fixed request ceilings and server waits remain enforced. See `docs/RELEASE_1_1_0.md` and `docs/VALIDATION_1_1_0.md` for behavior and actual validation scope.
