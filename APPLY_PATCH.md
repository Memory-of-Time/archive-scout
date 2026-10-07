# Apply Archive Scout v1.0.8

This is a replacement-file patch for the supplied **v1.0.7 / schema 12** source tree (`archive-scout-main(8).zip`). It is not a complete repository or an installer. Do not overlay it on an unrelated branch or the v3 beta. No source files need deletion.

1. Close Archive Scout. Keep a copy of your source checkout and project data.
2. Extract `ArchiveScout-v1.0.8-Patch.zip` into a separate folder. The alternative `.tar.gz` contains the same files. Use only one archive.
3. With Python 3.11+, run the helper from that extracted folder, pointing at your existing checkout:

   ```bash
   python scripts/verify_patch.py --project "PATH_TO_YOUR_CHECKOUT"
   python scripts/verify_patch.py --project "PATH_TO_YOUR_CHECKOUT" --apply
   ```

   The first command checks package integrity and every changed source file against its before/after hash. It does not write. The second copies verified replacements and saves original source files in an adjacent `ArchiveScout-v1.0.8-source-backup-*` folder. A mismatched local file blocks the entire preflight. Repeating a completed apply is safe. The helper does not open or modify project databases or captures.

4. Install and check the patched checkout in an isolated Python environment:

   ```bash
   python -m pip install .
   python -m pip check
   python -m compileall -q archive_scout tests scripts
   python scripts/verify_release.py
   python -m unittest discover -s tests -p "test_*.py" -v
   python scripts/benchmark_offline.py --cdx-rows 1000 --result-rows 100 --body-bytes 1024
   ```

The first normal open of an older project creates a database backup before migrating to schema 13. Keep that backup if you might return to v1.0.7: the old program cannot read schema 13 directly. Capture files, reviews, notes and deterministic scores are preserved. Existing report selections remain in effect; enable the indexed-URL inventory and its desired fields in **Reports** when needed.

## GitHub upload and release

Commit the changed files at their original relative paths. For web uploads, select the extracted contents, not the ZIP itself or an extra enclosing patch folder. Keep nested directories intact. The file count is recorded in `PATCH_MANIFEST.json` and is below GitHub's 100-file upload limit.

**`.github/workflows/tests.yml` must keep its leading period and its full path.** On macOS, `Command+Shift+Period` reveals hidden files in Finder. `github/workflows/tests.yml` is a synchronized convenience copy; GitHub runs the `.github` copy. Do not flatten either workflow into the repository root. `scripts/verify_release.py` checks this placement and both mirrors.

Run **Tests** on the commit after upload, including the Python 3.11/3.12 Windows, Linux and macOS jobs. Wait for that matrix to pass before tagging `v1.0.8`. The existing build-and-release workflow creates GUI and CLI packages; Windows signing still requires its existing repository configuration. Native installer builds and remote GitHub runs were not executed in this Linux workspace.

`PATCH_MANIFEST.json` describes changed files and before/after hashes. `SHA256SUMS.txt` checks those files and the manifest. The separate validation archive contains the final local test/workflow logs, benchmark data and offline reproduction harnesses. See [release notes](docs/RELEASE_1_0_8.md) and [validation scope](docs/VALIDATION_1_0_8.md).
