# Apply Archive Scout v1.0.9

This replacement-file patch applies to the supplied **v1.0.8 / schema 13** source, `archive-scout-main(9).zip`, SHA-256 `1a8f67d4119f91d6be3b520ea1cd9891886ba55041529662f64a1c239b4a7682`. It is source code, not a compiled application or a complete repository. Rebuild the packaged GUI and CLI after applying it. No source deletions or schema migration are needed.

The updated helper also accepts the exact earlier v1.0.9 candidate distributed in `ArchiveScout-v1.0.9-Source.zip` with SHA-256 `d2e0e9b1c0171450199674b37fb3684258775f595d5c09a214c1803694e840c1`. Use the helper from this updated patch when upgrading that candidate. The manifest permits only recorded earlier file hashes; local edits still stop all copying. A completed current patch needs no additional copies.

1. Close Archive Scout and extract `ArchiveScout-v1.0.9-Patch.zip` into a separate folder.
2. Run the helper from that extracted patch folder with Python 3.11 or later:

   ```bash
   python scripts/verify_patch.py --project "PATH_TO_V1_0_8_SOURCE"
   python scripts/verify_patch.py --project "PATH_TO_V1_0_8_SOURCE" --apply
   ```

   Preflight checks every replacement checksum and its exact before/after hash. A local mismatch stops all copying. Apply backs up original source files in an adjacent `ArchiveScout-v1.0.9-source-backup-*` folder and rolls back failed copies. Reapplying a completed patch is safe. The helper never opens project databases or capture files.

3. Install and validate the patched source in an isolated environment:

   ```bash
   python -m pip install .
   python -m pip check
   python -m compileall -q archive_scout tests scripts
   python scripts/verify_release.py
   python -m unittest discover -s tests -p "test_*.py" -v
   python scripts/verify_packaged_scan.py --encoding-stress
   python scripts/benchmark_offline.py --cdx-rows 100000 --output benchmark.json
   ```

4. Upload/commit replacements at their original relative paths. Run **Tests** on that commit, then run **Build All Platforms** manually to produce the applications. Install the resulting platform package before opening your existing project. Source files cannot change an already-frozen executable.

Keep `.github/workflows/tests.yml` and `.github/workflows/build-and-release.yml` at those exact paths, including the leading period. Their `github/workflows` copies are convenience mirrors. Do not flatten folders or upload the ZIP as a source file. The manifest records the actual file count, kept below 100. On macOS, Command+Shift+Period reveals `.github`.

Schema 13, capture files, scan history, reviews and report selections remain compatible. Changed local imports create new immutable evidence. Download-and-scan now overlaps acquisition and local processing; disable **Scan saved captures while downloading** to run the phases separately. Automatic local scanning uses up to four workers, overlap up to two. Two workers offer lower total memory. Explicit thread mode is available, but CPU-heavy regex work can delay Stop in that mode.

Use the Errors checkbox or select unavailable error rows for explicit manual rechecks. Automatic acquisition still skips permanent errors. Indexed-URL report output continues to follow the Reports settings.

Tests covers Python 3.11/3.12 on Windows, Linux and macOS. Builds additionally verify a complete process scan through each frozen CLI. Tagged Windows releases retain the existing signing requirements. Remote GitHub jobs and native Windows/macOS builds were not run in this Linux workspace. Confirm them on the actual commit before tagging `v1.0.9`.

`PATCH_MANIFEST.json` and `SHA256SUMS.txt` verify the replacements. The separate validation archive contains executed logs, scale/fault results and reproduction harnesses. See [release notes](docs/RELEASE_1_0_9.md) and [validation scope](docs/VALIDATION_1_0_9.md).
