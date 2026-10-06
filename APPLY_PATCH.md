# Apply Archive Scout v1.0.7

This ZIP contains replacement patch files, not a complete repository. Apply it over the prepared v1.0.6 source, with or without the separate Windows test-fixture fix. Do not apply it to an older release or to an unrelated later development tree.

1. Close Archive Scout and keep a copy of your current source checkout.
2. Extract this ZIP into that checkout's root, preserving the relative paths and replacing the corresponding files. No source files need deletion. Existing project databases and capture folders do not need changes.
3. Make sure `.github/workflows/tests.yml` retains its leading period. This is the actual GitHub test workflow. The `github/workflows/tests.yml` copy is synchronized for convenience.
4. Commit the replacement files. Wait for the GitHub Tests matrix to pass on your commit before creating the `v1.0.7` release tag. The existing build workflow builds GUI and CLI packages for all three platforms. Tagged Windows builds retain the existing Artifact Signing configuration requirement.

For a local source check on Python 3.11 or newer:

```bash
python -m pip install .
python -m compileall -q archive_scout tests scripts
python -m unittest discover -s tests -p "test_*.py" -v
```

`PATCH_MANIFEST.json` lists the replacement files and before/after SHA-256 values. `SHA256SUMS.txt` covers the repository patch files. The `_patch_meta` folder contains local validation evidence. These metadata files may be kept outside your checkout if preferred.

Local verification: 439 tests, zero failures, one native-display skip; 28 new regressions; source compilation; installed package version/import/CLI checks; workflow YAML parsing; and a 100,000-row offline indexing benchmark. All patch source files were compared after overlay on both v1.0.6 variants. Windows/macOS GitHub jobs and installer builds were not run in this Linux environment, so their success must be confirmed on GitHub.

Normal pacing stays at 2.5 s for CDX and 0.125 s for replay starts. Existing conservative no-header defaults remain 60/600 s, while chosen shorter fallback settings are now honored. Valid server Retry-After deadlines are preserved. Full CDX/body/Range validation and text/media differentiation remain intact. See `docs/RELEASE_1_0_7.md` for the changes.
