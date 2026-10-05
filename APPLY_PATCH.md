# Archive Scout v1.0.6 patch files

Apply these files over the v1.0.5 repository prepared in the preceding release. This is a patch-file package, not the complete repository or an executable distribution.

1. Extract the ZIP.
2. Copy the changed files to the matching relative paths in your v1.0.5 repository, replacing the existing copies. Include `.github/workflows/tests.yml`.
3. For GitHub browser upload, open each matching repository folder and upload that folder's changed files. The entire package is below 100 files. Do not upload the outer extraction folder as a new repository folder.
4. `APPLY_PATCH.md` and `PATCH_MANIFEST.json` are delivery instructions/verification metadata; they do not need to be committed.
5. Run the existing test/build workflows, then create the v1.0.6 release/tag when ready. No GitHub publication or native builds were performed here.

Existing projects remain schema 12. Keep your project database and captures. No file deletions or new runtime dependencies are required. A source installation may need its normal package reinstall to update installed version metadata.

Validation: 411 tests, 410 passed, one native-display skip; compilation passed; 100,000-row offline benchmark completed with four database transactions, zero duplicate work and zero unchanged-row writes. Network regression tests use only mocks and loopback fixtures; live Wayback throughput has not been benchmarked.

The patch improves pooled fallback recovery, received-prefix retention, early curl throttle/redirect handling and bounded text retry scheduling. It preserves current scanner/classifier and request ceilings. See docs/RELEASE_1_0_6.md and docs/NETWORK_PERFORMANCE.md for scope and details.
