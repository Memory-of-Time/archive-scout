# Archive Scout v1.1.2 repository cleanup

Inspected commit: `219b2e3ec0dba12504425542ae624e6b7d793ab2`.

89 exact file paths: 71 required rollback deletions and 18 optional historical delivery files. This list applies to the inspected repository; it is not a rule to delete similarly named future files.

## Required: removed feature modules (5)

- `archive_scout/downloads/metrics.py`
- `archive_scout/downloads/recovery.py`
- `archive_scout/eta.py`
- `archive_scout/network/cancellation.py`
- `archive_scout/ui/eta.py`

## Required: obsolete test modules (17)

These tests exercise deliberately removed features. Retained safety/scanning tests are in the v1.1.2 test modules. Delete the obsolete modules rather than skipping/filtering test discovery.

- `tests/unit/test_v101_rate_scrolling_audit.py`
- `tests/unit/test_v102_release.py`
- `tests/unit/test_v104_fundamentals.py`
- `tests/unit/test_v105_stability.py`
- `tests/unit/test_v106_connection_stability.py`
- `tests/unit/test_v107_index_reports.py`
- `tests/unit/test_v107_waiting_speed.py`
- `tests/unit/test_v108_patch.py`
- `tests/unit/test_v109_network.py`
- `tests/unit/test_v109_patch_delivery.py`
- `tests/unit/test_v109_platform_fixes.py`
- `tests/unit/test_v109_reliability.py`
- `tests/unit/test_v109_scanning_analysis.py`
- `tests/unit/test_v110_adaptive.py`
- `tests/unit/test_v110_ci_workflows.py`
- `tests/unit/test_v110_recovery.py`
- `tests/unit/test_v111_cooldowns.py`

## Required: superseded delivery notes and evidence (49)

- `APPLY_CI_FIX.md`
- `APPLY_PATCH.md`
- `APPLY_V1_1_1.md`
- `APPLY_WINDOWS_TEST_FIX.md`
- `CI_FIX_MANIFEST.json`
- `CI_FIX_TESTS.log`
- `CI_FIX_VALIDATION.json`
- `FOLLOWUP_APPLY.md`
- `FOLLOWUP_PATCH_MANIFEST.json`
- `FOLLOWUP_SHA256SUMS.txt`
- `FOLLOWUP_VALIDATION.md`
- `_patch_meta/README-APPLY.txt`
- `_patch_meta/benchmark.json`
- `_patch_meta/index-reports.patch`
- `_patch_meta/new-regressions.log`
- `_patch_meta/package-verification.json`
- `_patch_meta/tests.log`
- `_patch_meta/v1.1.0/cli-spawn-smoke.json`
- `_patch_meta/v1.1.0/offline-benchmark.json`
- `_patch_meta/v1.1.0/release-identity.json`
- `_patch_meta/v1.1.0/request-spacing.json`
- `_patch_meta/v1.1.0/test-summary.json`
- `_patch_meta/v1.1.0/tests.log`
- `_patch_meta/validation.json`
- `docs/DOWNLOAD_RECOVERY_COMPARISON_1_1_0.md`
- `docs/RELEASE_1_0_5.md`
- `docs/RELEASE_1_0_6.md`
- `docs/RELEASE_1_0_7.md`
- `docs/RELEASE_1_0_8.md`
- `docs/RELEASE_1_0_9.md`
- `docs/RELEASE_1_1_0.md`
- `docs/RELEASE_1_1_1.md`
- `docs/V105_DOWNLOAD_SPEED_COMPARISON.md`
- `docs/VALIDATION_1_0_8.md`
- `docs/VALIDATION_1_0_9.md`
- `docs/VALIDATION_1_1_0.md`
- `docs/VALIDATION_1_1_1.md`
- `docs/validation-1.0.5.json`
- `docs/validation-1.0.6.json`
- `followup-validation/alias-after.json`
- `followup-validation/alias-before.json`
- `followup-validation/focused-platform-fixes.log`
- `followup-validation/full-suite-forced-windows-reads.log`
- `followup-validation/full-suite.log`
- `followup-validation/installed-cli.json`
- `followup-validation/offline-benchmark.json`
- `followup-validation/platform-after.json`
- `followup-validation/platform-before.json`
- `validation-1.0.7.json`

## Optional: historical validation and delivery files (18)

Archive these elsewhere first if you want to keep release history. Remove the patch manifest/checksums only after applying the patch: the old patch helper requires those delivery files. Normal app operation, the retained tests and CI do not require them. The cleanup helper removes these only with --include-optional.

- `APPLY-v1.1.2.md`
- `PATCH_MANIFEST.json`
- `REPOSITORY_DETAILS.md`
- `SHA256SUMS.txt`
- `SOURCE_VALIDATION.txt`
- `UPLOAD_CHECKLIST.txt`
- `evidence/capacity-current.json`
- `evidence/capacity-stale.json`
- `evidence/cli-spawn.json`
- `evidence/gui-smoke.json`
- `evidence/offline-benchmark.log`
- `evidence/release-identity.json`
- `evidence/test-summary.json`
- `evidence/tests.log`
- `evidence/validation.json`
- `validation/V105_PACING_COMPARISON.json`
- `validation/V1_1_1_TESTS.log`
- `validation/V1_1_1_VALIDATION.json`

## Keep

Keep the remaining runtime modules, tests/migrations, assets, examples, packaging/build scripts, dependencies, pyproject.toml, README and LICENSE. Keep current documentation and both workflow directories: scripts/verify_release.py checks .github/workflows against github/workflows. Keep the other github templates or move them into .github to activate them. Rename gitignore to .gitignore; it is not a deletion candidate.

Never delete project.json, SQLite databases/WAL files, saved captures, media, reports or project backups to clean the source repository. Local build/dist/cache/egg-info folders are regenerable, but are not tracked in this revision and are outside these exact lists.

The cleanup add-on includes guarded deletion, source backups, rollback on deletion failure and instructions for committing the deletions so that GitHub Actions runs the cleaned tree.
