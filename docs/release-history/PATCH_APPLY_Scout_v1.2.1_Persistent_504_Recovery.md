# Scout v1.2.1 — Persistent CDX HTTP 504 Recovery Add-on

**Type:** incremental overlay on the current v1.2.1 patched repository. **Not** a complete repository and **not** intended for v1.2.2 or newer code. Do not apply it to an unpatched v1.2.0 checkout.

## Files

- `archive_scout/cdx/indexer.py`: bounded HTTP 504 subdivision with persistent checkpoints.
- `tests/unit/test_v121_gateway_timeout_subdivision.py`: 8 regression tests automatically discovered by the existing GitHub Actions test workflow.
- `PATCH_APPLY_Scout_v1.2.1_Persistent_504_Recovery.md`: these instructions.
- `PATCH_SHA256_MANIFEST.json`: provenance and file hashes.

## Apply

1. Ensure your repository contains the prior **v1.2.1 consolidated patch**, both cross-platform CI hotfixes, the completeness-audit overlay, the recovery/resumability/CDX-encoding add-on, and the media retry CI test hotfix.
2. Close Scout or stop any running operations before replacing Python source. Extract this ZIP **at the repository root**, preserving relative paths and replacing `archive_scout/cdx/indexer.py`.
3. Run `python -m compileall -q archive_scout tests scripts`, `python scripts/verify_release.py`, and `python scripts/run_tests.py --output validation/test-results`.
4. Push/commit and rerun `.github/workflows/tests.yml` in GitHub Actions. The hosted Windows/macOS matrix has **not** been run for this add-on.

## Exact behavior

- A single HTTP 504 **does not** subdivide a CDX window. A **second consecutive** HTTP 504 from the same unfinished broad window can replace it with **two contiguous nonoverlapping halves**.
- At most **five** exploratory 504 splits are permitted **per saved target/indexing task without a valid CDX response**; the budget persists across Stop/Resume, preventing exponential one-second-window fragmentation during host-wide outages.
- No window shorter than **28 days** is subdivided by this 504-specific rule. CDX requests with an established `resume_key` or numbered-page ledger are never subdivided under this special rule, preserving existing progress.
- Both replacement ranges and the budget are committed together in SQLite. On commit failure, in-memory state is rolled back as well. Range boundaries are inclusive and gap-free to the second; deduplication continues using existing database keys.
- A valid CDX response resets the exploratory budget; connection errors, HTTP 429/503 and mixed 503/504 aggregated failures do **not** authorize this subdivision.
- User-configured CDX spacing, workers, page sizes, replay rate, default retry logic, and healthy requests are unchanged.
- When Wayback is entirely unavailable, Scout will continue to preserve and retry the unfinished work under existing policy. **This add-on cannot force Wayback to serve a failing query**, and does not guarantee that every persistent 504 is caused by query size. It is a bounded exploration, not a proof of backend health.

## Test evidence

Offline Linux Python 3.13.5: **294 tests, zero failures, zero errors, six skips**. Targeted new test module: **8 tests passed**. `verify_release.py`, compileall, and offline benchmark smoke test passed. Production Wayback performance and hosted Windows/macOS CI not verified. Existing test workflow automatically picks up the new `test_*.py` module; no workflow changes are necessary.

Source baseline `archive_scout/cdx/indexer.py` git blob SHA-1: `aeda7e2481570150e5f72c458fe781b91404d40e` (matching latest GitHub Scout v1.2.1 file when prepared). Use this to avoid overwriting unrelated changes.
