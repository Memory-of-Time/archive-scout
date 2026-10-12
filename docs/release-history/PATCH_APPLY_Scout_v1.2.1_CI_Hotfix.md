# Scout v1.2.1 — Cross-platform CI hotfix (follow-up only)

**Install over the existing consolidated v1.2.1 patch.** Do not use as a replacement for that earlier ZIP.

Apply both files at the repository root, preserving their paths:

- `archive_scout/projects/backups.py`: restore snapshots using SQLite's transactional online backup API instead of unlinking open WAL/SHM sidecars and replacing the database file. Explicit backup restoration only; no indexing, network, scanning or acquisition behavior changes.
- `tests/unit/test_v121_safety_and_operations.py`: compare file identity instead of path string aliases on macOS (`/var` vs `/private/var`) and Windows (`RUNNER~1` vs long paths). Extend WAL restore regression so a connection held open remains valid after restoring; add corrupted-source integrity rejection test.

The corrections target CI run https://github.com/Memory-of-Time/archive-scout-testing/actions/runs/38194965161 (commit `fe2bcfd246da9031b8cd1e27af2d2369949d1beb`). Both Ubuntu jobs passed there, the four macOS jobs failed path comparisons, and two Windows jobs also encountered an open WAL sidecar permission error.

**Validation after overlay:**

```sh
python -m compileall -q archive_scout tests scripts
python scripts/verify_release.py
python scripts/run_tests.py --output validation/test-results
```

Local test suite and release validation passed; hosted Windows/macOS CI must still be rerun. No changes to v1.2.1 branding, CDX/replay spacing, workers, or project DB schema.
