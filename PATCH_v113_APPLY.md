# Archive Scout v1.1.3 patch files

**BASELINE REQUIRED:** the complete v1.1.2 repository from `archive-scout-main (1)(1).zip`, SHA-256 `7784b85e5a23d01d51d04d2c79809b5885dc12484de0efafbf086891133e8e92`.

This ZIP contains **26** modified/new repository files, relative to the repository root. It is not a standalone install. Preserve the directory paths inside the ZIP (`archive_scout/`, `tests/`, `scripts/`, `packaging/`, `.github/`, etc.). Existing personal Archive Scout project folders and their SQLite databases must NOT be overwritten.

## Apply

1. Back up your v1.1.2 Git repository (or commit your current changes).
2. Extract this ZIP directly into the v1.1.2 repository root with overwrite enabled, or use `git`/GitHub Desktop to commit the changed files while preserving paths. Do not flatten folders in a browser upload. The extra `PATCH_v113_*.md/json` delivery files are informational and need not be committed.
3. Verify from the repository root:

```powershell
python -m pip install .
python scripts/verify_repository.py
python scripts/verify_release.py
python scripts/run_tests.py
python scripts/verify_packaged_scan.py --encoding-stress
```

4. Push to GitHub and verify the **Tests** workflow on Ubuntu, Windows, and macOS (Python 3.11/3.12) before tagging `v1.1.3`. The CI workflows are included and version-pinned to 1.1.3. Hosted GitHub Actions jobs have NOT been executed in this offline workspace.

## Local validation

- Canonical `scripts/run_tests.py`: **435 tests, 431 passed, 0 failed, 0 errors, 4 skipped** (Linux/Python 3.13.5; the skipped tests require an available GUI display or other optional conditions).
- Separate Xvfb Tk regression and 940x680 full-interface smoke: **passed**, including access to wide tabs.
- `compileall`, repository verification, release identity: **passed**.
- Spawn-safe scanner benchmark: 1,200 synthetic bodies, 1 vs 4 worker result SHA-256 identical; 1 worker initial 3.414s, 4 workers 2.438s (local fixture).
- Request-spacing fixture: 10,000 virtual admissions and 90 loopback saves, fixed-paced; actual WAN performance unverified.

Validation is local and not evidence that eight fresh saves per second will persist on live Wayback across multi-day runs. The audit's large-scale endurance and native device tests remain open.

See `RELEASE_NOTES.md` for deliberate omissions and unchanged fixed traffic policy.
