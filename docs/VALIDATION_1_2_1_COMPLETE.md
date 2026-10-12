# Scout 1.2.1 complete repository validation

## Source provenance

- Source baseline: `Memory-of-Time/archive-scout` main at public v1.2.0, matching `Scout-v1.2.0-Complete-Repository.zip`; sampled Git blob IDs for `constants.py`, `cdx/indexer.py`, `database/connection.py`, and `.github/workflows/tests.yml` matched the repository.
- Overlays applied in order: v1.2.1 operations and safety, cross-platform CI hotfixes 1 and 2, unified completeness/indexing/resume/encoding, media retry-selection CI fix, persistent 504 CDX recovery.
- Additional safeguards: read-only upgrade inventory inspector and compatible interrupted scan-run reuse, with unit regression coverage.
- Retains SQLite schema 9 and existing CDX semantic query signatures for unchanged inventory settings.

## Executed tests

- Local Linux Python 3.13.5: **295 tests run, 0 failures, 0 errors, 6 skips** (headless GUI tests).
- `python -m compileall -q archive_scout scripts tests`: passed.
- `python scripts/verify_release.py`: passed.
- `python scripts/benchmark_offline.py --cdx-rows 1000 --result-rows 100 --body-bytes 1024`: passed; returned 1,000 parsed/indexed CDX records and 0 writes on the repeat no-op upsert.
- Simulated v1.2.0 schema-9 project: **exactly 650,000** capture records loaded, and v1.2.1 reopened it without changing capture count, original query signature, document count or the saved CDX resume key. The read-only before/after inventory comparison found no lost capture identity samples. Interrupted `downloading` states safely reverted to `pending`. This was an offline **synthetic fixture**, not the user's real project or a live Wayback test.

## Not verified here

- GitHub Actions hosted Windows/macOS jobs have **not** been run on this assembled ZIP. Upload to a branch and wait for the full matrix before treating the release as CI-qualified. Test results from earlier overlays are not a substitute for this final source tree's hosted run.
- Signed binaries and release assets were not built or published.
- Live Wayback availability, true complete archival coverage, and sustained real-world download throughput were not proven.
- The user's own 650,000-URL database was not supplied; the preflight script should be run on that project before switching versions.

## Upgrade verification commands

```bash
python scripts/check_project_resume.py "/path/to/existing/project" --snapshot before.json
# close v1.2.0, back up the project, open it with v1.2.1, and stop it
python scripts/check_project_resume.py "/path/to/existing/project" --compare before.json
```

The checker never writes to the existing database and refuses to create an absent database. Preserve `before.json` alongside an independent project backup.
