# Scout v1.2.2 — Apply indexing/scanning patch

**Patch type:** file overlay. Built against the first provided Scout v1.2.1 ZIP in the request. Do not apply to a different development branch without comparing its files first.

1. **Stop Scout completely.** Use Scout's backup feature for the existing project, preferably stored on a separate disk; do not copy an open `archive_scout.sqlite3` without its active WAL state.
2. Unzip `Scout-v1.2.2-Indexing-Scanning-Patch-Files.zip`. Copy/extract **its contents** directly into the root of the Scout *source repository*, preserving `archive_scout/`, `tests/`, `docs/`, and other folders. Replace matching source files. Do not place the ZIP itself inside the repository as a file.
3. Keep the existing project directory, `project.json`, `archive_scout.sqlite3` (and WAL/SHM, if present), and every `captures/` and media folder. **Do not** overwrite the project with the repository or start a fresh project to Resume.
4. Reinstall dependencies when appropriate, then run `python scripts/verify_release.py` and `python scripts/run_tests.py`; source tests do not call Wayback.
5. Reopen the same project and use **Resume** or **Rescan** as appropriate. Local saved bodies can now finish scanning without working CDX. View `reports/summary.txt`, the `reports/scan-*` folders, and `reports/last_nonempty_matches_ranked.txt`. The latest `matches_ranked.txt` intentionally represents the latest scan only.

Database schema: **9 (unchanged)**. Text-versus-media differentiation and persisted settings are preserved. Indexing failures do not convert unfinished coverage into an empty completed inventory. A genuinely unavailable Internet Archive endpoint may still need time to recover; the patch cannot guarantee live service availability or sustained 8/s performance.

**Evidence and limitations:** `docs/SCOUT_1_2_2_INDEX_SCAN_AUDIT.md`. The two benchmark JSON files and the canonical test summary are provided separately for reproducibility. No native Windows/macOS GUI or live Wayback verification was executed.
