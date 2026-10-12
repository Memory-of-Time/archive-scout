# Continue a Scout 1.2.0 archive in Scout 1.2.1

**Same project. Same schema 9. No reset is required.** The v1.2.1 source contains the cumulative operations, CI portability, indexing/resume and encoding fixes. This document applies to projects created in Scout v1.2.0 (schema 9). It does not authorize opening unrelated historical schema-13 databases.

## Before upgrading a large existing project

1. **Stop Scout v1.2.0 gracefully.** Let it finish committing and close every Scout GUI, CLI, and bot process using the project. Do not delete or rename the old project directory. Closing cleanly matters when SQLite uses WAL.
2. **Back up the whole project folder** separately from the Scout application source. Include `project.json`, `archive_scout.sqlite3`, `captures/`, `media/`, and existing reports. If copying during an active SQLite connection, plain copying of the `.sqlite3` file is *not* a consistent database backup. Stop the program or use Scout's SQLite-aware Backup operation. Keep at least one untouched original.
3. Extract the Scout v1.2.1 complete repository into a **different program folder**; do not extract over the 650,000-URL project folder. Use Python 3.11 or 3.12 on Windows/macOS/Linux where possible.
4. While the project remains stopped, create an independent **read-only baseline**:

   ```powershell
   python scripts/check_project_resume.py "D:\archives\your-scout-project" --snapshot "D:\archives\scout-before-v121.json"
   ```

   Replace the example paths with real paths and run from the extracted v1.2.1 source directory. The script refuses a missing database. It records capture/document/media counts, scan and index checkpoint totals, and sampled immutable capture IDs, without modifying project data. The snapshot file should be stored outside the project folder if possible.
5. **Open the existing project's `project.json`**, not a new project. Keep the original target list, date bounds, collapse scope, filters and extra CDX parameters unchanged while confirming continuity. Your saved 2.5-second CDX spacing (if used) and download pacing persist through the project configuration. The `index`, `resume`, and download operations read the same database and committed checkpoint records.
6. After v1.2.1 opens and has resumed or inspected the project, you can recheck it using:

   ```powershell
   python scripts/check_project_resume.py "D:\archives\your-scout-project" --compare "D:\archives\scout-before-v121.json"
   ```

   Stop Scout first for a consistent snapshot. A successful check confirms that historical inventory counts and sampled capture identities were not lost. **It is not proof that every remote file is available or that scanning is complete.**

## What is preserved

- Indexed captures and their IDs, query signatures, download states, download attempt records and classification evidence.
- `index_state`, range coverage, saved resume keys and complete Timemap page checkpoints.
- Saved text documents, media captures, scan-run history and match records, unresolved error evidence and associated reports.
- `downloading` states left from a process that has stopped/crashed are moved back to `pending` on the next open so that incomplete operations can retry. Already downloaded/scanned records stay intact. Partially downloaded media are reused only when Range response continuity can be validated.
- When the `Resume` operation finds a compatible **interrupted** scan run, it now reuses that run's ID and existing matches rather than starting its scan accounting at zero. An unrelated or incompatible scan creates a new run; older scan results remain stored.

## Important limitations

- No **real** 650,000-URL user database was provided for migration tests. A synthetic schema-9 project with exactly 650,000 captures was created and reopened; its capture count, sample identities and CDX resume token survived. Your actual files and configuration still require the baseline check.
- Changing inventory-defining options such as target domains, date limits, collapse, and CDX filters can intentionally change the active query identity and show different progress. First validate the old settings unchanged.
- The *current operation's* progress counter may restart at zero when a **new** operation is initiated even though durable project-wide progress and scan results remain. Verify cumulative database counts, not just a new-operation percentage.
- Previously interrupted or skipped downloads remain retryable only to the extent that Wayback can still provide their bytes. A missing remote object is not a local resume bug.
- Partial transfers without trustworthy validators cannot be blindly concatenated. Scout must restart those requests or preserve them as unresolved.
- Project relocation only rebases verified project-owned internal document/media paths. Missing actual files cannot be recreated from a database entry alone.
- GitHub Actions Windows/macOS/Linux validation is required before calling the release cross-platform certified; a local Linux pass is not a hosted CI result.

## Troubleshooting

If you see 0 indexed captures after opening, **do not index a new project yet**. Stop Scout and check whether the folder selected contains the old database. Run the read-only preflight tool on both the original and selected paths. Never rename the SQLite database, edit `schema_info`, drop tables or delete WAL files to force migration. Preserve the original project and request help with the exact error.
