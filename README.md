# Scout 1.2.2

**Scout** is a cross-platform desktop and command-line workspace for finding, preserving, downloading, searching, and reviewing public Wayback Machine captures.

### Download Scout

| Windows | macOS | Linux |
|---|---|---|
| [**Download for Windows (x64)**](https://github.com/Memory-of-Time/archive-scout/releases/latest/download/Scout-Windows-x64.zip) | [**Download for macOS (Universal)**](https://github.com/Memory-of-Time/archive-scout/releases/latest/download/Scout-macOS-Universal.zip) | [**Download for Linux (x64)**](https://github.com/Memory-of-Time/archive-scout/releases/latest/download/Scout-Linux-x64.tar.gz) |

[All releases](https://github.com/Memory-of-Time/archive-scout/releases) · [Report an issue](https://github.com/Memory-of-Time/archive-scout/issues)

## What Scout does

- Index Wayback CDX inventories with restartable resume-key and paged traversal, query compatibility, and durable checkpoints.
- Download eligible captures with fixed request pacing, resilient network transports, and separate local keyword scanning.
- **Index and download only (no scanning)** or retry failed acquisition independently of keyword sets.
- Explain blocked acquisition using passive stall diagnostics, with optional (default-off) coordinated host connection recovery.
- Classify text and media, optionally retrieve images and video, follow approved archived redirects, and discover embedded media.
- Search stored files and indexed URLs using keyword sets and an optional resumable Hitlist; review results and export reports.
- Maintain project databases, diagnostics, backups, integrity tools, analysis, and optional AI research tools.

**Performance:** Scout's healthy replay start clock defaults to 0.125 seconds (nominal eight requests/s), but successful saves depend on network and archive availability. No adaptive request-rate escalation is used. A local benchmark cannot guarantee live Wayback throughput.

## Requirements and installation

Use the linked platform builds once the release is published. On Windows, run `Scout.exe` inside the extracted `Scout` folder (or the included `Install Scout.cmd`). On macOS, extract the ZIP and launch `Scout.app`. On Linux, extract the archive and use its `install.sh` script or launch the contained binary directly. Consult `packaging/` for installation details and checksum verification. Unsigned Windows builds may show SmartScreen prompts; do not disable antivirus protection.

For source installations, Python 3.11 or 3.12 and Tk are recommended:

```bash
python -m pip install .
scout-gui
scout --help
```

The legacy `archive-scout` and `archive-scout-gui` entry points remain available to existing automations. Internally, Python imports remain `archive_scout` to preserve project and integration compatibility; that does not change the public **Scout** name.

## First project

1. Choose a project folder, target URL patterns, date range and operation.
2. Choose **Index and download only** to preserve text without scanning, or use the full index/download/scan workflow with a keyword set.
3. Review results and generate reports. Use **Refresh dashboard** to update the summary counters manually.
4. If a service outage interrupts work, pause and resume in the **same project folder** to preserve checkpoints. Acquisition stalls over 30 seconds now surface worker/host-gate diagnostics in Activity without extra network requests.

The existing schema-9 project database format is retained. Schema-13 projects created by historical releases cannot be opened in this codebase; preserve their backups. Imported files are copied into project-owned storage, and moved schema-9 projects can rebase previously saved internal file paths on opening. See [Migration](docs/MIGRATION.md).

## Development

```bash
python -m pip install .
python scripts/verify_release.py
python scripts/run_tests.py --output validation/test-results
python scripts/benchmark_offline.py --cdx-rows 1000 --result-rows 100 --body-bytes 1024
```

The [test matrix](.github/workflows/tests.yml) covers Python 3.11/3.12 on Windows, Linux, Intel macOS and Apple Silicon macOS. [Build and release](.github/workflows/build-and-release.yml) packages platform executables and uploads matching assets to tagged GitHub releases. Published binaries require those hosted jobs to complete successfully; local tests alone are not proof of cross-platform builds.

Source overview: [`archive_scout/`](archive_scout/) application code; [`tests/`](tests/) regression tests; [`scripts/`](scripts/) benchmarking/build automation; [`packaging/`](packaging/) OS setup; [`docs/`](docs/) operational and developer guides. See [Architecture](docs/ARCHITECTURE.md), [Operations](docs/OPERATIONS.md), [Indexing and recovery](docs/INDEXING_RECOVERY.md), [Privacy](docs/PRIVACY.md), [Contributing](CONTRIBUTING.md) and [Security](SECURITY.md).

## Upgrade and resume existing projects

Scout **1.2.2** retains the v1.2.0 **schema-9** database and CDX query identity. Existing project folders hold their own indexed inventory, durable download states, scan matches, and CDX checkpoints. **Do not create a new project** when upgrading a large archive. Stop the old Scout process, take a consistent backup of the project (including all capture/media folders), and open the existing `project.json` in the new app. Keep original targets, date range, filters, and collapse options while verifying the upgrade. Changing network pacing does not alter CDX identity.

Before/after inventory verification: `python scripts/check_project_resume.py "PATH_TO_EXISTING_PROJECT" --snapshot before.json` before opening v1.2.2, then `python scripts/check_project_resume.py "PATH_TO_EXISTING_PROJECT" --compare before.json` after upgrading. This is **read-only** and refuses a missing database instead of silently creating one. See [Upgrade v1.2.0 projects](docs/UPGRADE_1_2_0_TO_1_2_1.md) for exact instructions and limitations.

**Keyword scan selection:** The **Keyword sets** page's **Use in next scan** checkbox selects a single set of rules for each new download/scan or rescan. Only that set is used; each operation creates at most one new scan run and one matching scan-report folder. Checking a different set replaces the selection. Simply opening an unchecked set does not change the selection. Uncheck the currently selected set to disable keyword scanning until another set is selected. Older projects with several checked sets use the first checked set, retaining the others for later selection. Prior reports and scan history are intentionally not deleted.

See the [1.2.2 indexing and scanning audit](docs/SCOUT_1_2_2_INDEX_SCAN_AUDIT.md) for the new recovery behavior and verification limits.

The historical product version reset began with Scout 1.2.0. Scout 1.2.2 continues that product line; it does **not** downgrade the database schema or discard prior captured progress.

MIT-licensed. Scout is an independent research tool, not affiliated with the Internet Archive.
