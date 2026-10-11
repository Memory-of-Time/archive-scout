# Scout 1.2.0

**Scout** is a cross-platform desktop and command-line workspace for finding, preserving, downloading, searching, and reviewing public Wayback Machine captures.

### Download Scout

| Windows | macOS | Linux |
|---|---|---|
| [**Download for Windows (x64)**](https://github.com/Memory-of-Time/archive-scout-testing/releases/latest/download/Scout-Windows-x64.zip) | [**Download for macOS (Universal)**](https://github.com/Memory-of-Time/archive-scout-testing/releases/latest/download/Scout-macOS-Universal.zip) | [**Download for Linux (x64)**](https://github.com/Memory-of-Time/archive-scout-testing/releases/latest/download/Scout-Linux-x64.tar.gz) |

[All releases](https://github.com/Memory-of-Time/archive-scout-testing/releases) · [Report an issue](https://github.com/Memory-of-Time/archive-scout-testing/issues)

> These download links resolve once the first GitHub release with matching assets is published. They currently target the verified development repository. If Scout is moved to a new public repository, update the three links and the adjacent links before publishing. The release workflow automatically creates these exact filenames.

## What Scout does

- Index Wayback CDX inventories with restartable resume-key and paged traversal, query compatibility, and durable checkpoints.
- Download eligible captures with fixed request pacing, resilient network transports, and separate local keyword scanning.
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
2. Index captures, then download and search them with a selected keyword set.
3. Review results and generate reports. Use **Refresh dashboard** to update the summary counters manually.
4. If a service outage interrupts work, pause and resume in the **same project folder** to preserve checkpoints.

The existing schema-9 project database format is retained. Schema-13 projects created by historical releases cannot be opened in this codebase; preserve their backups. See [Migration](docs/MIGRATION.md).

## Development

```bash
python -m pip install .
python scripts/verify_release.py
python scripts/run_tests.py --output validation/test-results
python scripts/benchmark_offline.py --cdx-rows 1000 --result-rows 100 --body-bytes 1024
```

The [test matrix](.github/workflows/tests.yml) covers Python 3.11/3.12 on Windows, Linux, Intel macOS and Apple Silicon macOS. [Build and release](.github/workflows/build-and-release.yml) packages platform executables and uploads matching assets to tagged GitHub releases. Published binaries require those hosted jobs to complete successfully; local tests alone are not proof of cross-platform builds.

Source overview: [`archive_scout/`](archive_scout/) application code; [`tests/`](tests/) regression tests; [`scripts/`](scripts/) benchmarking/build automation; [`packaging/`](packaging/) OS setup; [`docs/`](docs/) operational and developer guides. See [Architecture](docs/ARCHITECTURE.md), [Operations](docs/OPERATIONS.md), [Indexing and recovery](docs/INDEXING_RECOVERY.md), [Privacy](docs/PRIVACY.md), [Contributing](CONTRIBUTING.md) and [Security](SECURITY.md).

## Release identity

Scout **1.2.0** is the new public product version built from the validated Archive Scout 1.2.4 development line. The reset is deliberate; it does **not** downgrade project databases or reset existing progress. Existing installations may need a reinstall because package version numbers have been reset.

MIT-licensed. Scout is an independent research tool, not affiliated with the Internet Archive.
