# Archive Scout v1.2.4

**Archive Scout** is an independent, cross-platform desktop and command-line tool for researching and preserving public Wayback Machine captures. It indexes archived URLs, acquires and validates saved bodies, scans selected keyword sets, supports media discovery, and produces reports.

## Download

| Windows x64 | macOS Universal | Linux x64 |
|---|---|---|
| [Download ZIP](https://github.com/Memory-of-Time/archive-scout/releases/latest/download/ArchiveScout-Windows-x64.zip) | [Download ZIP](https://github.com/Memory-of-Time/archive-scout/releases/latest/download/ArchiveScout-macOS-Universal.zip) | [Download tar.gz](https://github.com/Memory-of-Time/archive-scout/releases/latest/download/ArchiveScout-Linux-x64.tar.gz) |

[All releases](https://github.com/Memory-of-Time/archive-scout/releases) · [Report a bug](https://github.com/Memory-of-Time/archive-scout/issues)

Links work once matching build artifacts have been published on GitHub. The [release workflow](.github/workflows/build-and-release.yml) defines and packages those filenames.

### Installation

- **Windows:** Open `ArchiveScout.exe` in the extracted `ArchiveScout` folder, or run `Install Archive Scout.cmd` for current-user desktop/start-menu shortcuts. See [Windows instructions](packaging/windows/README-WINDOWS.txt).
- **macOS:** Extract the ZIP and open `Archive Scout.app`. You can also use the included `ArchiveScoutCLI`. See [macOS instructions](packaging/macos/README.md).
- **Linux:** Extract the archive, run `install.sh`, or run `ArchiveScout/ArchiveScout` directly. See [Linux installer](packaging/linux/install.sh).

For source installation use Python 3.11 or 3.12 and Tk:

```bash
python -m pip install .
archive-scout-gui
archive-scout --help
```

Legacy `scout` and `scout-gui` console entry points remain available. Internally the Python module is still named `archive_scout` to preserve imports and project compatibility.

## Core workflows

- Resume-capable CDX indexing, including durable page/range checkpoints and safeguards for `collapse=urlkey`.
- Validated text acquisition with fixed request pacing, persisted transient retries, streaming retention, and independent bounded local scanning.
- Text/media routing with the configured suffix exclusions; optional media indexing, acquisition, and embedded-media discovery.
- One selected keyword set per new scan, with saved matches, score-zero coverage, reviews, search, and exports.
- Repair, integrity reports, backups, repeatable project merges, and research analysis.
- **Write report now** in Reports: background, read-only committed-state snapshots even during an active operation, including media-only projects, saved in `reports/manual/`. A separate report export does not interrupt downloads or scans.
- Resume preserves the selected direct/external media phase. GUI and CLI explicitly distinguish `incomplete` operations from a successful report export.

An eight-requests-per-second admission target is **not** a guarantee of eight saved captures per second on the live Internet Archive. Checkpoint safety and correct coverage take priority over throughput.

## Projects and compatibility

Archive Scout v1.2.4 preserves the **schema-9** project database, project folder layout, captured content, scan history, and prior indexing/download progress. This release builds on the cleaned v1.2.3 repository with indexing integrity and recovery corrections. Do not downgrade older Python dependencies or overwrite `archive_scout.sqlite3` with an empty database.

Before replacing an installation, stop any active run and back up its project folder. Reopen the same `project.json`; there is no required project reindex. Old `Scout` CLI commands remain supported, and existing GUI preferences stored under the legacy `Scout` settings directory are recognized. See [Project migration](docs/MIGRATION.md), [Project recovery](docs/PROJECT_RECOVERY.md) and the [operations and report snapshot guide](docs/OPERATIONS.md).

## Development and verification

```bash
python -m pip install .
python scripts/verify_release.py
python scripts/run_tests.py --output validation/test-results
python scripts/benchmark_offline.py --cdx-rows 1000 --result-rows 100 --body-bytes 1024
```

The [CI test workflow](.github/workflows/tests.yml) covers Python 3.11/3.12 on Windows, Linux, Intel macOS, and Apple Silicon macOS. The [build workflow](.github/workflows/build-and-release.yml) creates platform-native packages, and verifies their contents. See [Architecture](docs/ARCHITECTURE.md), [Operations](docs/OPERATIONS.md), [Developer guide](docs/DEVELOPMENT.md), [Contributing](CONTRIBUTING.md), and [Security](SECURITY.md).

MIT-licensed. Not affiliated with the Internet Archive.
