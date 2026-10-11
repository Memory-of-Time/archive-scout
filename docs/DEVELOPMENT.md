# Development

Scout 1.2.0 supports Python 3.11/3.12 for published source/CI builds. Some newer Python versions may work but are not part of the official matrix. Tkinter is needed to run the desktop UI.

```bash
python -m pip install .
scout --help
scout-gui
python scripts/verify_release.py
python scripts/run_tests.py --output validation/test-results
python scripts/benchmark_offline.py --cdx-rows 1000 --result-rows 100 --body-bytes 1024
```

Use `scripts/build_windows.ps1`, `scripts/build_macos.sh`, and `scripts/build_linux.sh` only on their respective operating systems. Releases from GitHub Actions include platform-specific executable bundles, and tagged releases upload asset names matched by the top README links.

The internal `archive_scout` package and schema-9 database names intentionally remain stable. Make behavioral changes in the appropriate engine module rather than adding networking or scanning side effects in the GUI. Preserve restartability and avoid extra per-capture database queries.
