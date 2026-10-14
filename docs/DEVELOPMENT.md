# Development

Archive Scout v1.2.3 supports Python 3.11 and 3.12 in CI on Windows, macOS Intel/Apple Silicon, and Linux. Newer Python versions may work, but are not included in the official matrix. Tkinter is needed for the desktop GUI.

```bash
python -m pip install .
python scripts/verify_release.py
python scripts/run_tests.py --output validation/test-results
python scripts/benchmark_offline.py --cdx-rows 1000 --result-rows 100 --body-bytes 1024
```

The source lives in `archive_scout/`; tests live in `tests/`. Keep compatibility-critical module names, serialized settings, schema 9, and operation keys stable when changing public branding. `scripts/verify_release.py` checks version alignment, executable names, and the published asset contract. CI runs the canonical test runner and GUI tests under a virtual display on Linux. Release packages are built using the three platform build scripts.

Do not mistake successful local fixture benchmarks for live Wayback throughput. Keep regression tests for preservation, interrupted downloads/scans, media routing, and report correctness.
