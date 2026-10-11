"""Cross-platform canonical test runner with machine-readable test evidence."""
from __future__ import annotations
import argparse
import datetime
import json
import platform
import sys
import unittest
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("validation/test-results"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    args.output.mkdir(parents=True, exist_ok=True)
    suite = unittest.defaultTestLoader.discover(str(root / "tests"), pattern="test_*.py", top_level_dir=str(root / "tests"))
    with (args.output / "tests.log").open("w", encoding="utf-8") as handle:
        result = unittest.TextTestRunner(stream=handle, verbosity=2).run(suite)
    payload = {
        "version": "1.2.0",
        "generated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "platform": platform.platform(), "python": sys.version,
        "tests_run": result.testsRun,
        "failures": len(result.failures), "errors": len(result.errors),
        "skipped": len(result.skipped),
        "successful": result.wasSuccessful(),
        "test_environment": "Local fixture and mock traffic; live Wayback throughput not tested",
    }
    (args.output / "summary.json").write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(payload, indent=2))
    if not result.wasSuccessful():
        print("Tests failed; consult " + str(args.output / "tests.log"), file=sys.stderr)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
