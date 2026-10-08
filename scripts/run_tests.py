"""Run the offline test suite and retain a log plus machine-readable evidence."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


class Tee:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, value):
        for stream in self.streams:
            stream.write(value)

    def flush(self):
        for stream in self.streams:
            stream.flush()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / 'validation/test-results')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    suite = unittest.defaultTestLoader.discover(str(ROOT / 'tests'), pattern='test_*.py')
    with (args.output / 'tests.log').open('w', encoding='utf-8') as log:
        result = unittest.TextTestRunner(stream=Tee(sys.stderr, log), verbosity=2).run(suite)
    from archive_scout.constants import VERSION, SCHEMA_VERSION
    report = {
        'release': VERSION,
        'schema': SCHEMA_VERSION,
        'timestamp_utc': datetime.now(timezone.utc).isoformat(),
        'python': sys.version,
        'platform': sys.platform,
        'tests_run': result.testsRun,
        'passed': result.testsRun - len(result.failures) - len(result.errors) - len(result.skipped) - len(result.expectedFailures) - len(result.unexpectedSuccesses),
        'failures': len(result.failures),
        'errors': len(result.errors),
        'skipped': len(result.skipped),
        'expected_failures': len(result.expectedFailures),
        'unexpected_successes': len(result.unexpectedSuccesses),
        'elapsed_seconds': round(time.perf_counter() - started, 3),
        'successful': result.wasSuccessful(),
        'network_scope': 'Mocked service responses and local fixtures; no live Wayback throughput claim.',
    }
    (args.output / 'summary.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report, indent=2))
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    raise SystemExit(main())
