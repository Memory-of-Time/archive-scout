"""Offline end-to-end CLI/spawn smoke test, including a frozen executable.

No Archive Scout module is imported by this driver: every operation runs through
the specified executable (or run_cli.py). This detects frozen worker/import bugs.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--executable', type=Path)
    parser.add_argument('--count', type=int, default=300)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--encoding-stress', action='store_true', help='Start the CLI with an ASCII stdout default; structured output must still be UTF-8.')
    args = parser.parse_args()
    command = [str(args.executable.resolve())] if args.executable else [sys.executable, str(Path(__file__).resolve().parents[1] / 'run_cli.py')]
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix='archive-scout-package-smoke-') as folder:
        root = Path(folder)
        project, inputs = root / 'project' / 'project.json', root / 'inputs'
        inputs.mkdir()
        expected = set()
        for index in range(args.count):
            data = f'needle archive complete record {index} café 東京\n'.encode()
            path = inputs / f'{index}.txt'
            path.write_bytes(data)
            os.utime(path, (1104537600, 1104537600))
            expected.add(hashlib.sha256(data).hexdigest())
        def run(*arguments):
            environment = dict(os.environ)
            if args.encoding_stress:
                environment['PYTHONIOENCODING'] = 'ascii'
            result = subprocess.run([*command, *map(str, arguments)], text=True, encoding='utf-8',
                                    capture_output=True, timeout=120, env=environment)
            if result.returncode:
                raise RuntimeError(f'CLI failed ({result.returncode}): {result.stderr[-4000:]}')
            return [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
        run('init', project, '--target', 'example.com/*', '--keyword', 'needle', '--no-auto-research', '--format', 'json')
        payload = json.loads(project.read_text(encoding='utf-8'))
        payload.update(import_source=str(inputs), scan_workers=2, scan_backend='process', dashboard_eta_enabled=True)
        project.write_text(json.dumps(payload), encoding='utf-8')
        run('run', project, '--mode', 'import_folder', '--format', 'jsonl')
        events = run('run', project, '--mode', 'rescan', '--format', 'jsonl')
        # CLI JSONL must stay parseable, including child-process startup.
        progress = [record.get('event') or {} for record in events if record.get('type') == 'progress']
        assert any((event.get('detail') or {}).get('scan_backend') == 'process' for event in progress), 'No process scan telemetry'
        run('status', project, '--format', 'json')
        run('results', project, '--limit', args.count, '--format', 'json')
        database = sqlite3.connect(project.parent / 'archive_scout.sqlite3')
        try:
            assert database.execute('SELECT COUNT(*) FROM documents').fetchone()[0] == args.count
            assert database.execute('SELECT COUNT(*) FROM document_matches WHERE score>0').fetchone()[0] == args.count
            actual = {hashlib.sha256(Path(row[0]).read_bytes()).hexdigest() for row in database.execute('SELECT path FROM documents')}
            assert actual == expected, 'Saved source differs from the complete fixture'
            assert database.execute('PRAGMA quick_check').fetchall() == [('ok',)]
            database.execute("INSERT INTO documents_fts(documents_fts) VALUES('integrity-check')")
            database.rollback()
        finally:
            database.close()
        output = {'status': 'passed', 'files': args.count, 'elapsed_seconds': time.monotonic() - started,
                  'executable': str(args.executable) if args.executable else 'source run_cli.py',
                  'encoding_stress': args.encoding_stress,
                  'checks': ['clean JSONL', 'spawn scanner', 'complete source bytes', 'all matches', 'SQLite reopen', 'FTS integrity']}
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(output, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(output, indent=2))


if __name__ == '__main__':
    main()
