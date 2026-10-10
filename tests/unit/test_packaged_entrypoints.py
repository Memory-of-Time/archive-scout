from __future__ import annotations

import builtins
from pathlib import Path
import runpy
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[2]


class PackagedEntrypointTests(unittest.TestCase):
    def execute(self, launcher, module_name, *, worker=False, imported=False, failure=None):
        events = []
        module = types.ModuleType(module_name)
        module.main = Mock(side_effect=failure or (lambda: events.append('main')))
        original_import = builtins.__import__

        def observe_import(name, *args, **kwargs):
            if name == module_name:
                events.append('application import')
            return original_import(name, *args, **kwargs)

        def freeze():
            events.append('freeze_support')
            if worker:
                # PyInstaller dispatches the worker/tracker here, then exits.
                raise SystemExit(0)

        with patch.dict(sys.modules, {module_name: module}), patch(
            'multiprocessing.freeze_support', side_effect=freeze
        ), patch('builtins.__import__', side_effect=observe_import):
            if worker:
                with self.assertRaises(SystemExit) as exited:
                    runpy.run_path(str(ROOT / launcher), run_name='__main__')
                self.assertEqual(exited.exception.code, 0)
            else:
                runpy.run_path(str(ROOT / launcher), run_name='launcher_import' if imported else '__main__')
        return events, module.main

    def test_cli_dispatches_before_import_and_argument_parsing(self):
        events, main = self.execute('run_cli.py', 'archive_scout.cli')
        self.assertEqual(events, ['freeze_support', 'application import', 'main'])
        main.assert_called_once_with()

    def test_gui_dispatches_before_import_and_window_creation(self):
        events, main = self.execute('run_app.py', 'archive_scout.app')
        self.assertEqual(events, ['freeze_support', 'application import', 'main'])
        main.assert_called_once_with()

    def test_cli_worker_does_not_parse_application_arguments(self):
        events, main = self.execute('run_cli.py', 'archive_scout.cli', worker=True)
        self.assertEqual(events, ['freeze_support'])
        main.assert_not_called()

    def test_gui_worker_does_not_launch_or_write_startup_error(self):
        with tempfile.TemporaryDirectory() as folder, patch('pathlib.Path.home', return_value=Path(folder)):
            events, main = self.execute('run_app.py', 'archive_scout.app', worker=True)
            self.assertEqual(events, ['freeze_support'])
            main.assert_not_called()
            self.assertEqual(list(Path(folder).iterdir()), [])

    def test_importing_cli_launcher_does_not_start_application(self):
        events, main = self.execute('run_cli.py', 'archive_scout.cli', imported=True)
        self.assertEqual(events, [])
        main.assert_not_called()

    def test_importing_gui_launcher_does_not_start_application(self):
        events, main = self.execute('run_app.py', 'archive_scout.app', imported=True)
        self.assertEqual(events, [])
        main.assert_not_called()

    def test_real_gui_startup_failure_still_records_error_and_propagates(self):
        with tempfile.TemporaryDirectory() as folder, patch('pathlib.Path.home', return_value=Path(folder)), patch.object(sys, 'platform', 'win32'):
            with self.assertRaisesRegex(RuntimeError, 'startup regression fixture'):
                self.execute('run_app.py', 'archive_scout.app', failure=RuntimeError('startup regression fixture'))
            report = (Path(folder) / '.archive-scout' / 'startup-error.log').read_text(encoding='utf-8')
            self.assertIn('Archive Scout startup failure', report)
            self.assertIn('RuntimeError: startup regression fixture', report)


if __name__ == '__main__':
    unittest.main()
