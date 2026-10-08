from __future__ import annotations

import contextlib
import io
import json
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from archive_scout.cli import build_parser, cli_main
from archive_scout.config import ProjectConfig, load_project_config, save_project_config
from archive_scout.downloads import rate_limit as rl
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import finish_operation_run, start_operation_run
from archive_scout.operations import run_project


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def advance(self, seconds):
        self.now += seconds

    def monotonic(self):
        return self.now


class AdaptivePacingTests(unittest.TestCase):
    def setUp(self):
        rl.reset_shared_traffic_state_for_tests()
        self.clock = _Clock()
        self.clock_patch = mock.patch.object(rl.time, "monotonic", self.clock.monotonic)
        self.clock_patch.start()
        self.addCleanup(self.clock_patch.stop)
        self.addCleanup(rl.reset_shared_traffic_state_for_tests)

    def _limiter(self, delay=0.125, *, adaptive=False):
        limiter = rl.SharedFixedRateLimiter(delay, key=rl.WAYBACK_REPLAY_RATE_KEY, adaptive=adaptive)
        self.addCleanup(limiter.close)
        return limiter

    def _start(self, limiter):
        with mock.patch.object(limiter.condition, "wait", side_effect=lambda timeout: self.clock.advance(timeout)):
            limiter.wait(threading.Event())
        return self.clock.now

    def test_default_off_ignores_throttle_and_keeps_eight_starts_per_second(self):
        limiter = self._limiter()
        self.assertFalse(limiter.adaptive)
        self.assertFalse(limiter.note_rate_limit(1))
        limiter.note_healthy_response()
        starts = [self._start(limiter) for _ in range(8001)]
        self.assertAlmostEqual(starts[-1] - starts[0], 1000.0)
        self.assertEqual(limiter.effective_delay, 0.125)
        self.assertEqual(limiter.snapshot()["healthy_starts"], 0)

    def test_disabled_client_ignores_adaptive_state_of_active_opted_in_client(self):
        experimental = self._limiter(adaptive=True)
        fixed = self._limiter()
        self.assertTrue(experimental.note_rate_limit(1))
        self.assertEqual(experimental.effective_delay, 0.25)
        self.assertEqual(fixed.effective_delay, 0.125)
        first = self._start(experimental)
        second = self._start(fixed)
        self.assertAlmostEqual(second - first, 0.125)
        self.assertFalse(fixed.note_rate_limit(2))
        fixed.note_healthy_response()
        self.assertEqual(experimental.snapshot()["last_incident_id"], 1)
        self.assertEqual(experimental.snapshot()["healthy_starts"], 0)
        self.assertAlmostEqual(self._start(experimental) - first, 0.25)

    def test_fixed_global_floor_survives_mixed_client_settings(self):
        slower = self._limiter(0.5)
        experimental = self._limiter(adaptive=True)
        experimental.note_rate_limit(1)
        self.assertEqual(slower.effective_delay, 0.5)
        self.assertEqual(experimental.effective_delay, 1.0)
        first = self._start(experimental)
        self.assertAlmostEqual(self._start(slower) - first, 0.5)

    def test_operation_turnover_clears_optional_debt_and_retains_fixed_floor(self):
        experimental = self._limiter(adaptive=True)
        experimental.note_rate_limit(1)
        first = self._start(experimental)
        experimental.close()
        fixed = self._limiter()
        self.assertEqual(fixed.effective_delay, 0.125)
        self.assertAlmostEqual(self._start(fixed) - first, 0.125)
        fixed.close()
        opted_in_later = self._limiter(adaptive=True)
        self.assertEqual(opted_in_later.effective_delay, 0.125)
        self.assertEqual(opted_in_later.snapshot()["last_incident_id"], 0)

    def test_enabled_coalesces_incident_and_recovers_after_sustained_success(self):
        limiter = self._limiter(adaptive=True)
        self.assertTrue(limiter.note_rate_limit(9))
        for _ in range(10):
            self.clock.advance(3)
            self.assertFalse(limiter.note_rate_limit(9))
        self.assertEqual(limiter.effective_delay, 0.25)
        for _ in range(7):
            self.clock.advance(1)
            limiter.note_healthy_response()
        self.assertEqual(limiter.effective_delay, 0.25)
        self.clock.advance(1)
        limiter.note_healthy_response()
        self.assertEqual(limiter.effective_delay, 0.125)

    def test_disabled_pacing_preserves_server_requested_host_wait(self):
        limiter = self._limiter()
        gate = rl.SharedHostGate(base_pause=1, max_pause=10)
        gate.signal_rate_limit(retry_after=180, reason="HTTP 429")
        limiter.note_rate_limit(gate.incident_id)
        with mock.patch.object(gate.condition, "wait", side_effect=lambda timeout: self.clock.advance(timeout)):
            permit = gate.acquire_request(threading.Event())
        self.assertEqual(self.clock.now, 1180.0)
        self.assertTrue(permit.probe)
        self.assertEqual(limiter.effective_delay, 0.125)
        gate.finish_request(permit, recovered=True)
        self.assertFalse(gate.snapshot()["probe_required"])


class AdaptiveConfigurationTests(unittest.TestCase):
    def test_new_and_legacy_projects_default_off_with_fixed_floors(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = ProjectConfig(root, ["example.com/*"], [], cdx_delay=0, download_delay=0).normalized()
            self.assertFalse(config.adaptive_rate_limiting)
            self.assertEqual((config.cdx_delay, config.download_delay), (2.5, 0.125))
            path = root / "project.json"
            path.write_text(json.dumps({"version": "1.1.0", "targets": ["example.com/*"]}), encoding="utf-8")
            self.assertFalse(load_project_config(path).adaptive_rate_limiting)

    def test_setting_roundtrips_and_reaches_target_runtime_config(self):
        with tempfile.TemporaryDirectory() as directory:
            config = ProjectConfig(Path(directory), ["example.com/*"], [], adaptive_rate_limiting=True)
            loaded = load_project_config(save_project_config(config))
            self.assertTrue(loaded.adaptive_rate_limiting)
            self.assertTrue(loaded.for_target("example.com/*").adaptive_rate_limiting)

    def test_cli_init_persists_explicit_opt_in_and_defaults_off(self):
        with tempfile.TemporaryDirectory() as directory:
            for enabled in (False, True):
                path = Path(directory) / ("enabled.json" if enabled else "default.json")
                argv = ["init", str(path), "--format", "json"]
                if enabled:
                    argv.append("--adaptive-rate-limiting")
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(cli_main(argv), 0)
                self.assertEqual(load_project_config(path).adaptive_rate_limiting, enabled)

    def test_cli_run_and_resume_allow_enabling_and_disabling(self):
        with tempfile.TemporaryDirectory() as directory:
            path = save_project_config(ProjectConfig(Path(directory), ["example.com/*"], [], adaptive_rate_limiting=True))
            for mode in ("download_only", "resume"):
                for enabled in (False, True):
                    flag = "--adaptive-rate-limiting" if enabled else "--no-adaptive-rate-limiting"
                    with mock.patch("archive_scout.cli.run_project", return_value={}) as run, contextlib.redirect_stdout(io.StringIO()):
                        self.assertEqual(cli_main(["run", str(path), "--mode", mode, flag, "--format", "json"]), 0)
                    self.assertEqual(run.call_args.args[0].adaptive_rate_limiting, enabled)

    def test_cli_help_marks_option_as_experimental_and_still_in_testing(self):
        for command in ("run", "init"):
            with contextlib.redirect_stdout(io.StringIO()) as output:
                with self.assertRaises(SystemExit):
                    build_parser().parse_args([command, "--help"])
            help_text = " ".join(output.getvalue().casefold().split())
            self.assertIn("experimental", help_text)
            self.assertIn("still in testing", help_text)

    def test_resume_restores_saved_scope_and_retention_but_uses_current_adaptive_choice(self):
        for enabled in (False, True):
            with self.subTest(enabled=enabled), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                saved = ProjectConfig(root, ["saved.example/*"], [], text_retention="discard_after_scan",
                                      adaptive_rate_limiting=not enabled).normalized()
                database = open_database(root)
                operation = start_operation_run(database, "retry_download_errors", "1.0.9",
                                                retention_policy=saved.text_retention,
                                                config_json=json.dumps(saved.to_payload()))
                finish_operation_run(database, operation, "interrupted")
                database.commit()
                database.close()
                current = replace(saved, targets=["changed.example/*"], text_retention="keep",
                                  adaptive_rate_limiting=enabled)
                stats = {"queued": 0, "downloaded": 0, "skipped": 0, "errors": 0}
                with mock.patch("archive_scout.operations.retry_error_downloads", return_value=stats) as retry:
                    run_project(current, "resume", threading.Event())
                resumed = retry.call_args.args[0]
                self.assertEqual(resumed.targets, saved.targets)
                self.assertEqual(resumed.text_retention, "discard_after_scan")
                self.assertEqual(resumed.adaptive_rate_limiting, enabled)


if __name__ == "__main__":
    unittest.main()
