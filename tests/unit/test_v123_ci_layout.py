"""Responsive GUI geometry regression checks for the v1.2.3 add-on.

Run the native Tk assertion in a child process so Tcl cleanup cannot affect
network worker tests in the same Python interpreter. The Linux CI matrix
executes this test under Xvfb; native desktop availability varies on runners.
"""
from __future__ import annotations

import gc
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


class ResponsiveLayoutTests(unittest.TestCase):
    def test_media_editors_have_equal_room_and_sidebar_cannot_expand(self):
        if os.environ.get("ARCHIVE_SCOUT_LAYOUT_TEST_CHILD") != "1":
            env = dict(os.environ, ARCHIVE_SCOUT_LAYOUT_TEST_CHILD="1")
            name = "tests.unit.test_v123_ci_layout.ResponsiveLayoutTests." + self._testMethodName
            result = subprocess.run(
                [sys.executable, "-m", "unittest", name, "-v"],
                cwd=Path(__file__).resolve().parents[2], env=env,
                text=True, capture_output=True, timeout=90,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            if "skipped=1" in result.stderr:
                self.skipTest("Tk desktop display unavailable or too small")
            return

        import tkinter as tk
        from archive_scout.ui.main_window import ArchiveScoutApp

        with tempfile.TemporaryDirectory() as temp:
            with patch("archive_scout.ui.main_window.app_support_dir", return_value=Path(temp)), \
                    patch.object(ArchiveScoutApp, "show_welcome"):
                try:
                    app = ArchiveScoutApp()
                except tk.TclError as exc:
                    if "display" in str(exc).casefold():
                        self.skipTest(str(exc))
                    raise
                try:
                    app.withdraw()
                    # Native Windows/macOS CI may expose a smaller virtual
                    # desktop. Test every viewport that actually fits.
                    sizes = [(940, 680), (1280, 720)]
                    tested = 0
                    for width, height in sizes:
                        if app.winfo_screenwidth() < width or app.winfo_screenheight() < height:
                            continue
                        app.deiconify()
                        app.geometry(f"{width}x{height}+0+0")
                        app.interface_mode_var.set("Advanced")
                        app.refresh_navigation()
                        app.show_page("Media")
                        app.update()
                        tested += 1
                        self.assertLessEqual(app.sidebar_page.winfo_width(), 244)
                        editors = (app.media_targets_text, app.media_include_text, app.media_exclude_text)
                        measured = [widget.winfo_width() for widget in editors]
                        self.assertGreaterEqual(min(measured), 110)
                        self.assertLessEqual(max(measured)-min(measured), 20, measured)
                        rightmost = max(widget.winfo_rootx()+widget.winfo_width() for widget in editors)
                        notebook_right = app.notebook.winfo_rootx()+app.notebook.winfo_width()
                        self.assertLessEqual(rightmost, notebook_right + 8)
                        app.withdraw()
                    if not tested:
                        self.skipTest("Desktop smaller than 940x680")
                finally:
                    for job in app.tk.splitlist(app.tk.call("after", "info")):
                        app.after_cancel(job)
                    app.destroy()
                    del app
                    gc.collect()


if __name__ == "__main__":
    unittest.main()
