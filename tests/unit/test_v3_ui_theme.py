from __future__ import annotations

import tkinter as tk
import unittest

from archive_scout.ui.theme import apply_theme


class V3UIThemeTests(unittest.TestCase):
    def test_light_and_dark_themes_are_available(self) -> None:
        try:
            root = tk.Tk()
        except tk.TclError:
            self.skipTest("display is not available")
        root.withdraw()
        try:
            light, light_colors = apply_theme(root, "Light", 1.0)
            dark, dark_colors = apply_theme(root, "Dark", 1.0)
            self.assertEqual(light, "light")
            self.assertEqual(dark, "dark")
            self.assertNotEqual(light_colors["bg"], dark_colors["bg"])
        finally:
            root.destroy()


if __name__ == "__main__":
    unittest.main()
