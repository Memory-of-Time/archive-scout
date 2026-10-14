from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from archive_scout.config import NetworkConfig, ProjectConfig, load_project_config, save_project_config
from archive_scout.utils import normalize_cdx_date, normalize_target, parse_cdx_parameter_lines


class ConfigTests(unittest.TestCase):
    def test_target_normalization(self):
        self.assertEqual(normalize_target("https://example.com/forum"), "example.com/forum*")
        self.assertEqual(normalize_target("example.com/*"), "example.com/*")

    def test_date_normalization(self):
        self.assertEqual(normalize_cdx_date("2006", False), "20060101000000")
        self.assertEqual(normalize_cdx_date("200602", True), "20060228235959")
        self.assertEqual(normalize_cdx_date("20060911", True), "20060911235959")
        self.assertEqual(normalize_cdx_date("09/01/2008", False), "20080901000000")
        self.assertEqual(normalize_cdx_date("12/31/2009", True), "20091231235959")
        self.assertEqual(normalize_cdx_date("2008-09-01", False), "20080901000000")

    def test_reserved_cdx_parameter_rejected(self):
        with self.assertRaises(ValueError):
            parse_cdx_parameter_lines(["url=example.com"])

    def test_project_round_trip(self):
        with tempfile.TemporaryDirectory() as temp:
            config = ProjectConfig(
                output_dir=Path(temp),
                targets=["example.com"],
                keywords=["alpha", "beta"],
                from_date="2005",
                to_date="2007",
                network=NetworkConfig(
                    backend="auto",
                    endpoint_mode="auto",
                    index_strategy="paged",
                    page_blocks=3,
                    persistent_retries=False,
                    retry_base_seconds=7,
                    retry_max_seconds=90,
                    failure_pause_threshold=17,
                ),
            ).normalized()
            path = save_project_config(config)
            loaded = load_project_config(path)
            self.assertEqual(loaded.targets, ["example.com/*"])
            self.assertEqual(loaded.keywords, ["alpha", "beta"])
            self.assertEqual(loaded.from_date, "20050101000000")
            self.assertEqual(loaded.to_date, "20071231235959")
            self.assertEqual(loaded.network.index_strategy, "paged")
            self.assertEqual(loaded.network.page_blocks, 3)
            self.assertFalse(loaded.network.persistent_retries)
            self.assertEqual(loaded.network.retry_base_seconds, 7)
            self.assertEqual(loaded.network.retry_max_seconds, 90)
            self.assertEqual(loaded.network.failure_pause_threshold, 17)


if __name__ == "__main__":
    unittest.main()
