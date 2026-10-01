from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from archive_scout.cdx.parallel import effective_page_workers
from archive_scout.cdx.parameters import build_num_pages_params, build_paged_cdx_params
from archive_scout.config import ProjectConfig, load_project_config


class V102IndexingTests(unittest.TestCase):
    def test_broad_paging_uses_server_selected_large_page_grouping(self):
        config = ProjectConfig(output_dir=Path('.'), targets=['example.com/*'], keywords=[]).normalized()
        count = dict(build_num_pages_params(config, 'example.com/*', '20000101000000', '20001231235959'))
        page = dict(build_paged_cdx_params(config, 'example.com/*', '20000101000000', '20001231235959', 3))
        self.assertEqual(count['showNumPages'], 'true')
        self.assertEqual(page['page'], '3')
        self.assertEqual(count['pageSize'], '9')
        self.assertEqual(page['pageSize'], '9')

    def test_explicit_smaller_page_grouping_remains_supported(self):
        config = ProjectConfig(output_dir=Path('.'), targets=['example.com/*'], keywords=[]).normalized()
        count = dict(build_num_pages_params(config, 'example.com/*', '20000101000000', '20001231235959', page_blocks=5))
        page = dict(build_paged_cdx_params(config, 'example.com/*', '20000101000000', '20001231235959', 0, page_blocks=5))
        self.assertEqual(count['pageSize'], '5')
        self.assertEqual(page['pageSize'], '5')

    def test_server_sized_pages_cap_concurrent_large_bodies(self):
        self.assertEqual(effective_page_workers(10, 0), 3)
        self.assertEqual(effective_page_workers(2, 0), 2)
        self.assertEqual(effective_page_workers(10, 5), 10)

    def test_v101_untouched_nine_block_default_upgrades_but_custom_value_does_not(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            common = {
                'version': '1.0.1', 'output_dir': temp, 'targets': ['example.com/*'], 'keywords': [],
                'from_date': '2001', 'to_date': '2001', 'page_size': 50000, 'cdx_delay': 0.75,
            }
            default_path = root / 'default.json'
            default_path.write_text(json.dumps({**common, 'network': {'page_blocks': 9, 'cdx_workers': 10}}), encoding='utf-8')
            self.assertEqual(load_project_config(default_path).network.page_blocks, 9)
            custom_path = root / 'custom.json'
            custom_path.write_text(json.dumps({**common, 'network': {'page_blocks': 7, 'cdx_workers': 10}}), encoding='utf-8')
            self.assertEqual(load_project_config(custom_path).network.page_blocks, 7)


if __name__ == '__main__':
    unittest.main()
