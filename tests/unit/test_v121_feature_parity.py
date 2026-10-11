"""v1.2.1 feature restoration without acquisition hot-path regressions."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from archive_scout.cdx.indexer import PendingWindow, _resolve_strategy
from archive_scout.cdx.parameters import preferred_index_strategy
from archive_scout.config import NetworkConfig, ProjectConfig, ReportConfig, save_project_config, load_project_config
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import (
    get_or_create_target, get_or_create_keyword_set, start_scan_run,
    upsert_capture, upsert_document, save_match,
)
from archive_scout.reports.text import generate_reports
from archive_scout.ui.main_window import enforce_active_keyword_set_selection
from archive_scout.utils import hash_text

class FeatureRestorationTests(unittest.TestCase):
    def test_active_keyword_is_exclusive_without_deleting_sets(self):
        sets = [
            {"name": "A", "rules": ["alpha"], "selected": True},
            {"name": "B", "rules": ["beta"], "selected": True},
            {"name": "C", "rules": ["gamma"], "selected": False},
        ]
        enforce_active_keyword_set_selection(sets, 1, True)
        self.assertEqual([v['selected'] for v in sets], [False, True, False])
        self.assertEqual([v['rules'][0] for v in sets], ['alpha', 'beta', 'gamma'])
        enforce_active_keyword_set_selection(sets, 1, False)
        self.assertFalse(any(v['selected'] for v in sets))

    def test_automatic_resume_preserves_existing_paged_work_and_fixed_policy(self):
        config = ProjectConfig(output_dir=Path('.'), targets=['example.com/*'], keywords=['text']).normalized()
        self.assertEqual(config.cdx_delay, 2.5)
        self.assertEqual(config.download_delay, 0.125)
        self.assertEqual(preferred_index_strategy(config, 'example.com/*'), 'resume')
        old = PendingWindow('20010101000000', '20011231235959', strategy='paged', page=740,
                            page_count=1000, page_blocks=50, retry_pages=[7, 128])
        _resolve_strategy(old, config, 'example.com/*')
        self.assertEqual((old.strategy, old.page, old.page_count, old.page_blocks, old.retry_pages),
                         ('paged', 740, 1000, 50, [7, 128]))
        new = PendingWindow('20010101000000', '20011231235959')
        _resolve_strategy(new, config, 'example.com/*')
        self.assertEqual(new.strategy, 'resume')
        self.assertEqual(preferred_index_strategy(ProjectConfig(
            output_dir=Path('.'), targets=['example.com/*'], keywords=['x'],
            network=NetworkConfig(index_strategy='paged')).normalized(), 'example.com/*'), 'paged')

    def test_report_configuration_persists_and_skips_disabled_outputs(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            report = ReportConfig(outputs=['matches_ranked', 'summary'],
                                  fields={'matches_ranked': ['rank','original_url','snippets'],
                                          'summary': ['heading', 'ranked_matches']},
                                  max_matches=1, snippet_limit=1, snippet_chars=7, sort_order='newest')
            config = ProjectConfig(output_dir=root, targets=['example.com/*'], keywords=['test'], report=report).normalized()
            project = save_project_config(config)
            loaded = load_project_config(project)
            self.assertEqual(loaded.report.outputs, ['matches_ranked', 'summary'])
            self.assertEqual(loaded.report.sort_order, 'newest')
            database = open_database(root)
            tid = get_or_create_target(database, 'example.com/*')
            for i in range(2):
                path = root / f'body{i}.txt'
                path.write_text('test body', encoding='utf-8')
                upsert_capture(database, {
                    'original': f'https://example.com/{i}', 'timestamp':f'2001010100000{i}',
                    'mimetype': 'text/html', 'statuscode': '200', 'digest':'', 'length': '9'}, tid, 'sig')
                cap = database.execute('SELECT id FROM captures WHERE original_url=?', (f'https://example.com/{i}',)).fetchone()[0]
                doc = upsert_document(database, cap, path, f'Title {i}', 'test body', [], hash_text('test body'), hash_text('test body'), 9)
                if i == 0:
                    kid = get_or_create_keyword_set(database, 'Current', ['test'])
                    run = start_scan_run(database, kid, 'Current', 1, 'rescan')
                save_match(database, run, doc, {
                    'score':10+i, 'hits':{'test':1}, 'hit_fields':{'test':['body']},
                    'snippets':['test snippet longer than seven chars', 'additional snippet'],
                    'interesting_links':['https://other.example/test'],
                })
            database.commit()
            result = generate_reports(loaded, database, run)
            database.close()
            self.assertEqual(set(result), {'scan_folder', 'matches_ranked', 'summary'})
            text = result['matches_ranked'].read_text()
            self.assertEqual(text.count('RANK:'),1)
            self.assertIn('https://example.com/1',text)
            self.assertIn('SNIPPETS:\n  1. test sn',text)
            self.assertNotIn('SCORE:',text)
            self.assertNotIn('TITLE:',text)
            self.assertFalse((root/'reports'/'all_indexed_urls.txt').exists())
            self.assertFalse((root/'reports'/'interesting_links.txt').exists())
            self.assertEqual(result['summary'].read_text().splitlines()[0],'Scout')

if __name__ == '__main__':
    unittest.main()
