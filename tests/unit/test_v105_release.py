from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.cdx.indexer import PendingWindow, _resolve_strategy, _select_page_batch
from archive_scout.cdx.parallel import effective_page_workers
from archive_scout.cdx.parameters import cdx_paged_endpoints, parse_num_pages, preferred_index_strategy
from archive_scout.config import MediaConfig, NetworkConfig, ProjectConfig, load_project_config
from archive_scout.database.connection import open_database
from archive_scout.media.downloader import download_media, fetch_media, media_path, media_replay_url
from archive_scout.media.indexer import _resolve_media_strategy


class V105ReleaseTests(unittest.TestCase):
    def test_audit3_auto_starts_with_resume_while_timemap_paged_profile_remains_available(self):
        config = ProjectConfig(
            output_dir=Path('.'), targets=['example.com/*'], keywords=[],
            network=NetworkConfig(index_strategy='auto'),
        ).normalized()
        self.assertEqual(preferred_index_strategy(config, 'example.com/*'), 'resume')
        self.assertEqual(config.network.page_blocks, 0)
        self.assertEqual(config.network.cdx_workers, 10)
        self.assertEqual(config.cdx_delay, 2.5)
        self.assertEqual(
            cdx_paged_endpoints(config),
            ('https://web.archive.org/web/timemap/json',),
        )
        self.assertEqual(effective_page_workers(10, 9), 10)


    def test_timemap_page_count_accepts_reference_two_row_shape(self):
        self.assertEqual(parse_num_pages([["pages"], ["37"]]), 37)

    def test_auto_mode_forces_reference_page_size_even_for_legacy_saved_queue(self):
        config = ProjectConfig(
            output_dir=Path('.'), targets=['example.com/*'], keywords=[],
            network=NetworkConfig(index_strategy='auto', page_blocks=50),
        ).normalized()
        window = PendingWindow(
            '20010101000000', '20011231235959',
            strategy='paged', page=11, page_count=100, page_blocks=50,
        )
        _resolve_strategy(window, config, 'example.com/*')
        self.assertEqual(window.page_blocks, 9)
        media_window = PendingWindow(
            '20010101000000', '20011231235959',
            strategy='paged', page=11, page_count=100, page_blocks=50,
        )
        _resolve_media_strategy(media_window, config, 'example.com/*')
        self.assertEqual(media_window.page_blocks, 9)

    def test_parallel_index_pipeline_queues_one_thousand_pages(self):
        window = PendingWindow(
            '20010101000000', '20011231235959',
            strategy='paged', page=0, page_count=1500, page_blocks=9,
        )
        pages, next_page = _select_page_batch(window, 1000)
        self.assertEqual(len(pages), 1000)
        self.assertEqual(pages[0], 0)
        self.assertEqual(pages[-1], 999)
        self.assertEqual(next_page, 1000)

    def test_v104_automatic_index_profile_upgrades_to_page_size_nine(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'project.json'
            path.write_text(json.dumps({
                'version': '1.0.4',
                'output_dir': temp,
                'targets': ['example.com/*'],
                'keywords': [],
                'from_date': '2001',
                'to_date': '2001',
                'page_size': 100000,
                'cdx_delay': 0.75,
                'network': {'page_blocks': 0, 'cdx_workers': 10, 'index_strategy': 'auto'},
            }), encoding='utf-8')
            config = load_project_config(path)
            self.assertEqual(config.network.page_blocks, 9)
            self.assertEqual(config.network.index_strategy, 'auto')

    def test_media_paths_are_flat_and_keep_url_filename_spelling(self):
        root = Path('/project')
        image = {
            'original_url': 'http://example.com/gallery/photo%20one.jpg',
            'media_kind': 'image', 'extension': '.jpg',
        }
        video = {
            'original_url': 'http://example.com/media/clip.mp4',
            'media_kind': 'video', 'extension': '.mp4',
        }
        self.assertEqual(media_path(root, image), root / 'media' / 'images' / 'http%3A%2F%2Fexample.com%2Fgallery%2Fphoto%20one.jpg')
        self.assertEqual(media_path(root, video), root / 'media' / 'videos' / 'http%3A%2F%2Fexample.com%2Fmedia%2Fclip.mp4')
        self.assertEqual(len(media_path(root, image).relative_to(root / 'media').parts), 2)
        self.assertEqual(len(media_path(root, video).relative_to(root / 'media').parts), 2)

    def test_media_replay_matches_reference_downloader_modifiers(self):
        normal = {
            'timestamp': '20010102030405', 'original_url': 'http://example.com/a.jpg', 'extension': '.jpg'
        }
        flash = {
            'timestamp': '20010102030405', 'original_url': 'http://example.com/a.swf', 'extension': '.swf'
        }
        self.assertIn('20010102030405if_/', media_replay_url(normal))
        self.assertIn('20010102030405oe_/', media_replay_url(flash))

    def test_empty_media_run_creates_only_images_and_videos_folders(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(
                output_dir=root, targets=['example.com/*'], keywords=[],
                media=MediaConfig(enabled=True),
            ).normalized()
            database = open_database(root)
            download_media(config, database, threading.Event())
            database.close()
            media = root / 'media'
            self.assertEqual(sorted(item.name for item in media.iterdir()), ['images', 'videos'])
            self.assertTrue((media / 'images').is_dir())
            self.assertTrue((media / 'videos').is_dir())
            self.assertEqual(list((media / 'images').iterdir()), [])
            self.assertEqual(list((media / 'videos').iterdir()), [])

    def test_existing_exact_media_filename_skips_network_like_reference_downloader(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = ProjectConfig(output_dir=root, targets=['example.com/*'], keywords=[]).normalized()
            row = {
                'id': 1,
                'timestamp': '20010102030405',
                'original_url': 'http://example.com/path/photo.jpg',
                'media_kind': 'image',
                'extension': '.jpg',
            }
            # ProjectConfig.normalized() canonicalizes output_dir. On macOS,
            # /var resolves to /private/var; on Windows, an 8.3 temp path such as
            # RUNNER~1 can resolve to its long form. Build the expected path from
            # the normalized project root, exactly as fetch_media() does.
            path = media_path(config.output_dir, row)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"\xff\xd8\xff\xe0JPEG")
            client = unittest.mock.Mock()
            client.download_to_path.side_effect = AssertionError('network must not run')
            result = fetch_media(row, config, client)
            self.assertEqual(result['path'], path)
            self.assertEqual(result['bytes'], 8)
            client.download_to_path.assert_not_called()


if __name__ == '__main__':
    unittest.main()
