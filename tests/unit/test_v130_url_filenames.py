from __future__ import annotations

import base64
import hashlib
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock
from urllib.parse import unquote

from archive_scout.config import ProjectConfig
from archive_scout.downloads.checkpoint import preserve_unverified, write_receipt, prove_file
from archive_scout.downloads.downloader import capture_path
from archive_scout.filenames import url_filename
from archive_scout.media.downloader import fetch_media, media_path


class UrlFilenameTests(unittest.TestCase):
    def test_short_url_is_reversible_including_query_and_percent_escapes(self):
        original='https://example.org/a%20b.php?x=1&y=2#part'
        name=url_filename(original,'20050102030405','.txt',7)
        self.assertEqual(unquote(name.split('__2005')[0]),original)
        self.assertIn('20050102030405_7_',name)
        self.assertTrue(name.endswith('.txt'))

    def test_portable_names_are_bounded_for_unicode_controls_and_long_urls(self):
        originals=['http://example.org/'+('超長😀'*500)+'?q=<>:"/\\|?*\x00\n',
                   'http://example.org/CON.txt','CON.foo','http://example.org/a%2Fb',
                   'http://example.org/'+('a'*20000)]
        for original in originals:
            with self.subTest(original=original[:50]):
                name=url_filename(original,'20050102030405','.jpg',1)
                self.assertTrue(name.isascii())
                self.assertLessEqual(len(name.encode()),140)
                self.assertNotRegex(name,r'[<>:"/\\|?*\x00-\x1f]')
                self.assertFalse(name.endswith((' ','.')))
                self.assertNotRegex(name.split('__')[0],r'%[0-9A-Fa-f]?$')
                self.assertNotIn(name.split('.')[0].upper(),{'CON','NUL','PRN','AUX'})

    def test_case_queries_long_url_tails_ids_and_timestamps_remain_distinct(self):
        common='https://example.org/'+('a'*500)
        inputs=[(common+'X','20050102030405',1),(common+'Y','20050102030405',1),
                (common+'X','20050102030406',1),(common+'X','20050102030405',2),
                ('http://example.org/a?x=1','20050102030405',1),
                ('http://example.org/a?x=2','20050102030405',1),
                ('http://example.org/A?x=1','20050102030405',1)]
        names=[url_filename(u,t,'.txt',i) for u,t,i in inputs]
        self.assertEqual(len(set(n.casefold() for n in names)),len(inputs))

    def test_new_text_and_media_use_url_names_and_remain_stable_after_relocation(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            original='https://example.org/folder/a.jpg?size=2'
            first=capture_path(root,7,'20050102030405',original)
            other=capture_path(root/'moved'/'longer-folder',7,'20050102030405',original)
            self.assertEqual(first.name,other.name)
            self.assertTrue(first.name.startswith('https%3A%2F%2Fexample.org'))
            row={'id':7,'timestamp':'20050102030405','original_url':original,'media_kind':'image','extension':'.jpg'}
            media=media_path(root,row)
            self.assertEqual(media.parent,root/'media/images')
            self.assertTrue(media.name.endswith('.jpg'))
            self.assertIn('20050102030405_7_',media.name)

    def test_legacy_text_body_transfer_and_receipt_keep_original_recovery_path(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);original='http://example.org/a';timestamp='20050102030405'
            digest=hashlib.sha1(original.encode()).hexdigest()
            legacy=root/'captures/2005/01'/f'7_{digest}.txt'
            legacy.parent.mkdir(parents=True)
            for suffix in ('','.transfer','.validated.json'):
                path=legacy.with_name(legacy.name+suffix);path.write_bytes(b'old evidence')
                self.assertEqual(capture_path(root,7,timestamp,original),legacy)
                self.assertEqual(path.read_bytes(),b'old evidence')
                path.unlink()
            self.assertNotEqual(capture_path(root,7,timestamp,original),legacy)

    def test_new_text_receipt_and_quarantine_fit_portable_components(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);original='https://example.org/'+('a'*1000)
            row={'id':7,'timestamp':'20050102030405','original_url':original,'digest':''}
            path=capture_path(root,7,row['timestamp'],original);path.parent.mkdir(parents=True)
            body=b'complete retained evidence';path.write_bytes(body)
            digest=hashlib.sha256(body).hexdigest()
            write_receipt(path,row,{'content_hash':digest,'bytes_saved':len(body)})
            self.assertIsNotNone(prove_file(path,row))
            preserve_unverified(path)
            saved=list(path.parent.glob('*.unverified-*'))
            self.assertEqual(len(saved),1)
            self.assertEqual(saved[0].read_bytes(),body)
            self.assertLessEqual(len(saved[0].name.encode()),160)

    def test_verified_legacy_media_is_reused_without_network_or_renaming(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg=ProjectConfig(Path(temp),targets=['example.org/*'],keywords=[]).normalized()
            body=b'previously validated media'
            row={'id':7,'timestamp':'20050102030405','original_url':'http://example.org/photo.jpg',
                 'media_kind':'image','extension':'.jpg',
                 'digest':base64.b32encode(hashlib.sha1(body).digest()).decode().rstrip('=')}
            legacy=cfg.output_dir/'media/images/photo.jpg';legacy.parent.mkdir(parents=True);legacy.write_bytes(body)
            client=Mock();client.download_to_path.side_effect=AssertionError('legacy evidence requested again')
            result=fetch_media(row,cfg,client)
            self.assertEqual(result['path'],legacy)
            self.assertEqual(legacy.read_bytes(),body)
            client.download_to_path.assert_not_called()
            self.assertFalse(media_path(cfg.output_dir,row).exists())

    def test_recorded_media_path_is_preserved_and_distinct_origins_get_distinct_names(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);legacy=root/'media/images/old-photo.jpg';legacy.parent.mkdir(parents=True);legacy.write_bytes(b'kept')
            row={'id':7,'timestamp':'20050102030405','original_url':'http://example.org/photo.jpg',
                 'media_kind':'image','extension':'.jpg','path':str(legacy.relative_to(root))}
            self.assertEqual(media_path(root,row),legacy)
            row.pop('path')
            other={**row,'id':8,'original_url':'http://different.org/photo.jpg'}
            self.assertNotEqual(media_path(root,row),media_path(root,other))

    def test_unrelated_legacy_media_is_not_claimed_or_overwritten(self):
        with tempfile.TemporaryDirectory() as temp:
            cfg=ProjectConfig(Path(temp),targets=['example.org/*'],keywords=[]).normalized()
            expected=b'correct archived image'
            row={'id':7,'timestamp':'20050102030405','original_url':'http://example.org/photo.jpg',
                 'media_kind':'image','extension':'.jpg',
                 'digest':base64.b32encode(hashlib.sha1(expected).digest()).decode().rstrip('=')}
            legacy=cfg.output_dir/'media/images/photo.jpg';legacy.parent.mkdir(parents=True);legacy.write_bytes(b'unrelated')
            class Client:
                def download_to_path(self,url,path,max_bytes):
                    path.write_bytes(expected)
                    return {'bytes':len(expected),'content_hash':hashlib.sha256(expected).hexdigest(),
                            'headers':{'content-type':'image/jpeg'},'preview':expected,'status':200,'final_url':url}
            result=fetch_media(row,cfg,Client())
            self.assertEqual(result['path'],media_path(cfg.output_dir,row))
            self.assertEqual(Path(result['path']).read_bytes(),expected)
            self.assertEqual(legacy.read_bytes(),b'unrelated')

    def test_legacy_collision_variant_is_reused_with_digest_proof(self):
        from archive_scout.media.downloader import _disambiguate_media_path
        with tempfile.TemporaryDirectory() as temp:
            cfg=ProjectConfig(Path(temp),targets=['example.org/*'],keywords=[]).normalized();body=b'old collision capture'
            row={'id':7,'timestamp':'20050102030405','original_url':'http://example.org/photo.jpg',
                 'media_kind':'image','extension':'.jpg',
                 'digest':base64.b32encode(hashlib.sha1(body).digest()).decode().rstrip('=')}
            legacy=_disambiguate_media_path(cfg.output_dir/'media/images/photo.jpg',row)
            legacy.parent.mkdir(parents=True);legacy.write_bytes(body)
            client=Mock();client.download_to_path.side_effect=AssertionError('old variant redownloaded')
            self.assertEqual(fetch_media(row,cfg,client)['path'],legacy)
            client.download_to_path.assert_not_called()

    def test_new_text_publication_keeps_exact_bytes_and_url_in_receipt(self):
        from archive_scout.downloads.downloader import fetch_parse_scan
        from archive_scout.downloads.checkpoint import read_receipt
        with tempfile.TemporaryDirectory() as temp:
            cfg=ProjectConfig(Path(temp),targets=['example.org/*'],keywords=[]).normalized()
            row={'id':7,'timestamp':'20050102030405','original_url':'http://example.org/page?x=1',
                 'mimetype':'text/html','digest':''}
            body=b'<html><body>retained source evidence</body></html>'
            class Client:
                def get(self,url,max_bytes):
                    return {'data':body,'headers':{'content-type':'text/html'},'status':200,'final_url':url}
            result=fetch_parse_scan(row,cfg,[],Client(),scan_now=False)
            path=Path(result['path'])
            self.assertTrue(path.name.startswith('http%3A%2F%2Fexample.org%2Fpage%3Fx=1'))
            self.assertEqual(path.read_bytes(),body)
            self.assertEqual(read_receipt(path,row)['original_url'],row['original_url'])


if __name__=='__main__':unittest.main()
