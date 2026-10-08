from __future__ import annotations

import importlib.util
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock


class PatchDeliveryTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('archive_scout_patch_verifier', Path(__file__).resolve().parents[2] / 'scripts/verify_patch.py')
        self.helper = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.helper)
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.patch, self.project = self.root / 'patch', self.root / 'project'
        self.patch.mkdir(); self.project.mkdir()
        self.before = "VERSION = '1.0.8'\nSCHEMA_VERSION = 13\n"
        self.after = "VERSION = '1.0.9'\nSCHEMA_VERSION = 13\n"
        for base, text in ((self.project,self.before),(self.patch,self.after)):
            (base / 'archive_scout').mkdir()
            (base / 'archive_scout/constants.py').write_text(text)
        (self.patch / 'new.txt').write_text('new release source')
        entries = []
        for name in ('archive_scout/constants.py','new.txt'):
            target = self.project / name
            entries.append({'path':name,'sha256':self.helper.digest(self.patch/name),
                            'base_sha256':self.helper.digest(target) if target.exists() else None})
        manifest = {'release':'1.0.9','base_release':'1.0.8','deletions':[], 'files':entries}
        (self.patch/'PATCH_MANIFEST.json').write_text(json.dumps(manifest))
        sums = [f"{entry['sha256']}  {entry['path']}" for entry in entries]
        sums.append(self.helper.digest(self.patch/'PATCH_MANIFEST.json')+'  PATCH_MANIFEST.json')
        (self.patch/'SHA256SUMS.txt').write_text('\n'.join(sums)+'\n')
        self.helper.ROOT = self.patch

    def tearDown(self):
        self.temp.cleanup()

    def test_apply_and_idempotence_never_open_or_modify_database(self):
        database = self.project/'archive_scout.sqlite3';database.write_bytes(b'not opened by source patch')
        _, pending = self.helper.preflight(self.project)
        backup = self.helper.apply(self.project,pending)
        self.assertEqual((backup/'archive_scout/constants.py').read_text(),self.before)
        self.assertEqual((self.project/'archive_scout/constants.py').read_text(),self.after)
        self.assertEqual(database.read_bytes(),b'not opened by source patch')
        self.assertEqual(self.helper.preflight(self.project)[1],[])

    def test_local_mismatch_blocks_entire_preflight(self):
        constants=self.project/'archive_scout/constants.py'
        constants.write_text(self.before+'LOCAL_CHANGE = True\n')
        with self.assertRaisesRegex(RuntimeError,'different/local version'):
            self.helper.preflight(self.project)
        self.assertEqual(constants.read_text(),self.before+'LOCAL_CHANGE = True\n')
        self.assertFalse((self.project/'new.txt').exists())

    def test_corrupt_replacement_is_rejected_before_target_mutation(self):
        (self.patch/'new.txt').write_text('corrupted replacement')
        with self.assertRaisesRegex(RuntimeError,'integrity check failed'):
            self.helper.preflight(self.project)
        self.assertEqual((self.project/'archive_scout/constants.py').read_text(),self.before)

    def prior_candidate(self):
        constants = self.project / 'archive_scout/constants.py'
        constants.write_text(self.after + '# prior delivered candidate\n')
        manifest_path = self.patch / 'PATCH_MANIFEST.json'
        manifest = json.loads(manifest_path.read_text())
        manifest['files'][0]['accepted_prior_sha256'] = [self.helper.digest(constants)]
        manifest_path.write_text(json.dumps(manifest))
        checksum_path = self.patch / 'SHA256SUMS.txt'
        sums = checksum_path.read_text().splitlines()
        sums[-1] = self.helper.digest(manifest_path) + '  PATCH_MANIFEST.json'
        checksum_path.write_text('\n'.join(sums) + '\n')
        return constants

    def test_known_prior_candidate_updates_with_backup_and_idempotence(self):
        constants = self.prior_candidate()
        prior = constants.read_bytes()
        _, pending = self.helper.preflight(self.project)
        self.assertIn('archive_scout/constants.py', pending)
        saved = self.helper.apply(self.project, pending)
        self.assertEqual((saved / 'archive_scout/constants.py').read_bytes(), prior)
        self.assertEqual(constants.read_text(), self.after)
        self.assertEqual(self.helper.preflight(self.project)[1], [])

    def test_prior_candidate_local_edit_still_blocks_entire_apply(self):
        constants = self.prior_candidate()
        constants.write_text(constants.read_text() + 'LOCAL_CHANGE = True\n')
        before = constants.read_bytes()
        with self.assertRaisesRegex(RuntimeError, 'different/local version'):
            self.helper.preflight(self.project)
        self.assertEqual(constants.read_bytes(), before)
        self.assertFalse((self.project / 'new.txt').exists())

    def test_copy_failure_rolls_back_existing_and_new_files(self):
        _, pending = self.helper.preflight(self.project)
        original = self.helper.atomic_copy
        calls=0
        def copy(source,target):
            nonlocal calls
            calls+=1
            if calls==3:
                raise OSError('injected failed copy')
            return original(source,target)
        with mock.patch.object(self.helper,'atomic_copy',side_effect=copy):
            with self.assertRaisesRegex(RuntimeError,'original files were restored'):
                self.helper.apply(self.project,pending)
        self.assertEqual((self.project/'archive_scout/constants.py').read_text(),self.before)
        self.assertFalse((self.project/'new.txt').exists())

    def test_path_guards_reject_traversal_directories_and_symlinks(self):
        for name in ('../outside','/absolute','a/../b','a//b','a\\b','C:drive'):
            with self.subTest(name=name),self.assertRaises(RuntimeError):
                self.helper.checked_path(self.project,name)
        (self.project/'occupied').mkdir()
        with self.assertRaises(RuntimeError):
            self.helper.checked_path(self.project,'occupied')
        if os.name != 'nt':
            (self.project/'link').symlink_to(self.root/'outside')
            with self.assertRaisesRegex(RuntimeError,'symlink'):
                self.helper.checked_path(self.project,'link/anything')


if __name__ == '__main__':
    unittest.main()
