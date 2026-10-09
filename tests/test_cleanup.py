from pathlib import Path
import hashlib
import importlib.util
import json
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('cleanup_helper', Path(__file__).resolve().parents[1] / 'scripts/cleanup_v112.py')
h = importlib.util.module_from_spec(spec)
spec.loader.exec_module(h)

def sha(value):
    return hashlib.sha256(value).hexdigest()

class CleanupTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.patch = self.root / 'patch'
        self.target = self.root / 'checkout'
        self.patch.mkdir()
        (self.target / 'archive_scout').mkdir(parents=True)
        data = b'VERSION="1.1.2"\nSCHEMA_VERSION=13\n'
        (self.target / 'archive_scout/constants.py').write_bytes(data)
        (self.target / 'first.py').write_bytes(b'old first\n')
        (self.target / 'second.py').write_bytes(b'old second\n')
        (self.target / 'historic.log').write_bytes(b'old log\n')
        (self.target / 'project.json').write_bytes(b'settings sentinel')
        (self.target / 'archive_scout.sqlite3').write_bytes(b'evidence sentinel')
        self.manifest = {'release': '1.1.2', 'schema': 13,
            'required': [self.entry('first.py', b'old first\n'), self.entry('second.py', b'old second\n')],
            'optional': [self.entry('historic.log', b'old log\n')],
            'retained': [{'path': 'archive_scout/constants.py', 'sha256': sha(data), 'text_sha256': sha(data)}]}
        self.metadata()
        context = mock.patch.object(h, 'ROOT', self.patch)
        context.start()
        self.addCleanup(context.stop)

    def entry(self, name, data):
        return {'path': name, 'accepted_sha256': [sha(data)], 'accepted_text_sha256': [sha(data)]}

    def metadata(self):
        data = json.dumps(self.manifest).encode()
        (self.patch / 'CLEANUP_MANIFEST.json').write_bytes(data)
        (self.patch / 'CLEANUP_MANIFEST.sha256').write_text(sha(data))

    def test_apply_backup_idempotence_and_project_preservation(self):
        pending = h.preflight(self.target)
        backup = h.apply(self.target, pending)
        self.assertEqual(len(pending), 2)
        self.assertEqual((backup / 'first.py').read_bytes(), b'old first\n')
        self.assertFalse((self.target / 'first.py').exists())
        self.assertTrue((self.target / 'historic.log').exists())
        self.assertEqual((self.target / 'project.json').read_bytes(), b'settings sentinel')
        self.assertEqual((self.target / 'archive_scout.sqlite3').read_bytes(), b'evidence sentinel')
        self.assertEqual(h.preflight(self.target), [])

    def test_optional_cleanup_requires_explicit_flag(self):
        self.assertNotIn('historic.log', h.preflight(self.target))
        self.assertIn('historic.log', h.preflight(self.target, include_optional=True))

    def test_modified_retired_file_blocks_entire_preflight(self):
        (self.target / 'second.py').write_bytes(b'local edits')
        with self.assertRaisesRegex(RuntimeError, 'local changes'):
            h.preflight(self.target)
        self.assertTrue((self.target / 'first.py').exists())

    def test_modified_retained_source_blocks_preflight(self):
        (self.target / 'archive_scout/constants.py').write_bytes(b'VERSION="1.1.2"\nSCHEMA_VERSION=13\n# edit\n')
        with self.assertRaisesRegex(RuntimeError, 'rollback source differs'):
            h.preflight(self.target)

    def test_crlf_and_bom_do_not_accept_meaningful_local_edits(self):
        (self.target / 'first.py').write_bytes(b'\xef\xbb\xbfold first\r\n')
        self.assertIn('first.py', h.preflight(self.target))
        (self.target / 'first.py').write_bytes(b'\xef\xbb\xbflocal changes\r\n')
        with self.assertRaises(RuntimeError):
            h.preflight(self.target)

    def test_corrupt_manifest_blocks_preflight(self):
        (self.patch / 'CLEANUP_MANIFEST.json').write_bytes(b'corrupt')
        with self.assertRaisesRegex(RuntimeError, 'integrity check'):
            h.preflight(self.target)

    def test_duplicate_or_unsafe_paths_are_rejected(self):
        self.manifest['optional'].append(self.manifest['required'][0])
        self.metadata()
        with self.assertRaisesRegex(RuntimeError, 'Duplicate'):
            h.preflight(self.target)
        for name in ('../outside.py', '/absolute.py', 'a\\b.py', '.git/config', 'archive_scout.sqlite3', 'project.json', 'captures/body.txt', 'media/a.jpg', 'backups/source.py'):
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                h.checked_path(self.target, name)

    def test_deletion_failure_restores_all_prior_deletions(self):
        pending = h.preflight(self.target)
        unlink = Path.unlink
        def fail(path, *args, **kwargs):
            if path == self.target / 'second.py':
                raise PermissionError('injected file lock')
            return unlink(path, *args, **kwargs)
        with mock.patch.object(Path, 'unlink', fail), self.assertRaisesRegex(RuntimeError, 'source was restored'):
            h.apply(self.target, pending)
        self.assertEqual((self.target / 'first.py').read_bytes(), b'old first\n')
        self.assertEqual((self.target / 'second.py').read_bytes(), b'old second\n')

    def test_wrong_release_is_rejected(self):
        (self.target / 'archive_scout/constants.py').write_bytes(b'VERSION="1.1.1"\nSCHEMA_VERSION=13\n')
        with self.assertRaisesRegex(RuntimeError, 'original v1.1.2 patch'):
            h.preflight(self.target)

if __name__ == '__main__':
    unittest.main()
