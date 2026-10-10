from __future__ import annotations
import importlib.util
from pathlib import Path
import shutil
import tempfile
import unittest

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

class WorkflowVerificationTests(unittest.TestCase):
    def setUp(self):
        source = Path(__file__).resolve().parents[2]
        self.verifier = load('ci_release_verifier', source / 'scripts/verify_release.py')
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.verifier.ROOT = self.root
        for name in ('pyproject.toml', 'packaging/windows/version_info.txt', '.github/workflows/tests.yml', '.github/workflows/build-and-release.yml'):
            target = self.root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source / name, target)
    def test_canonical_workflows_need_no_mirror(self):
        self.assertFalse((self.root / 'github').exists())
        self.assertEqual(self.verifier.verify(source_only=True)['status'], 'passed')
    def test_lf_crlf_bom_and_missing_final_newline_are_equivalent(self):
        for name in ('tests.yml', 'build-and-release.yml'):
            path = self.root / '.github/workflows' / name
            original = path.read_bytes()
            for data in (original.replace(b'\n', b'\r\n'), b'\xef\xbb\xbf' + original, original.rstrip(b'\n')):
                with self.subTest(name=name):
                    path.write_bytes(data)
                    self.assertEqual(self.verifier.verify(source_only=True)['status'], 'passed')
            path.write_bytes(original)
    def test_duplicate_mirror_is_rejected(self):
        mirror = self.root / 'github/workflows/tests.yml'
        mirror.parent.mkdir(parents=True)
        shutil.copy2(self.root / '.github/workflows/tests.yml', mirror)
        with self.assertRaisesRegex(RuntimeError, 'Duplicate workflow mirror'):
            self.verifier.verify(source_only=True)
    def test_missing_canonical_workflow_is_rejected(self):
        (self.root / '.github/workflows/tests.yml').unlink()
        with self.assertRaisesRegex(RuntimeError, 'Missing canonical workflow'):
            self.verifier.verify(source_only=True)
    def test_misplaced_root_workflow_is_rejected(self):
        (self.root / 'tests.yml').write_text('name: misplaced')
        with self.assertRaisesRegex(RuntimeError, 'Misplaced root workflow'):
            self.verifier.verify(source_only=True)
    def test_stale_workflow_release_assertion_is_rejected(self):
        path = self.root / '.github/workflows/tests.yml'
        path.write_bytes(path.read_bytes().replace(b"== '1.1.3'", b"== '1.1.1'"))
        with self.assertRaisesRegex(RuntimeError, 'workflow package version'):
            self.verifier.verify(source_only=True)
    def test_release_identity_checks_stay_enforced(self):
        path = self.root / 'pyproject.toml'
        path.write_bytes(path.read_bytes().replace(b'1.1.3', b'1.0.9'))
        with self.assertRaisesRegex(RuntimeError, 'runtime VERSION disagree'):
            self.verifier.verify(source_only=True)

class RepositoryInventoryTests(unittest.TestCase):
    def setUp(self):
        source = Path(__file__).resolve().parents[2]
        self.verifier = load('repository_verifier', source / 'scripts/verify_repository.py')
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
    def test_retired_test_is_reported_before_discovery(self):
        path = self.root / 'tests/unit/test_v111_cooldowns.py'
        path.parent.mkdir(parents=True)
        path.write_text('obsolete')
        with self.assertRaisesRegex(RuntimeError, 'test_v111_cooldowns.py'):
            self.verifier.verify(self.root)
    def test_patch_machinery_and_duplicate_configuration_are_reported(self):
        for name in ('CLEANUP_MANIFEST.json', 'scripts/verify_patch.py', 'github/workflows/tests.yml'):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('obsolete')
        with self.assertRaisesRegex(RuntimeError, 'Obsolete files remain'):
            self.verifier.verify(self.root)
    def test_retained_tests_and_generated_validation_are_allowed(self):
        for name in ('tests/unit/test_v112_fixed_engine.py', 'validation/test-results/summary.json'):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('retained')
        self.assertEqual(self.verifier.verify(self.root)['status'], 'passed')

if __name__ == '__main__':
    unittest.main()
