from __future__ import annotations

import importlib.util
from pathlib import Path
import shutil
import tempfile
import unittest


class WorkflowVerificationTests(unittest.TestCase):
    def setUp(self):
        source = Path(__file__).resolve().parents[2]
        spec = importlib.util.spec_from_file_location("ci_release_verifier", source / "scripts/verify_release.py")
        self.verifier = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.verifier)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.verifier.ROOT = self.root
        for name in ("pyproject.toml", "packaging/windows/version_info.txt"):
            target = self.root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source / name, target)
        for name in ("tests.yml", "build-and-release.yml"):
            for directory in (".github/workflows", "github/workflows"):
                target = self.root / directory / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(b"name: Test\non:\n  workflow_dispatch:\njobs:\n  test:\n    runs-on: ubuntu-22.04\n")

    def test_lf_crlf_mixed_endings_and_bom_are_equivalent(self):
        for name in ("tests.yml", "build-and-release.yml"):
            mirror = self.root / "github/workflows" / name
            original = mirror.read_bytes()
            for data in (original.replace(b"\n", b"\r\n"),
                         original.replace(b"\n", b"\r\n", 3),
                         b"\xef\xbb\xbf" + original):
                with self.subTest(name=name, data=data):
                    mirror.write_bytes(data)
                    self.assertEqual(self.verifier.verify(source_only=True)["status"], "passed")
            mirror.write_bytes(original)

    def test_optional_final_newline_is_equivalent(self):
        mirror = self.root / "github/workflows/tests.yml"
        mirror.write_bytes(mirror.read_bytes().rstrip(b"\n"))
        self.assertEqual(self.verifier.verify(source_only=True)["status"], "passed")

    def test_actual_workflow_change_is_rejected(self):
        mirror = self.root / "github/workflows/tests.yml"
        mirror.write_bytes(mirror.read_bytes().replace(b"ubuntu-22.04", b"windows-2022"))
        with self.assertRaisesRegex(RuntimeError, "different workflow copies"):
            self.verifier.verify(source_only=True)

    def test_missing_canonical_or_mirror_is_rejected(self):
        for directory in (".github/workflows", "github/workflows"):
            path = self.root / directory / "tests.yml"
            data = path.read_bytes()
            path.unlink()
            with self.subTest(directory=directory), self.assertRaisesRegex(RuntimeError, "Missing or different"):
                self.verifier.verify(source_only=True)
            path.write_bytes(data)

    def test_misplaced_root_workflow_is_rejected(self):
        (self.root / "tests.yml").write_bytes(b"name: misplaced\n")
        with self.assertRaisesRegex(RuntimeError, "Misplaced root workflow"):
            self.verifier.verify(source_only=True)

    def test_release_identity_checks_stay_enforced(self):
        path = self.root / "pyproject.toml"
        path.write_bytes(path.read_bytes().replace(b'1.1.0', b'1.0.9'))
        with self.assertRaisesRegex(RuntimeError, "runtime VERSION disagree"):
            self.verifier.verify(source_only=True)


if __name__ == "__main__":
    unittest.main()
