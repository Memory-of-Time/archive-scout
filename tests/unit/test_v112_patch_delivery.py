import importlib.util,json,hashlib,tempfile,unittest
from pathlib import Path
from unittest import mock
spec=importlib.util.spec_from_file_location('v112_patch_helper',Path(__file__).resolve().parents[2]/'scripts/verify_patch.py')
helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
def sha(data):return hashlib.sha256(data).hexdigest()

class PatchDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name);self.patch=self.root/'patch';self.target=self.root/'checkout'
        self.patch.mkdir();(self.target/'archive_scout').mkdir(parents=True)
        (self.target/'archive_scout/constants.py').write_text('VERSION="1.1.1"\nSCHEMA_VERSION=13\n')
        (self.target/'archive_scout.sqlite3').write_bytes(b'evidence sentinel')
        (self.target/'module.py').write_bytes(b'old\n');(self.patch/'module.py').write_bytes(b'new\n');(self.target/'retired.py').write_bytes(b'retired\n')
        self.manifest={'release':'1.1.2','base_release':'1.1.1','replacement_count':1,
            'files':[{'path':'module.py','sha256':sha(b'new\n'),'base_sha256':sha(b'old\n'),'accepted_text_sha256':[sha(b'old\n')]}],
            'deletions':[{'path':'retired.py','accepted_sha256':[sha(b'retired\n')],'accepted_text_sha256':[sha(b'retired\n')]}]}
        self.metadata();self.context=mock.patch.object(helper,'ROOT',self.patch);self.context.start()
    def metadata(self):
        p=self.patch/'PATCH_MANIFEST.json';p.write_text(json.dumps(self.manifest))
        (self.patch/'SHA256SUMS.txt').write_text(sha(b'new\n')+'  module.py\n'+sha(p.read_bytes())+'  PATCH_MANIFEST.json\n')
    def tearDown(self):self.context.stop();self.temp.cleanup()
    def test_apply_removes_only_known_retired_source_and_is_idempotent(self):
        manifest,pending=helper.preflight(self.target);backup=helper.apply(self.target,pending,manifest)
        self.assertEqual((self.target/'module.py').read_bytes(),b'new\n');self.assertFalse((self.target/'retired.py').exists())
        self.assertEqual((backup/'retired.py').read_bytes(),b'retired\n');self.assertEqual((self.target/'archive_scout.sqlite3').read_bytes(),b'evidence sentinel')
        self.assertEqual(helper.preflight(self.target)[1],[])
    def test_modified_retired_source_rejects_entire_patch_before_writes(self):
        (self.target/'retired.py').write_bytes(b'user edit')
        with self.assertRaises(RuntimeError):helper.preflight(self.target)
        self.assertEqual((self.target/'module.py').read_bytes(),b'old\n')
    def test_crlf_source_is_accepted_without_weakening_content_checks(self):
        (self.target/'module.py').write_bytes(b'old\r\n');self.assertIn('module.py',helper.preflight(self.target)[1])
        (self.target/'module.py').write_bytes(b'changed\r\n')
        with self.assertRaises(RuntimeError):helper.preflight(self.target)
    def test_corrupt_payload_is_rejected(self):
        (self.patch/'module.py').write_bytes(b'corrupt')
        with self.assertRaises(RuntimeError):helper.preflight(self.target)
    def test_deletion_of_project_database_or_unsafe_path_is_rejected(self):
        for name in ('archive_scout.sqlite3','project.json','captures/a.txt','../escape.py',''):
            with self.subTest(name=name),self.assertRaises(RuntimeError):helper.checked_path(self.target,name)
    def test_write_failure_rolls_back_deletions_and_replacements(self):
        manifest,pending=helper.preflight(self.target);copy=helper.atomic_copy
        def fail(source,target):
            if source==self.patch/'PATCH_MANIFEST.json':raise OSError('injected copy failure')
            return copy(source,target)
        with mock.patch.object(helper,'atomic_copy',side_effect=fail),self.assertRaises(RuntimeError):helper.apply(self.target,pending,manifest)
        self.assertEqual((self.target/'module.py').read_bytes(),b'old\n');self.assertEqual((self.target/'retired.py').read_bytes(),b'retired\n')

if __name__=='__main__':unittest.main()
