# Archive Scout v1.1.2 patch

Apply to the current v1.1.1 source repository. Close Archive Scout and pause running
operations first. Extract this ZIP into a separate folder; do not just copy it
over the repository: the rollback also removes retired source and test files.

From the extracted patch folder, with Python 3.11 or newer:

```
python scripts/verify_patch.py --project "C:\path\to\archive-scout" --apply
```

The helper verifies every supplied file and every affected original before
changing anything, backs up changed/deleted source files beside the repository,
and rejects unfamiliar local edits. Databases, captures, media and project.json
are never opened or changed. Reapplying the same patch is safe.

Then, from the updated repository:

```
python -m pip install .
python scripts/run_tests.py
python scripts/verify_release.py
python scripts/verify_packaged_scan.py --encoding-stress
```

Commit all changes, including removed files and .github/workflows, then push to
run the Tests workflow. The workflow installs the package, compiles sources,
checks metadata, runs all offline tests, checks spawned encoding scans and uploads
results. The mirrored github/workflows files are included for convenient upload;
GitHub executes the copies under .github/workflows.

The patch retains current projects (schema13) and saved evidence, restores the old
interface and v1.0.2 network engine, and removes adaptive controls. See the included
validation report for precise local results and what has not been verified.
