# Archive Scout v1.1.2 CI cleanup add-on

Apply this to the repository AFTER the previous v1.1.2 patch. This is a separate,
deletion-only follow-up. The application version and download behavior stay 1.1.2.

The failed revision contains all 71 files retired by the rollback. Seventeen old
test modules add 166 discovered tests. They exercise removed adaptive pacing,
recovery, ETA and interface features. An obsolete clock fixture also leaks its
1970 clock when setup fails; diagnostics ZIP creation then fails, and Windows
reports a secondary open-database cleanup error. The retained source files match
the previously tested patch. No runtime network change is needed for this fix.

Extract this add-on into a SEPARATE folder. Close Archive Scout, then run from
that folder with Python 3.11 or newer:

```powershell
python scripts/cleanup_v112.py --project "C:\path\to\archive-scout" --apply
```

The helper checks the v1.1.2/schema13 identity, retained rollback files, manifest,
and exact retired-file content BEFORE changing anything. It backs up removed
files beside the repository, accepts known LF/CRLF/BOM variants, rejects changed
local files and unsafe paths, and restores deleted files if an operation fails.
Databases, captures, media, user settings and project backups are never touched.
Reapplication is safe. Without --apply it only previews the changes.
Use --check instead of --apply for an early check that exits with an error if
retired files remain. It does not start tests or change any files.

DELETE-REQUIRED-v1.1.2.txt lists all 71 rollback deletions. DELETE-OPTIONAL-v1.1.2.txt
lists 18 additional historical logs, validation evidence and patch/release notes
that can be removed from the source repository AFTER archiving them separately.
They contain no runtime dependencies. They are not deleted by default. To include
those optional files, add --include-optional to the command above. Optional list
entries are pinned to the inspected revision, not patterns matching future logs.
If you retain PATCH_MANIFEST.json/SHA256SUMS.txt, they describe the original patch
delivery, not the later deletion-only cleanup. Keep the add-on and its manifest
outside the repository.

Then run FROM THE UPDATED REPOSITORY:

```powershell
python -m pip install .
python scripts/run_tests.py --output validation/test-results
python scripts/verify_release.py
python scripts/verify_packaged_scan.py --encoding-stress
```

Commit the actual deletions and push them to GitHub. Uploading replacement files
alone cannot delete the old files; the GitHub Tests workflow must use the cleaned
tree. This add-on intentionally does not skip, filter or disable failing tests.

Keep archive_scout/, retained tests, migration tests, assets, packaging, examples,
requirements, pyproject.toml, README, LICENSE, current documentation and scripts.
Keep BOTH .github/workflows/ and github/workflows/: release verification currently
checks that the two copies agree. The other github/ templates should be moved to
.github/ if you want GitHub to activate them. Rename gitignore to .gitignore rather
than deleting it. Local build/, dist/, __pycache__/ and *.egg-info/ are regenerable;
they are not tracked in the inspected revision and are not included in the lists.
Never delete project databases, project.json, captures, media or project backups.

Full local results and remaining validation limits are in evidence/validation.json
inside this add-on. Hosted CI requires pushing the deletion commit and has not
been rerun by this add-on.
