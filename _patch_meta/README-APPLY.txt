Archive Scout v1.0.7 - index-only reports add-on

APPLY THIS AFTER THE PREVIOUS v1.0.7 PATCH
This is an incremental add-on, not a complete repository or the earlier patch bundle.
The application version remains 1.0.7. There are three replacement source files
and one new regression-test file. No database migration is required.

Close Archive Scout, extract this ZIP, and copy archive_scout/ and tests/ into the
repository root, replacing the matching files while preserving their paths.
If using GitHub's upload interface, upload the individual replacement files into
their matching repository directories, plus the new file in tests/unit/.
Do not upload the ZIP as a source file. _patch_meta/ contains instructions and
validation only. Alternatively, apply _patch_meta/index-reports.patch using
git apply after the previous v1.0.7 patch has been installed.

WHAT CHANGES
- Successful Index URLs only runs report the files actually written in Activity.
- Pause & save writes a partial URL inventory and summary from saved indexing
  progress. One-shot network deferrals also export a partial inventory. Reports
  respect the user's enabled output files and selected fields.
- Resume restores an index-only operation and its saved configuration without
  requiring keyword sets or starting downloading or scanning.
- The Index only acquisition scope under a full-run mode follows that same
  index-only operation contract.
- Regenerate reports uses the latest text-index operation, even if an older scan
  exists, and preserves the partial label if that index was unfinished.
- Reports has an Indexed URLs only preset that enables all_indexed_urls.txt
  with one original URL per line. It does not deduplicate timestamps or change
  collapse settings; those remain indexing choices.
- Activity explains when no compatible index reports are enabled, rather than
  claiming report files were written. No generated reports remains supported.
- Indexing counters and saved Retry-After deadlines are preserved.

HOW TO GET THE INDEX REPORTS
In Reports, select All files and fields, or enable All indexed URLs, Summary,
Errors and Site-specific issues individually. For a plain URL list, use the new
Indexed URLs only preset. Matched URLs only requires a scan and produces no
index-only inventory. Run Index URLs only. Files are written at completion,
or when you use Pause & save; they are not continuously rewritten during
active indexing or temporary automatic network recovery.

With the default report selection, the project reports/ folder contains:
  all_indexed_urls.txt - currently indexed capture inventory
  summary.txt          - count and completion/partial status; bodies searched is 0
  errors.txt           - unresolved operational errors
  site_issues.txt      - open site-specific archive issues

Existing indexed projects can use Regenerate reports without requesting Wayback
again. An empty index still gets an empty inventory and a zero-capture summary
when those outputs are enabled. Changing presets affects generated report
content, not what is indexed or downloaded.

VALIDATION
451 tests: 450 passed, 1 skipped, 0 failures. The skip requires a GUI display.
Compilation succeeded. The unified patch applies cleanly to the previous
v1.0.7 source. Applying the ZIP files reconstructs the tested source exactly.
The existing cross-platform CI workflows are preserved. Windows and macOS jobs
must still execute in GitHub Actions; they were not run in this Linux workspace.
No indexing, download, pacing, scanning, storage or schema algorithms changed.
