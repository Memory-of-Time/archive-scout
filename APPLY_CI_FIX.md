# Archive Scout v1.1.0 CI fix — separate add-on

Apply **after** the previous `ArchiveScout-v1.1.0-patch-files.zip`. Version remains **1.1.0**. This ZIP contains only two changed/new repository code files:

- `scripts/verify_release.py`
- `tests/unit/test_v110_ci_workflows.py`

Copy those files into your existing v1.1.0 repository at exactly those paths, replacing the verifier. Upload both files to GitHub, then rerun **Tests**. No workflow-file edits or application rebuild are needed to apply this source correction. Keep the original v1.1.0 patch and its other files in place. The `CI_FIX_*`, checksum and this guide files document this add-on independently; they do not replace the original patch manifest.

All eight jobs in [the inspected GitHub run](https://github.com/Memory-of-Time/archive-scout/actions/runs/37849543815) passed the test suite, then failed in **Verify release identity and workflow placement** with `Missing or different workflow copies: tests.yml`. Both workflow pairs have equivalent text but different line endings after editing/upload. The corrected verifier normalizes LF/CRLF, optional final newline and UTF-8 BOM while preserving real workflow-content, path, version and installed-metadata checks.

Validation: the exact GitHub failure reproduced before the fix; verification passed afterward. **573 tests passed locally**, zero failures/errors. Compilation, release verification, spawned CLI scans, and both offline benchmark smoke checks passed. Logs and the JSON validation report are included. A hosted run of this corrective add-on has not been triggered here.
