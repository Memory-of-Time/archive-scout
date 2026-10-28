# Archive Scout v1.2.6 download-pass patch

Base: the attached `archive-scout-main(20261028-123128).zip`. Public version **1.2.6**, SQLite schema **11**, unchanged.

Stop Scout. Extract this ZIP over the source repository root (containing `pyproject.toml`), replacing listed files. Reinstall with `python -m pip install .` or rebuild through the existing platform workflow. Open the same project normally; no full reindex is required. An existing executable must be rebuilt to use source changes.

This ZIP contains only changed/new files. Alternatively, apply `ArchiveScout-v1.2.6-download-pass.patch` with `git apply` from a clean copy of the baseline. Use one method once. `PATCH_MANIFEST.json` lists original/replacement hashes.

Changes: finish passes with confirmed unavailable captures retained in Errors; fair target timers; automatic recovery through saved cooldowns; corrected idle media eligibility; identical streamed/in-memory 403 classification; unfinished indexing totals/reasons and selected-target report; clearer GUI/CLI messages. Unknown failures, truncated bytes, size/policy refusals and unproven indexing stay unfinished.

Read [the audit](docs/V126_DOWNLOAD_PASS_AUDIT.md) for evidence, tradeoffs, exact checks and unverified work. Results are in `validation/v126-download-pass/`. Explicit healthy pacing and user settings are preserved.
