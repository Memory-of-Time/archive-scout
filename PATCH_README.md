# Archive Scout v1.2.5 — validation and text display cadence only

**Base:** `archive-scout-main (1)(4).zip` supplied with this request. Apply the contents of this patch ZIP directly over the first repository, keeping the relative folder structure. This ZIP already contains the scoped portions of the two reference patches, including the portable crash-test correction. **Do not also overlay either full reference patch**, because they contain deliberately excluded changes.

## Included

- **Validated save / recovery:** Capture-bound short-lived validation receipts between validated body publication and the database commit, with full-byte SHA-256 proof. Interrupted publication and staging are recoverable only with valid evidence. Files without reliable proof are preserved separately and scheduled for reacquisition, never silently counted as complete. Scan/owner error transitions and stale error resolution remain consistent. Includes portable crash-injection tests.
- **Presentation-only cadence:** A Dashboard control, **Text acquisition updates**, offers **Every available update**, **0.5**, and **1** (seconds). Default **0.5**. The choice is saved as a GUI appearance preference, not a project/acquisition setting. “Every available update” presents the most recent available counter update at every GUI event-pump opportunity (rendering is still coalesced if producers are faster than Tk can repaint). It does not impose a new worker delay, networking delay, database wait, scan wait, or CLI callback reduction.
- **Correct handling of important GUI notices:** Non-routine site/network/storage/redirect notices do not share the throttled acquisition counter slot. Final acquisition snapshot is flushed before completion, stop, error, deferral, or incomplete-run status. Bounded GUI event buffering is retained.
- Updated three legacy test fixtures to provide authentic CDX digest evidence where required by the stricter validation, plus focused tests of interruption, proof, and all three display modes.

## Deliberately excluded

No retry-query rewrite, extra acquisition timing counters, failure-stream telemetry, new network pacing or timeouts, global worker/memory/batch changes, indexing changes, text/media routing changes, database migration, CLI changes, or version bump. No project is reset or reindexed. This is a replacement-file overlay, **not** a complete repository.

## Validation performed

- `python -m unittest discover -s tests/unit -p 'test_v125_completeness_patch.py' -q`: **21 passed**.
- `python scripts/run_tests.py --output /mnt/data/scout_scope_workspace/test-results-final`: **502 tests, zero failures, zero errors, six skipped** on Linux / Python 3.13.5.
- Python `compileall`: passed for the modified application.
- The crash tests include process exits before publication, after publication/before SQLite commit, and after commit/before receipt cleanup. Alternate/noncanonical directory spellings exercise the cross-platform test normalization correction.

**Not tested:** native macOS/Windows GUI rendering and CI, power loss, millions of captures, days-long runs, or live Internet Archive acquisition speed. Display cadence reduces routine GUI update frequency; a network throughput increase is *not* established.
