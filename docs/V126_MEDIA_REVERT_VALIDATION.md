# v1.2.6 media revert: retain only indexing completeness

The revert restores the media downloader and media reports byte-for-byte from the baseline immediately before the recent media completeness/filter/admission work. The prior download-pass and HTTP 403 changes are part of that baseline and remain intact. The separate direct-media timestamp selection module is removed, along with its superseded regression module and benchmark. Indexing-only regressions are retained in `test_v126_media_revert.py`.

The excessive new cost had three distinct sources: broader index enumeration transferred many non-media records; mandatory verification could traverse the broad inventory again; and the admission add-on issued exact timestamp requests before replaying known direct-media URLs when broad verification failed. The last mechanism consumed request slots and lowered the previous local combined fixture from ordinary replay near 8/s to 3.62 selected-and-saved files/s. That measured difference was a fixture result, not a diagnosis of every live slowdown.

This revert removes the admission mechanism and restores the earlier acquisition/report flow. It retains the first two indexing costs because they belong to the explicitly retained completeness corrections. It does not claim old indexing speed while retaining those corrections.

## Source boundary

Only these application files differ from the pre-media baseline:

| File | Retained reason |
| --- | --- |
| `archive_scout/cdx/client.py` | Opt-in media terminal confirmation, token/prefix checks, unverified terminal protection |
| `archive_scout/cdx/indexer.py` | Shared checkpoint fields for merged verification and endpoint-bound page evidence |
| `archive_scout/media/indexer.py` | Expanded candidate discovery, standalone media coverage, paging validation and reconciliation |

`archive_scout/media/downloader.py` and `archive_scout/media/reports.py` must match the baseline bytes. Every other non-indexing application source must also match. The current media indexer and CDX client/indexer are unchanged from the preceding admission add-on. No new query or download mechanism is introduced by this revert. Public version remains 1.2.6, schema remains 11, and media index revision remains 7.

Known saved rows, capture IDs, bodies, states and index checkpoints are not deleted. Old private timestamp proof/work records remain inactive because all consumers are removed. Once broad indexing completes, the original snapshot policy operates on the committed inventory. If indexing remains incomplete, earliest/latest direct acquisition retains its original coverage gate. The revert does not turn incomplete coverage into complete coverage. Existing embedded-media lookup behavior predates these recent changes and is restored unchanged.

## Application

The unified patch applies over the last Media Download Admission add-on. For folder replacement, the three deletions in `DELETE_FILES.txt` are required as well as the replacements. Leaving the removed admission test in place would leave obsolete imports and expectations. A fresh build checkout avoids stale build outputs. Existing project folders and saved reports are not source files and must not be removed.

## Checks

Validation uses Linux and Python 3.12.14. Inherited proxy variables are removed for fixture network tests. Commands:

```text
python scripts/run_tests.py --output .../validation/full-tests
python scripts/verify_release.py
python -m pip install . --no-deps --no-build-isolation --target .../installed
python -m compileall -q archive_scout
python scripts/benchmark_offline.py --cdx-rows 1000 --result-rows 100 --body-bytes 1024 --output .../offline-benchmark.json
python scripts/benchmark_media_loopback.py --count 120 --output .../media-loopback.json
```

The new revert regression module has eleven tests. It preserves paging fallback/endpoint/ledger/count checks and verifies the restored flow: a valid bulk media_all run acquires original indexed timestamps without exact-URL requests; a fully indexed earliest run adds no exact requests; incomplete direct earliest rows do not trigger the removed fallback; old private proof records remain unchanged and ignored; all-snapshot partial acquisition and index-only behavior retain their prior contracts; restored reports do not create the removed selection export.

The canonical suite ran 747 tests: **743 passed, four display tests skipped, zero failures and zero errors**. Wheel building/installation, installed-source comparison including the removed module, release verification, compile checks and the offline smoke benchmark passed. Source hashes and fresh patch/deletion checks are included in the validation files.

The actual local HTTP replay fixture validated and saved 120/120 files of 64 KiB each at **7.80 saves/s** overall. Early/middle/late rates were 7.44/7.99/7.99 saves/s. Peak RSS samples were 37.4/37.9/38.2 MiB with 15 threads, including the fixture server; CPU samples were 0.25/0.40/0.52 seconds. Every file's SHA-256, size and database outcome were checked. This measures ordinary replay acquisition after completed all-snapshot indexing, not indexing speed or a live earliest-selection audit. It ran alongside other validation, so the timing is a functional smoke measurement, not an isolated comparative benchmark.

Unverified: the current live Phoenix response, native Windows/macOS behavior, long-duration consistency, and millions of newly acquired snapshots. The GUI checks requiring a display are skipped in this environment. A short loopback run cannot establish overnight stability or prove that blocked broad verification will succeed.
