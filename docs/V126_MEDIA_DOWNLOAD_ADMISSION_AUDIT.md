# Archive Scout v1.2.6: media acquisition admission correction

This add-on builds on the Media Filter 400 add-on. It retains version 1.2.6, schema 11 and private media checkpoint revision 7. It corrects a gap left by the earlier completeness work: strict site-wide verification could prevent every direct earliest/latest capture from being acquired, without an independent way to prove each known URL's timestamp selection.

## Evidence and limits of diagnosis

The supplied Phoenix log reports 100,000 received index rows, 2,113 accepted media captures and 0.27 seconds of database work after 19.5 seconds of network work. Subsequent attempts switch to numbered verification, stop after the no-progress budget and report no media to download. Three operations repeat this pattern. This excerpt does not show any media replay attempt.

Those observations do not demonstrate a replay transport, pool, digest-validation, scanning or database-throughput failure. The excerpt omits the actual paging error. It cannot establish whether the helper returned capture rows, timed out, rejected the query or encountered another failure. The old generic message wrongly described all these conditions as temporary Wayback unavailability.

Source tracing shows `media_all → index_media → index_direct_media → numbered verification`, followed by `download_media(allow_partial_inventory=True)`. Incomplete earliest/latest acquisition admits embedded URL proofs but excludes direct CDX captures without complete broad coverage. The default strategy is earliest. An incomplete direct inventory can therefore contain thousands of pending rows and still produce an empty acquisition queue. This path is reproduced with the production HTTP client and a transport fixture returning ordinary capture rows for page-count requests.

The excerpt does not contain the project JSON, database or build fingerprint, so the actual strategy and helper response cannot be confirmed from it. For all-snapshot acquisition, committed partial rows already bypass this earliest/latest gate; the included all-strategy regression verifies that behavior. The patch does not assert that every possible empty queue has this same cause.

## Changes

### Acquire known media with individual timestamp proof

When direct earliest/latest acquisition is authorized with incomplete coverage, Scout performs exact-URL CDX checks for the best known candidates. Earliest uses a positive limit of one; latest uses the existing negative-limit/fastLatest exact lookup. Queries preserve applicable target dates and explicit CDX filters. For overlapping target scopes with different dates or filters, each applicable scope is checked and the extreme result selected across them.

The exact result must match the queried URL spelling, satisfy local media policy, remain within the queried scope and not contradict a more extreme timestamp already indexed. Empty results, contradictory results, wrong URL spellings and errors remain unfinished. Conservative rejection of a legitimate canonical alias is possible; it does not certify the wrong capture.

Work and timestamp-specific proofs occupy separate private signature namespaces in the existing discovery queue. They do not relabel direct media as embedded, change the active media signature or certify broad coverage. Each proof commits atomically with its selected inventory row and per-URL strategy changes. Saved captures and bodies are retained. Normal completion still requires broad coverage and all requested eligible acquisition outcomes.

The resolver freezes a SQLite work-ID selection and uses keyset batches of at most twice the configured CDX workers, with a maximum of ten workers. Ready results flush while unrelated requests are still pending. Acquisition uses the existing downloader and its validation, durable file publication, recovery and request pacing. Proven captures are not looked up again just because Resume is pressed. Changed settings use another media signature. Failed exact checks retry fairly within the current operation; old attempt totals do not permanently prevent a new Resume attempt. Persistent failure still yields a truthful incomplete result after the bounded recovery budget.

The resolver runs only for incomplete direct earliest/latest acquisition. Index-only operations, all-snapshot acquisition, fully covered inventories and embedded-only inventories do not gain these exact checks. Explicit selected-ID operations do not sweep unrelated saved selection work.

### Verify paging metadata without misclassifying it

Media page counts now use the existing native-JSON helper. This avoids the legacy text adapter's ability to reinterpret a capture timestamp as a scalar page count. Boolean metadata, CDX capture arrays and unsupported count shapes remain invalid.

In automatic endpoint mode, an explicit incompatible helper response or HTTP 400/404/405 permits one compatibility attempt against the native CDX endpoint. Successful fallback binds the count, page requests and layout signature to that endpoint. The endpoint survives checkpoint serialization. An old endpoint's page ledger cannot prove coverage of the fallback endpoint. Old ledger rows remain preserved.

Connection failures, read timeouts, HTTP 504 and shared throttling do not justify this compatibility fallback; they retain ordinary saved-request retry behavior. Unsupported metadata is no longer repeatedly described as an outage. Failed verification never becomes completed empty coverage. Broad merged verification is still required alongside numbered coverage.

### Explain held work

Retry progress includes a bounded underlying reason using the existing URL-redacting error summarizer. An empty eligible acquisition selection reports the active inventory's pending/error count when coverage remains incomplete. Unresolved exact URL selection is exported separately as `media_selection_unfinished.tsv` when media error reports are enabled, preserving the URL, state, attempts and reason. Private proof namespaces do not enter embedded-discovery completion counts.

## Implementation scope

Application files:

- `archive_scout/media/selection.py`: bounded direct-media exact selection and durable timestamp proofs.
- `archive_scout/media/downloader.py`: proof admission, bounded ready-batch acquisition and truthful empty-queue status.
- `archive_scout/media/indexer.py`: native count validation, bounded compatibility fallback and failure explanations.
- `archive_scout/cdx/indexer.py`: optional `page_endpoint` checkpoint field and serialization only; normal text indexing/request functions remain unchanged.
- `archive_scout/media/reports.py`: unresolved exact-selection export.

No downloader pacing, raw replay validation, local scanning, GUI widgets, CLI arguments, database schema or public version changes. Existing media state revision 7 is retained because the broad query predicate is unchanged. Existing resumes do not need another full discovery reset.

## Validation

Commands run from the patched repository root with Python 3.12.14 on Linux; inherited proxy variables removed for fixture network commands:

```text
python scripts/run_tests.py --output .../validation/final-tests
python scripts/verify_release.py
python -m pip install . --no-deps --no-build-isolation --target .../final-installed
python -m compileall -q archive_scout scripts/benchmark_media_selection_loopback.py
python scripts/benchmark_offline.py --cdx-rows 1000 --result-rows 100 --body-bytes 1024 --output .../offline-benchmark.json
python scripts/benchmark_media_loopback.py --count 120 --output .../healthy-media-loopback.json
python scripts/benchmark_media_selection_loopback.py --count 120 --historical-captures 1000000 --output .../selection-with-million-history.json
```

The final canonical suite ran 758 tests: **754 passed, four display tests skipped, zero failures and zero errors**. The new admission module contains 22 tests. Results and fresh-patch checks are recorded in the accompanying validation JSON/logs. The new regressions cover the media_all reproduction and Resume, native helper fallback, helper ledger separation, endpoint checkpoint round-trip, true 504 retry classification, exact retry ordering, delayed unrelated lookup, earliest/latest admission, timestamp-bound proofs, contradictory results, wrong URL responses, target dates and overlapping scopes, index-only behavior, all-snapshot behavior, fully covered inventory behavior, explicit ID isolation, atomic write failure, Stop, saved attempt history and failure export. Prior completeness, HTTP 400, retry-tail and acquisition validation regressions remain in the full suite.

The ordinary replay fixture validates and publishes 120 × 64 KiB files over actual local HTTP: 7.82 saves/s overall; early/middle/late 7.49/8.01/7.98. Peak RSS samples are approximately 37.6/37.8/37.9 MiB, with 15 threads including the fixture server.

The incomplete-inventory fixture validates 120/120 selected earliest captures with a million inactive historical rows retained. It makes exactly 120 exact requests and 120 replay requests. Overall rate is 3.62 saves/s; early/middle/late 3.63/3.62/3.61. Acquisition/selection takes 33.15 seconds and 1.75 CPU seconds; initial historical seeding takes 22.29 seconds separately. Peak RSS samples are approximately 119.5/120.7/120.9 MiB, with 27 threads including the fixture server. Every final file's SHA-256 and recorded timestamp are checked. Historical states remain pending and broad coverage remains incomplete.

The fallback is materially slower because it adds CDX requests under the existing shared request-start pacing contract. These measurements do not indicate CPU saturation or growing per-file costs within the short fixture. They do not prove million-selected-URL behavior or long-term resource stability. Healthy replay remains near the stated 8 validated saves/s target locally; the fallback does not meet that rate when counting selection and acquisition together.

Unverified: live Phoenix behavior and its actual helper response, overnight/days-long service consistency, millions of newly selected media URLs, native Windows/macOS execution and the four skipped display-dependent GUI checks. Fixture benchmarks ran alongside other validation work, so small timing differences are not an isolated performance comparison. Native helper fallback success is proven in a protocol fixture, not on the live endpoint.

## Primary API reference

Internet Archive documents positive/negative result limits, exact fastLatest lookups, inclusive dates, resume keys and numbered pagination limitations:
https://github.com/internetarchive/wayback/blob/master/wayback-cdx-server/README.md

The documentation explains API intent; it does not identify the live service's response in the supplied log. The patch keeps that distinction visible.
