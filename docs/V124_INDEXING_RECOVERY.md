# Archive Scout v1.2.4 — indexing audit implementation and limits

## Authority, compatibility and release contract

This implementation is based on the cleaned v1.2.3 source supplied with the October 16, 2026 indexing audit. The database remains **schema 9**, the project root and Python package stay the same, and previously retained bodies, scan results, and committed CDX cursors are not reset. The updated schema-9 initialization creates two optional supporting tables (`capture_target_membership` and `media_index_pages`) idempotently; the release's schema integer is deliberately unchanged. Back up projects before upgrading. A project from a **newer** schema is refused; never hand-edit `schema_info`.

The work focuses on correctness and long-term liveness. All existing non-text suffix safeguards and text-vs-media classification remain in force. A failed host is never reclassified as an empty index merely to let an operation finish. Periodic successful work preserves its durable progress. **This release cannot force a remote CDX service to answer a permanently failing query.**

## Audit recommendation-by-recommendation status

| Item | Implementation and boundary |
|---|---|
| F1: Target starvation | Implemented a bounded-turn, round-robin target scheduler for text and media. A failing target yields its original semantic query/window/cursor and other targets can make progress. A persisted `next_eligible_at` supports cooldown after process restart. With no productive target, a persistent run remains active and interruptibly retries. |
| F2: Leaked transport probe | Reserved backend recovery probes are released on size-limit/policy failures and pre-request exits. Tested local refusal paths; no claim that every third-party transport exception is modeled. |
| F3: Invalid JSON false empty | Rejected `null`, scalar/string/boolean and malformed CDX response shapes as incomplete; retained legitimate empty-array responses. |
| F4: Lost numbered-page continuation | Page results now carry continuation evidence; a numbered page with unsupported explicit continuation is **rejected**, not silently completed. Following such an undocumented continuation was intentionally not invented. |
| F5: Layout/page-count mismatch | Saved page counts now carry layout identity and are invalidated/revalidated when the endpoint or block layout changes. Existing ledger evidence remains keyed by its original layout. |
| F6: Cross-page `collapse=urlkey` | **Still unverified / not fully resolved.** The compact response does not consistently retain canonical CDX URL keys, so the code cannot prove equivalent membership when a collapsed group spans numbered pages. This release does not discard existing captures or deduplicate by a guessed Python-normalized URL. Prefer resume traversal when exact collapse selection matters. |
| F7: Repeated no-progress endpoint loops | Partly addressed by bounded turns, saved retry eligibility and fair progression of independent targets. A semantics-preserving raw-enumeration/reducer fallback and full same-scope traversal experiment are **not implemented**. |
| F8: Response outside requested scope | Validates timestamp bounds, supported status-200 constraints and conservative target matching before durable writes. Arbitrary Java CDX filter regexes cannot be independently proven by the local validator. |
| F9: Overlapping target membership | An idempotent membership table and triggers retain multiple target-to-capture relationships while keeping one canonical capture/body. Existing captures are associated lazily when touched; preexisting overlapping ownership that was overwritten by an older release cannot be reconstructed without original query evidence. |
| F10: Failure accounting/diagnostics | Direct capture-row change counts exclude membership-trigger changes, and existing structured events identify window/stage. Full per-backend/wire-phase profiling, transport timings and complete request header provenance remain **partial**. |
| F11: Shared CDX memory | Page worker admission now respects a configurable **512 MiB default estimated response-body envelope** (range 128–2048 MiB) while preserving unpaged healthy pacing. This is **not** a strict process-RSS cap: decoded objects, SQLite, parsing and other operations require additional memory. Large explicit paged workloads may use fewer concurrent requests. |
| F12: Missing pre-migration backup | Migration aborts if the required schema-backup fails, before modifying the older project. |
| F13: Cold media transient exception | Removed the uninitialized retry-state crash; cold 429/503/504 branches save the incomplete work and proceed through recovery. |
| F14: Media HTTP 403 false complete | Marks policy-blocked media inventory as incomplete; preserves the cursor and reports the blocker. |
| F15: Page grouping mutation | Media recovery does not halve/rotate the active paged layout after ordinary transient failures. |
| F16: Media collapse/cursor mutation | Media recovery retains collapsed request boundaries and committed resume keys rather than splitting them on generic 5xx. |
| F17: Media page evidence | Accepted media page rows and a layout-specific page ledger commit together; interrupted page batches can reuse durable page records. |
| F18: `robots.txt` acquisition exclusion | Exact once-decoded, case-insensitive path-basename `robots.txt` is policy-skipped before new text replay; query parameter values don't trigger it. Historical files/results are preserved. |
| Optional target time budget | The GUI and saved project config support **Skip target after N minutes**, disabled by default. Minutes count cumulative active indexing turns for one target in the current operation, for text and media. Expiry defers and preserves the remaining cursor for a later Run/Resume and does **not** report completed indexing. The budget is enforced at safe request/commit boundaries, not as a guaranteed hard mid-response deadline. |

## Tests and quantitative evidence

- Local test runner: **373 tests, 0 failures, 0 errors, 2 skips**, Linux x86-64, Python 3.13, GUI under Xvfb. New fixture tests include permanently failing and healthy targets, wrong-scope rows, invalid JSON, 403 media deferral, overlapping membership, robots exclusion and budget/cursor preservation.
- Healthy localhost replay benchmark (120 small captures): **120/120 validated files and persisted scans; 8.016 validated saves/s**.
- Extended localhost replay benchmark (900 small captures): **900/900 files and scan results; all SHA-256 checks passed; 7.991 saves/s overall**. Early/middle/late: **7.994 / 7.989 / 7.990 saves/s**. Observed process RSS around 120–125 MiB during this run. This is a small-body throughput baseline, **not** a worst-case memory test.
- These measurements are offline/localhost and do **not** establish sustained live Wayback rate, outage recovery over days, production SQLite growth, million-row resource ceilings, Windows/macOS CI or unbounded completeness under remote service malfunctions.

## Decisions made for evidence preservation

- No arbitrary date splitting for collapsed retries. A faster-looking progress bar is not evidence of equivalent capture membership.
- No deletion of old scans or previous downloaded files. Unknown capture classes are not silently converted to text.
- A forbidden/failed inventory remains explicitly **incomplete**, rather than becoming a fabricated empty result.
- No indiscriminate worker increase. Paged in-flight buffer capacity is bounded before scaling concurrency; the healthy local 8/s request pacing remains unchanged.
- Schema-9 helper tables were preferred over an incompatible schema-number bump, with version guards and a required backup for genuine older-schema upgrades.

**Operational recommendation:** Keep the time-budget checkbox disabled when exhaustive live indexing is essential, or use it to temporarily yield problematic targets and explicitly resume later. Use resume-key mode rather than numbered pages for collapse-sensitive inventories until cross-page canonical URL-key selection can be validated. Back up a project before updating and retain its existing output folder.
