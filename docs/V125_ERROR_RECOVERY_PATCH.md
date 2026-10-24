# Archive Scout v1.2.5: error recovery patch validation

Base: `archive-scout-main(20261024-162650).zip`, supplied October 24, 2026.
Base ZIP SHA-256: `6c47820e87f7fb14f719cb4c187c4543eaff94d7907a6efe497b52570bf7a9f7`.
Version 1.2.5 and schema 10 are retained. This is a changed-files patch, not a full repository.

## Result and scope

The patch fixes supported stacked content encodings being classified as binary before fully decoding;
refused/DNS connections mislabeled as timeouts; oversized 429/503 error responses bypassing the
control-response path; negative cooldown displays; repeated history-dependent retry lookups; and
the inability to deliberately recheck selected missing captures. It adds bounded diagnostic evidence
for the recurring live digest failures without assuming those failures are false rejections.

The live cause of the NothingToxic digest mismatches is still unverified. This patch does not claim
to fix all 779 records, restore live 8/s, eliminate service outages, or prove completeness of the
existing project. Historical attempts and unresolved evidence are retained.

## Changes

1. **Compression and routing.** Decode gzip/deflate/identity chains in reverse application order,
   up to four supported layers, with bounded output chunks and the original wire-length check.
   Invalid/unsupported chains remain retryable representation failures. Truncated members and
   bad header repairs are rejected. A supported stacked text body reaches the local scanner and
   its persisted positive-match coverage instead of becoming a terminal binary skip.
2. **Connection classification.** Handle urllib3 NewConnectionError/NameResolutionError before
   their ConnectTimeoutError superclass. Transport timeout flags and CDX timeout classification
   also retain this distinction. Actual connect timeouts still classify as timeouts.
3. **Control responses.** HTTPX and urllib3 inspect 429/503 status before applying payload-size
   limits. Curl can recognize control headers after an early size abort without treating an
   incomplete field as a complete Retry-After value. No error body must be downloaded for this.
   Existing shared pauses, Retry-After handling, fixed pacing, and recovery probes are preserved.
4. **Retry selection.** Build an indexed TEMP SQLite queue of active, nonqueued scheduled retry
   identities once per batch. Failure commits update it incrementally. Each selection rechecks
   the persistent schedule, current state, error flags, project scope and memberships. Successes
   from another WAL connection cannot be re-admitted. Changed future deadlines are retained.
   Persistent schedules outside the active scope are never deleted. The existing one-after-16
   interleaving and per-capture retry backoff are unchanged.
5. **Selected 404 rechecks.** A deliberate capture-ID selection or explicit missing_capture category
   selection can recheck recorded 404s. This does not globally reopen robots/exclusion errors,
   change stored retryability flags, or create an endless automatic missing-capture sweep.
   The existing selected-errors GUI action and acquisition-only retry path remain compatible.
6. **Diagnostics.** Existing network diagnostics enable a build fingerprint, first-category notices,
   30-second summaries and a final summary. Completed HTTP response counters retain raw Retry-After,
   selected pause length, backend, gate wait and request-turn/transfer-write timing. These are
   response-level measurements, not a physical-wire-start counter or isolated server-latency trace.
   The backend elapsed time includes writing the transfer file, and excludes final fsync/publication.
7. **Bounded failure samples.** Up to 16 fixed sample slots per project, at most 20 KiB per JSON file,
   contain capture identity, build, timestamp, expected/calculated repair digest, selected safe
   response headers, byte counts, and raw/decoded prefixes capped at 4 KiB each. Repeated failures
   do not repeatedly write the same slot within a batch. Samples and latest.json are under
   `diagnostics/text-acquisition` inside the project. Authorization/Cookie/Set-Cookie headers
   are excluded, and URL credential fields are redacted. Optional diagnostic write failures do
   not downgrade a capture or abort acquisition. These prefixes are never published as bodies,
   used as resume proof, or treated as complete evidence.

## Tests actually run

- Full final suite: **523 tests, zero failures, zero errors, four GUI skips** on Linux/Python 3.12.14.
  Complete test names/results are in the evidence ZIP. There are 21 new regression methods.
- Actual local HTTPX, urllib3 and curl transfers: stacked text decode; wrong digest rejection in
  header-recovery mode; diagnostic byte/header evidence; and large-announced-body 503 handling.
- Malformed fixtures: unknown inner encoding, incomplete inner gzip, inconsistent wire length,
  excessive encoding layers, and wrong digest. None becomes a completed text body.
- End-to-end mixed cohort: stacked text is saved, scanned and scored; the mismatched capture
  remains error/retryable with no document, while a bounded diagnostic sample is retained.
- Scheduling: selected scope and membership, queued identities, deadlines, incremental failures,
  another WAL connection completing a capture or changing its deadline, and reopen/rebuild.
- Explicit 404 recheck selection: one sweep, with other nonretryable exclusions left alone.
- Diagnostic limits/disabled mode/write failures; original display throttling and final counters
  remain covered by the full suite.
- Compilation, release metadata/workflows validation, package installation and installed-module
  import, plus offline smoke benchmark: 1,000 CDX rows, 100 result rows, 1,024-byte body parameter.

Initial validation exposed two implementation issues that were corrected: large control responses
were checked against size limits too early, and curl could abort before its final header separator
was dumped. Fixture ID/deadline mistakes were also corrected. A first full-suite attempt needed
proxy-environment cleanup for local fixtures; it is not presented as a product defect.

## Healthy acquisition comparison

Same fixture, exact supplied baseline versus final patched application source. Each condition
acquired 96 real streamed/fsynced bodies and verified complete expected file hashes, documents,
and persisted scan coverage. There were **576 verified new captures across the six conditions**.
The real event queue used a 0.5-second display interval; native Tk painting was not measured.

| Historical metadata rows | Supplied baseline | Patched |
|---|---:|---:|
| 10,000 | 8.035/s | 8.037/s |
| 300,000 | 7.965/s | 7.969/s |
| 1,000,000 | 7.747/s | 7.743/s |

These wall rates include preparation and final scan drain. Differences are small and do not
establish a general speedup. The measurements show no clear healthy-path throughput regression
in this fixture. The million-row population contains synthetic metadata/document placeholders,
not a million retained bodies or realistically populated full-text/match history.

The acquisition cohorts used about 0.75-1.7 process CPU seconds per roughly 12-second condition.
Peak process RSS, including constructing/using synthetic history and mappings, reached about
384 MiB on baseline and 381 MiB on patched. This is not an isolated acquisition working set or
proof that memory stays constant across days.

The retry stress check used 1,000, 10,000 and 100,000 valid due-error records already in the main
queue. The old lookup repeats a linear scan; patched probes use the prepared empty ready queue.
The one-time preparation cost is reported separately in `patch-retry-scaling.json`. Removing
this repeated work does not establish that it alone explains the observed live 5/s limit.

## Evidence limits and unchanged policies

Direct Wayback access failed here with a DNS-resolution error. Native Windows/macOS, Python 3.14,
antivirus/storage effects, native GUI painting, physical power loss and multi-day live performance
remain unverified. Short loopback runs do not establish overnight service consistency.

The normal-response CDX verification policy is not broadened by this patch: the existing selected
CDX comparison remains the gate for mislabeled-gzip header recovery. A stored SHA-256 receipt proves
integrity of received bytes; it is not independently proof that every ordinary replay response
matches its expected CDX digest. The earlier ordinary-response identity gap remains a known limit;
changing acceptance across all historical compression representations without live evidence could
produce another major acquisition regression. This patch does not claim that gap is solved.

The dominant live header-recovery mismatches need their actual response evidence. Do not delete
their errors, reset acquisition/scanning state, append invented bytes, disable digest validation,
or treat unvalidated prefixes as successful saves. Automatic temporary-failure recovery remains
enabled by the existing settings. The broad 503 pause policy is unchanged pending response evidence.

Text/media routing after indexing, text/media collapse policies, selected keywords, retained/deleted
body settings, project settings, report formats and schema identities are preserved. The display
rate option is retained. No automatic worker scaling, adaptive host pauses, added per-request CDX
lookup, or new default request delay is introduced.

## Apply and collect useful evidence

Stop Scout normally, extract the patch ZIP, and merge `archive_scout`, `tests`, and `docs` into the
source checkout that contains `pyproject.toml`. Accept replacement of the listed files. Keep every
file in its included folder. Restart through the normal source launcher and resume the existing
project. This source patch does not modify an already built standalone executable; that needs a
normal rebuild. Do not delete project databases, capture files or receipts.

If digest failures recur, provide `diagnostics/text-acquisition/latest.json`, the small sample JSON
files, and activity covering the same timestamps. Samples identify the build and observation time
so old release errors can be distinguished from current attempts. The sampling files include
unvalidated prefixes for diagnosis and must not be merged into the captures directory.

Protocol references: [HTTP content encoding](https://www.rfc-editor.org/rfc/rfc9110.html#name-content-encoding),
[HTTP 404](https://www.rfc-editor.org/rfc/rfc9110.html#name-404-not-found),
[urllib3 exception hierarchy](https://urllib3.readthedocs.io/en/stable/reference/urllib3.exceptions.html).
