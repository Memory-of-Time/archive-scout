# v1.3.0 retry and CDX follow-up

This incremental add-on retains version 1.3.0, schema 12, the preceding replay
recovery patch and the optional indexing timer behavior. No database migration,
reset, forced reindex, settings change or file deletion is required.

## CDX failure fairness

The transport layer recognized raw HTTP-library incomplete-read errors, but
the JSON/text fallback guard checked only for an existing wrapper class. The
wrapper used for that check was not necessarily present in the exception
chain. The same failed response could therefore trigger more formats and
endpoint paths before control returned to the index scheduler.

Both CDX getter APIs now use the existing normalization helper when checking
the exception chain, and recognize the existing response-started marker.
Recognized transfer failures return directly to the checkpoint scheduler;
actual serialization errors still get the normal format fallback. The scheduler
can give another target a turn and later retry the saved cursor. This does not
shorten successful large queries or impose a target timer when its setting is off.

## Retry overhead

Small retry batches can be legitimate when only a few captures are due. This
patch does not delay ready captures to manufacture larger batches or retry
future captures early. Instead, text acquisition reuses one bounded worker pool
per operation/worker count. Direct downloader callers still own and close their
pool. Operation cleanup drains worker pools before closing their transports.
Local scanners remain separate from network cooldowns and close per batch.

Automatic retry selection now starts with due schedule IDs plus unresolved,
retryable legacy errors without a schedule, then looks up the capture IDs and
checks the same target/date/signature, latest decision and selected-error rules.
The working relation stays in SQLite, without project-sized Python lists.
Next-due lookup walks the existing due-time index and stops at the first eligible
capture. No schema change or new durable index is needed. Historical captures
without retry evidence no longer drive these recurring reads. Query work still
depends on the size of the active retry/error set; it is not universally constant.

The brief 90-second replay budget used for recovery is unchanged. Malformed
bodies are not marked successful, silently discarded, or declared permanently
missing by this patch. Persistent network recovery and existing failed-sweep
safeguards remain in place.

## Truthful status

The last-save timer and rolling rate are now owned by the operation's diagnostic
object, across batches. Only an actual committed save advances the timer. Before
the first save, it reports "none in this operation" and the numeric age is null.
Starting a retry batch no longer pretends that a save just occurred.

Selected retry progress is labeled "Retry batch" and additionally reports
operation-wide new saves. Existing batch count fields retain their meanings;
operation_new_saves and retry_batch are additive event fields. The operation
health summary already retains its cumulative new_saves total.

Health snapshots still update at the normal interval. Cached retry batch cleanup
no longer forces a fresh health message/file write on every tiny batch. Direct
calls, interrupted/failed batches, and operation close retain final summaries.
The build banner is emitted once per operation, and selected retry batches do
not repeat the full-queue preparation notice; their timing remains in diagnostics.
Existing optional Activity error updates and diagnostic controls are preserved.

## Evidence and limits

The supplied text log recorded 283,253 accumulated saves before its visible
85-minute zero-save retry interval, 1,059 queue-preparation messages and many
single-capture batches. These observations do not prove fresh captures were
starved, identify unique repeated capture IDs or establish an overall daily rate.

The Ogrish indexing excerpt showed repeated response-format and endpoint
fallbacks for the same broad query, including more than 15 minutes on 51.net.
The identified exception-chain gap was reproduced against the baseline source.
The patch prevents that multiplication; it cannot make unavailable Wayback
payloads valid or guarantee that a remote response will arrive promptly.

See the delivered validation report for exact tests, before/after measurements
and unresolved live-service, native-platform and long-run verification limits.
