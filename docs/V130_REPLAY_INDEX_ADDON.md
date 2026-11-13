# v1.3.0 replay and indexing add-on

This is incremental on the existing v1.3.0 sustainability release and Windows
test-cleanup add-on. The public version remains **1.3.0**, with schema **12**.
Existing projects, selections, saved settings, routing and checkpoints remain
compatible. No database migration or reindex is required for this add-on.

## Verified replay failure

The supplied `text-acquisition.zip` contains a sample for capture 223478,
timestamp 20080723134758. The 121 raw bytes in sample-10 independently match
both its recorded SHA-256 and expected CDX SHA-1
`UK4VJ7QXINJQ2BIFPYSP6B5NHIINZKGN`. Its replay advertises gzip and 132 bytes,
but supplies plain HTML and closes after 121 bytes. Previously the stream
exception prevented the existing digest recovery from completing.

For this specific contradictory-gzip path, a nonempty candidate can now finish
validation after a recognized premature HTTP EOF or length contradiction.
Acceptance still requires the exact selected capture's valid CDX SHA-1. The
same path is exercised through httpx, urllib3 and curl. Curl exit 18 is passed
through the shared verifier; it is never accepted merely because bytes exist.
Other curl errors retain their existing failure behavior.

Wrong/missing digests, genuinely truncated gzip streams, empty responses,
timeouts, cancellation, partial ranges, redirects to another payload and byte
limits retain their checks. The returned response carries a recovery marker;
diagnostics keep a cumulative recovery count and the latest eight redacted
proof summaries in their existing bounded health files.

This does not recover the two supplied digest-mismatch samples or the ten
sampled unfinished gzip streams that decode to no content. Their failures stay
unfinished and eligible under the existing retry policy. A repeated text
representation failure receives a minimum retry spacing after its third
failure, capped by the configured maximum retry delay and subject to the
existing per-capture jitter. This matches the intent of the existing media
recovery spacing. It never marks a capture downloaded or permanently missing.
Fresh work and due retries retain the existing interleaved scheduling policy.

## Indexing time budgets

The explicit target budget already consulted `skip_slow_index_targets`.
However, a second no-progress guard silently imposed a 30–300 second active
time limit derived from the network retry delay. That guard ran independently
of the checkbox in text and media indexing.

Indexing now uses the guard's attempt-count mode. Text and direct-media targets
only receive a target elapsed-time budget when **Skip target after** is enabled.
Embedded lookup recovery sweeps also no longer expire merely because a sweep
takes a long time. Consecutive failed-attempt limits remain bounded, and work
deferred after those failures remains explicitly incomplete with its checkpoint
retained. Turning off the target timer does not disable individual request
timeouts, payload size limits, Stop, or failed-attempt safeguards.

The existing GUI setting and project JSON remain unchanged. Loading/saving a
project preserves the opt-in value for both GUI and CLI operation paths.

## Owner-stall diagnostics

When acquisition diagnostics are enabled, a small daemon observer checks the
text acquisition owner's heartbeat every five seconds. After 120 seconds
without an owner heartbeat, it writes thread locations and the owner stage.
It writes at most once per five minutes and retains only `stall-latest.json`
and `stall-previous.json`, each capped at 64 KiB, under
`diagnostics/text-acquisition/`.

Reports contain at most 24 threads and 20 frames per thread. They include
function names, source paths and line numbers, without frame variables, source
lines or archived content. The observer does no database or network work and
stops at batch cleanup. Disabling diagnostics disables this observer too.

This provides evidence for a future stall. It does not establish the cause of
the supplied 20-minute gap or repair an unidentified blocking operation.
Computer suspension can also produce an elapsed heartbeat gap.

## Limits

The supplied run recorded real connection refusals across backends and no
validated saves. This patch does not claim to fix a remote endpoint, firewall,
or other unverified cause of those refusals. Existing persistent connection
recovery, host admission, pacing and server Retry-After behavior are retained.

The reported 19-minute startup interval and 346-second queue preparation also
remain unresolved. No startup speedup, large-project speedup or overnight
live-service consistency is established by this add-on. Short local regression
and throughput checks are scoped evidence, not a live-service guarantee.
