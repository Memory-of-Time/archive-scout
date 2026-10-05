# Network performance and Wayback pacing

Archive Scout 1.0.6 uses two shared request-attempt clocks. CDX/Timemap/index traffic starts at a conservative 2.5-second interval (24 attempts/minute) and replay traffic at 0.125 seconds (8 attempts/second). These are attempt ceilings, not throughput promises: redirects, retry attempts, and transport-backend fallbacks each consume admission because they each create network load.

The scheduler uses monotonic time and does not accumulate burst credit after idle periods or cooldowns. Per-target settings can make a target slower, but cannot silently weaken the effective project pool. Multiple workers are therefore useful for hiding response latency, not for multiplying the allowed request-start rate.

## 429/503 behavior

A live Wayback 429 or service-level 503 closes the shared host gate. Retry-After seconds and HTTP-date forms are treated as minimum deadlines. If no valid header is present, the first coordinated cooldown is at least 60 seconds and any jitter is positive-only. One recovery probe is allowed after the gate reopens; fresh 500/502/504 responses do not count as healthy recovery. Adaptive pacing changes at most once per coalesced service incident, then relaxes gradually after sustained healthy starts instead of snapping immediately back to the fastest configured interval.

A historical 429/503 reproduced inside an archived replay is different: when the response carries replay/memento context it is treated as an archived origin status rather than evidence that the live Wayback service is throttling Archive Scout.

## Metrics

The network client tracks logical operations separately from actual wire request starts. Progress output reports wire starts, completions, transport failures, retries/rate events, pacing/host-gate/retry wait time, saved captures, and network bytes. This makes rate-limit diagnosis possible without mistaking worker concurrency for request volume.

## Recovery

Rate-limit exhaustion and connectivity failure are separate pause reasons. Each recovery cycle shares a deadline across workers, including time spent behind another worker's cooldown. With persistent recovery enabled, exhausting that cycle temporarily stops admission, settles existing attempts and waits for actual service eligibility. The same replay client and worker pool then continue with a renewed recovery cycle and one real probe. Renewal never shortens Retry-After or clears an active cooldown. Connection outages use a shared bounded exponential pause without pretending they are quota incidents.

The exact queue and any wall-clock eligibility are saved for Resume. **Pause & save** remains cancellable during recovery. Disabling persistent recovery instead ends automatic recovery at the saved pause boundary. Persistent HTTP pools, validated Range resumes, durable page checkpoints and bulk SQLite commits reduce repeated work without bypassing the shared pacing policy.

## Connection prevention in 1.0.6

Backend cooldowns are scoped by request origin. One ordinary queued request requalifies a previously failed preferred backend after its cooldown; other workers continue through the functioning fallback. A later curl completion cannot replace a recovered higher-priority pool. Body stalls, read resets, malformed response bodies and pool waits do not cool an entire backend or trigger the common connection-setup outage policy. A failed allowed external redirect is attributed to its destination rather than to Wayback.

HTTPX native chunks and urllib3 read1 expose small received prefixes promptly. Curl retains validated identity prefixes on transfer failure and detects live 429/503, redirect and binary-prefix decisions before waiting for the complete body. Archived origin errors with Memento evidence retain their existing handling. Content-Range, encoding, byte limits, redirects and single partial-file ownership remain mandatory.

Text acquisition keeps delayed retries in a bounded coordinator queue, preserving the configured per-capture attempt limit. Workers return between failed attempts rather than sleeping through backoff. Ready fresh captures take precedence when repeated retries would otherwise occupy the pool. Existing pending paths and partial files survive Pause & save; an interrupted invocation resumes under the existing per-invocation retry contract. Media gets the shared transport fixes but retains its current media retry scheduler.

Activity reports delayed retry count. Structured progress also includes scheduled retry seconds and requested/effective request intervals. Scheduled retry time is not worker-blocked time. The existing transport_failures compatibility field counts attempt exceptions, including some service and classification exceptions; it is not a pure count of failed TCP connections. Default timeout values remain unchanged pending controlled latency measurement.
