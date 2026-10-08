# Network performance and Wayback pacing

Archive Scout 1.1.1 uses two shared request-attempt clocks. CDX/Timemap/index traffic starts at a conservative 2.5-second interval (24 attempts/minute) and replay traffic at 0.125 seconds (8 attempts/second). These are attempt ceilings, not throughput promises: redirects, retry attempts, and transport-backend fallbacks each consume admission because they each create network load.

The scheduler uses monotonic time and does not accumulate burst credit after idle periods or cooldowns. Per-target settings can make a target slower, but cannot silently weaken the effective project pool. Multiple workers are therefore useful for hiding response latency, not for multiplying the allowed request-start rate.

## 429/503 behavior

A live Wayback 429 or service-level 503 closes the shared host gate. Retry-After seconds and HTTP-date forms are treated as minimum deadlines. Without a valid header, adaptive-off uses a fixed retry wait of `min(5 seconds, rate_limit_base_pause)`, with no escalation or jitter. Adaptive-on uses the configured exponential cooldown (normally 60, 120, 240 seconds, bounded by the configured maximum), with positive-only jitter. One recovery probe is allowed after the gate reopens; fresh 500/502/504 responses do not count as healthy recovery. Optional adaptive pacing changes at most once per coalesced HTTP 429 incident and then relaxes after sustained healthy starts. It is experimental, still testing, and off by default in v1.1.0. Disabled clients use fixed requested spacing and fixed host fallback waits, including when an opted-in client shares the process; closing the last opted-in client clears optional pacing debt. Mandatory host recovery and Retry-After remain active in either mode.

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

## Recovery and pacing in 1.1.0

An admitted shared recovery probe can requalify an origin's cooled transport without stacking a 30-second backend wait on top of shared recovery. A backend-only scheduling delay is reconsidered after another successful request makes that backend available. This does not shorten a server's Retry-After or ordinary capture retry backoff.

Deferred recovery stops queued admissions while running healthy acquisitions settle normally and commit their results. Trustworthy headers/body prefixes can reset the connection-failure streak and end a matching connection-only outage; they cannot release a server throttle. Final payload validation still determines whether bytes are saved.

Select **Adaptive rate limiting (experimental — still testing)** in the GUI, set `adaptive_rate_limiting` in project JSON, or use `--adaptive-rate-limiting` / `--no-adaptive-rate-limiting` for a CLI run. Resume uses the current switch while preserving saved operation selection, retention, and server eligibility.

## Complete opt-out in 1.1.1

The checkbox controls both extra request spacing and escalating 429/503 fallback cooldowns. It stays experimental, still in testing, and off by default. The GUI's starting/maximum cooldown fields apply to adaptive-on only. A shorter configured base can shorten the fixed retry interval below five seconds. When projects sharing the host disagree, any active fixed-only client disables optional host escalation; opted-in clients can still use their own slower request spacing.

Only one recovery probe can start at eligibility. A healthy probe reopens admissions immediately at the configured fixed rate without accumulating burst credit. Already healthy downloads drain and commit normally. Genuine connection recovery, ordinary per-capture retries and explicit server Retry-After waits remain active.

Wait events identify `fixed_fallback`, `adaptive_fallback`, `server_retry_after`, or `connection_recovery`; service events include the parsed Retry-After duration and effective adaptive setting. Saved operation progress carries wait provenance and any server minimum. Opting out can shorten known adaptive debt, including after restart. Unlabelled waits saved by older versions are preserved because they may contain server instructions. No project-schema change is required.

Capture directories are prepared by the coordinator before transfers begin, preventing Windows workers from racing to create the same date directory.
