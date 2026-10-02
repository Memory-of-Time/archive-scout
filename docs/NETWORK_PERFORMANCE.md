# Network performance and Wayback pacing

Archive Scout 1.0.1 uses two shared request-attempt clocks. CDX/Timemap/index traffic starts at a conservative 2.5-second interval (24 attempts/minute) and replay traffic at 0.125 seconds (8 attempts/second). These are attempt ceilings, not throughput promises: redirects, retry attempts, and transport-backend fallbacks each consume admission because they each create network load.

The scheduler uses monotonic time and does not accumulate burst credit after idle periods or cooldowns. Per-target settings can make a target slower, but cannot silently weaken the effective project pool. Multiple workers are therefore useful for hiding response latency, not for multiplying the allowed request-start rate.

## 429/503 behavior

A live Wayback 429 or service-level 503 closes the shared host gate. Retry-After seconds and HTTP-date forms are treated as minimum deadlines. If no valid header is present, the first coordinated cooldown is at least 60 seconds and any jitter is positive-only. One recovery probe is allowed after the gate reopens; fresh 500/502/504 responses do not count as healthy recovery. Adaptive pacing changes at most once per coalesced service incident, then relaxes gradually after sustained healthy starts instead of snapping immediately back to the fastest configured interval.

A historical 429/503 reproduced inside an archived replay is different: when the response carries replay/memento context it is treated as an archived origin status rather than evidence that the live Wayback service is throttling Archive Scout.

## Metrics

The network client tracks logical operations separately from actual wire request starts. Progress output reports wire starts, completions, transport failures, retries/rate events, pacing/host-gate/retry wait time, saved captures, and network bytes. This makes rate-limit diagnosis possible without mistaking worker concurrency for request volume.

## Recovery

Rate-limit exhaustion and connectivity failure are separate pause reasons. A rate-limit incident has one absolute recovery deadline shared across workers, so time already spent behind another worker's gate counts toward the same budget. If Retry-After extends beyond that automatic budget, Archive Scout persists the wall-clock eligibility time and pauses without an early probe, including after restart. Both pause classes preserve the exact queue/checkpoint state for Resume, but the UI tells the user whether to wait for a quota/overload cooldown or restore connectivity. Persistent HTTP pools, validated Range resumes, durable page checkpoints, and bulk SQLite commits reduce repeated work without bypassing the shared pacing policy.
