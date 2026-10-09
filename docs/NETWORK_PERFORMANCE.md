# Network performance and Wayback pacing

Archive Scout 1.0.0 uses two shared request-attempt clocks. CDX/Timemap/index traffic starts at a conservative 2.5-second interval (24 attempts/minute) and replay traffic at 0.125 seconds (8 attempts/second). These are attempt ceilings, not throughput promises: redirects, retry attempts, and transport-backend fallbacks each consume admission because they each create network load.

The scheduler uses monotonic time and does not accumulate burst credit after idle periods or cooldowns. Per-target settings can make a target slower, but cannot silently weaken the effective project pool. Multiple workers are therefore useful for hiding response latency, not for multiplying the allowed request-start rate.

## 429/503 behavior

A live Wayback 429/503 honors Retry-After seconds or HTTP-date deadlines, including zero. Without a usable header the fixed wait is five seconds. Requests then resume at fixed spacing; there is no adaptive escalation or single-probe recovery phase. Historical archived-origin responses do not close the live service gate. Connection/backend retries use brief fixed waits.

A historical 429/503 reproduced inside an archived replay is different: when the response carries replay/memento context it is treated as an archived origin status rather than evidence that the live Wayback service is throttling Archive Scout.

## Metrics

The network client tracks logical operations separately from actual wire request starts. Progress output reports wire starts, completions, transport failures, retries/rate events, pacing/host-gate/retry wait time, saved captures, and network bytes. This makes rate-limit diagnosis possible without mistaking worker concurrency for request volume.

## Recovery

Rate-limit exhaustion and connectivity failure are separate pause reasons. Both preserve the exact queue/checkpoint state for Resume, but the UI tells the user whether to wait for a quota/overload cooldown or restore connectivity. Persistent HTTP pools, validated Range resumes, durable page checkpoints, and bulk SQLite commits reduce repeated work without bypassing the shared pacing policy.
