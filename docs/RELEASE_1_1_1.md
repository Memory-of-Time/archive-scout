# Archive Scout 1.1.1

Apply over the delivered v1.1.0 source and its CI fix. Project schema stays 13.

- Adaptive rate limiting remains experimental, still in testing, and off by default. Off now disables both extra request spacing and exponential 429/503 cooldowns. With no valid Retry-After header, retries use one shared probe after a fixed wait of at most five seconds; repeated throttle responses do not turn that into minutes. On preserves configurable escalating cooldowns and optional slower pacing.
- Server Retry-After deadlines, ordinary capture retry backoff, connection recovery and pending capture queues are preserved. Known optional wait debt is shortened when opting out; unlabelled legacy waits are retained because they may be server-requested.
- Activity identifies whether the app or server requested a wait. Persisted progress retains that distinction for stop/resume. Recovery coordinators follow the current shared deadline so they resume promptly when it changes.
- Download directories are created before concurrent transfers start, preventing a Windows recursive-directory creation race observed during validation.
- Runtime/package/Windows/workflow identity is 1.1.1. The v1.1.0 CI line-ending fix is retained, and tests upload their logs and summaries.

The complete supplied v1.0.5 repository was used only to audit download pacing, scheduling and recovery. No old schema, scanning, payload validation, error classification, selection or retention implementation was imported. Its 0.125-second fixed start schedule matches the current default. Actual saved snapshots per second still depend on server responses and network latency.
