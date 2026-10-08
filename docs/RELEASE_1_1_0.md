# Archive Scout v1.1.0

This release targets time lost to connection recovery and makes adaptive request spacing optional. It updates the supplied v1.0.9 source; the project schema remains 13.

## Recovery

The old path could admit a recovery probe after a short shared outage wait, refuse every backend because of a separate 30-second cooldown, and treat that no-wire outcome as another failed probe. The new path lets an admitted recovery probe requalify cooled transports, and backend-only retry delays respond to proven recovery.

Deferred failures stop queued admissions without cancelling healthy acquisitions already running. Successful transfers keep their normal validation and durable commit path. Trustworthy progress can reset the connection failure streak and clear only the matching connection-outage incident. HTTP 429/503 recovery, explicit Retry-After deadlines, and saved server eligibility retain their authority.

Pending captures and partial files remain resumable. User-requested Pause & save continues to cancel active I/O promptly. Existing capture selection, media rules, payload validation, retention policy, and bounded scanning remain in place.

## Adaptive pacing

**Adaptive rate limiting (experimental — still testing)** is **off by default**, including for older project files that do not contain the setting. Opt in from the GUI, project JSON (`adaptive_rate_limiting: true`), or the CLI (`--adaptive-rate-limiting`). Use `--no-adaptive-rate-limiting` to disable it for a run. Resume honors the current pacing switch while preserving the saved operation's selection and retention.

The switch controls extra request spacing after a live HTTP 429. It does not disable fixed traffic ceilings, host outage/service recovery, or server-requested waits. Fixed clients do not inherit optional pacing debt from adaptive clients sharing the same process.

## Speed evidence and limits

Both the supplied v1.0.4 patch and current source use ten default replay workers and 0.125-second replay spacing: eight request starts per second. The older attachment does not contain the entire networking implementation, and its offline validation does not measure live download speed. The user's sustained eight saved downloads per second is historical field evidence, not a result reproduced by this release's offline tests.

The supplied issue reports a nine-minute run with roughly 70% of its time in recovery. That log was not attached here, so the percentage and initial connection-failure cause cannot be independently verified. The patch corrects confirmed code paths that can amplify waits; it does not identify the network's initial failure or promise live Wayback throughput.

See [validation scope and results](VALIDATION_1_1_0.md). Cross-platform CI retains its existing Python 3.11/3.12 matrix and writes a test log plus JSON summary as downloadable workflow artifacts.
