# Archive Scout 1.0.7

This patch addresses redundant waiting and media recovery failures in the v1.0.6 waiting-speed audit. Apply it over v1.0.6, retaining relative paths. It includes the Windows CRLF test-fixture repair. Existing projects stay on schema 12, with the same capture storage, CDX inventory strategies, scanner, Hitlist checking and content classification.

## Shared recovery

- The first fallback deadline belongs to the shared incident. Duplicate no-header responses from older in-flight requests do not restart it, displace its probe or reopen a recovered gate.
- A later explicit Retry-After deadline remains authoritative, even beyond the configured fallback maximum. Zero and an expired HTTP date are distinguished from a missing header.
- Only a fresh failed probe escalates the fallback. Failed-probe completion is idempotent, and generation changes prevent an older completion from displacing a newer probe.
- Jitter is capped after multiplication. Connection fallback is capped at 60 seconds and generic response retry fallback at 120 seconds. Explicit server delays are preserved separately.
- Active clients register and release their pause policy. Closed projects do not permanently impose their fallback settings. Adaptive pacing stores a multiplier separately from the active requested baseline.
- HTTP 429 temporarily reduces the relevant traffic pool's rate. HTTP 503 still invokes shared service recovery, without adding a second quota pacing penalty. Eight healthy responses over at least five seconds halve the adaptive multiplier toward its baseline. A single success does not immediately restore a repeatedly throttled pool.
- A trustworthy archived response header, validated replay prefix or plausible CDX data prefix can release the service probe while the body continues. This is evidence of service activity only: incomplete bodies, invalid ranges, incomplete CDX inventories and incorrectly classified payloads are still rejected by the existing validation.
- An allowed external redirect destination's 503, 429 or connection failure cannot create a Wayback-wide recovery incident. Typed transport origin attribution is authoritative.
- Query-specific CDX response failures retry their saved work without marking a healthy archive host offline for other projects.

## Scheduling and visibility

Text and media retries use bounded coordinator queues. A retry waiting for its deadline releases its worker, and a due retry batch has a bounded allowance to stage fresh database work. Ordinary retry budgets remain enforced. Media counts one logical download job across its internal attempts, preserves partial files and uses internal cancellation for recovery instead of setting the user's Pause flag. Other completed media futures are committed before recovery returns; interrupted jobs remain pending.

Request spacing uses the actual remaining deadline, removing the artificial 50 ms minimum sleep. Acquisition events retain their existing fields and add `service_gate_wall_seconds`, an elapsed union of shared gate closure/probe time since the client opened. Summed worker waits are labeled `worker-s`; shared service time is labeled `wall-s`. No per-wait SQLite updates or dashboard aggregate queries are added.

## Defaults and completeness

The normal request ceilings remain 2.5 seconds for CDX starts and 0.125 seconds for replay starts. Conservative missing-header pause defaults remain 60/600 seconds; explicitly configured shorter fallback policies now round-trip instead of being silently replaced. Server Retry-After always takes precedence. The default persistent-recovery behavior continues saved work automatically; the user's stop request and unavailable storage retain their existing handling.

There are no new dependencies or schema migrations. Text/media routing, media formats, snapshot promotion, redirects, full-source scanner coverage and Hitlist body coverage remain unchanged. This patch does not claim a measured live Wayback speed increase or guarantee eight successful files per second.

## Validation

See `validation-1.0.7.json` for local test, benchmark, package metadata and patch reconstruction evidence. New regressions cover duplicate deadlines, later server deadlines, probe identity, bounded jitter, configuration lifecycle, external origins, early service recovery without accepting truncated bodies, bounded fresh-work scheduling and media pause/resume.

The cross-platform test workflow verifies package version 1.0.7 on Python 3.11/3.12. Native Windows/macOS CI and packaged application builds must run on GitHub; they were not executed in the Linux verification environment. Tagged Windows builds retain the repository's existing Artifact Signing requirement.
