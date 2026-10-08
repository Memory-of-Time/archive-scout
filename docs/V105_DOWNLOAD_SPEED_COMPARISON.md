# Download speed comparison with the complete v1.0.5 source

Reference: the user-supplied `Archive-Scout-v1.0.5-2026-09-12-Complete-Repository.zip`. Its constants confirm version 1.0.5 and schema 7. The current source remains on schema 13. The reference was inspected for download speed only; archived instructions were treated as reference data, not additional user requests.

| Download behavior | Complete 1.0.5 source | 1.1.1 |
| --- | --- | --- |
| Default workers | 10 | 10 |
| Default request spacing | 0.125 seconds | 0.125 seconds |
| Connect/read timeouts | 30/180 seconds | 30/180 seconds |
| Optional adaptive spacing | Absent | Off by default |
| Headerless service fallback | 30 seconds, exponential | Off: fixed at most 5 seconds. On: configured exponential policy |
| Queue | Bounded, 2 × workers | Bounded, 3 × workers, delayed retries release workers |
| Backend cooldown | 30 seconds; tries cooled backends when all are unavailable | Eligible recovery probe can requalify cooled backends; obsolete backend-only deferrals wake early |
| Healthy probe | Reopens aggressively, including some failures | Requires validated recovery evidence; opens promptly when proven |
| Scanning | Runs in fetch workers | Acquisition and scanning are independent with bounded backpressure |

The old `downloads/rate_limit.py` contains no adaptive rate multiplier. Its fixed scheduler spaces starts relative to the most recent admission and keeps a process-wide clock. Those useful characteristics remain in current fixed mode. Old `config.py` supplies the same workers, replay spacing and timeout defaults, so raising concurrency or rewriting timeouts is not justified by this source comparison.

Old `cdx/client.py` applied pacing before a logical request; internal transport fallback and redirect attempts could therefore bypass that logical clock. Current code counts each actual wire attempt. Copying the old policy could increase server load without increasing saved throughput, so it was retained only as reference evidence. The old gate still escalated no-header waits and could treat some failing probes as recovered; neither behavior was copied. Current validated payloads, queue preservation, scanning, selection and retention remain authoritative.

The reproducible offline comparison loads only the inspected old rate-limiter module, not the old application or its database. At two million virtual healthy admissions, both fixed schedulers retain the 0.125-second schedule (eight starts per second). Four consecutive headerless throttle probes with deterministic jitter give the old default waits 30/60/120/240 seconds, current off 5/5/5/5 seconds, and current on 60/120/240/480 seconds. These are synthetic scheduling checks, not millions of downloaded snapshots or a measured live completion rate.

Run `python scripts/benchmark_request_spacing.py --admissions 2000000 --reference-tree PATH_TO_EXTRACTED_V105 --output validation/pacing-comparison.json` to reproduce the comparison. The report hashes the exact reference pacing module and separately checks explicit server waits, fixed intervals and post-recovery burst prevention. CI runs the same current scheduling benchmark without requiring the old ZIP.

The user's reported sustained eight saved snapshots per second was not reproduced against live Internet Archive. The attached activity log reports HTTP 429 responses but does not record Retry-After headers; its particular 62/128-second waits cannot be conclusively attributed to application fallback. The new telemetry resolves that ambiguity on future runs. The original cause of connection failures remains unconfirmed.
