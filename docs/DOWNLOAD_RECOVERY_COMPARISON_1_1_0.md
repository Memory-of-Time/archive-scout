# Archive Scout v1.0.4 versus current acquisition behavior

The attached old release is a changed-files package, not a complete v1.0.4 tree. Its unified patch does not include `archive_scout/cdx/client.py`, `archive_scout/downloads/rate_limit.py`, or `archive_scout/network/transports.py`. Consequently those older retry, admission, adaptive pacing, and backend behavior cannot be reconstructed from this attachment. No inferred deletion or rollback of later correctness work is justified.

## Evidence and limits

- The user reports consistent eight completed snapshots per second across millions of snapshots. This is useful field evidence; a reproducible live acquisition log or benchmark is not present in either supplied ZIP.
- Old `SOURCE_VALIDATION.txt` reports a 10-worker, 0.125-second shared request-start envelope, explicitly says the offline benchmark made zero HTTP attempts, and explicitly declines a guaranteed live completion rate.
- Old `config.py:255,261` and current `config.py:428,447` have the same 10 workers and 0.125-second default replay spacing. Both have 30-second connection and 180-second read timeouts. Current `constants.py:9` also preserves a 0.125-second replay floor. The main speed regression is not a slower default replay ceiling.
- The old patch changes the default envelope from four workers at 0.5 seconds to ten workers at 0.125 seconds. It also reduces local parsing/scoring passes and declares native parsers/matchers. These local CPU improvements are not an explanation for a reported log in which scanning already kept up.
- A nine-minute run spending 70% of its wall time in shared pauses leaves approximately 162 of 540 seconds for admissions. At an ideal eight starts/second, that alone lowers the average ceiling to approximately 2.4 starts/second before latency, retries, redirects, or validation. This is arithmetic illustrating the pause impact, not a measured acquisition result.

## Concrete current amplification paths

1. **Every connection failure starts a fixed local backend penalty.** `ResilientTransport._backend_failed` sets `cooldown_until` to now plus 30 seconds for every failure that is not classified as a response failure. A first isolated connection failure disables that backend for other workers even when other requests using the same backend remain healthy. Pool renewal correctly waits for owners to drain and must retain that safety.
2. **A backend penalty can extend the shared recovery series without a wire attempt.** `HttpClient.get` and `download_to_path` first obtain a shared host permit, then ask the transport to choose eligible backends. If all backends are cooling, `BackendsCoolingDown` leads to `finish_request(permit, recovered=False)` even though no connection was attempted. For a connection recovery probe, this increments the outage cycle and schedules a longer shared pause. Thus the local 30-second penalty can manufacture extra shared backoff.
3. **The local retry deadline does not know that recovery has already occurred.** Scheduled replay cooldowns become `ReplayRetryScheduled` objects with an immutable `eligible_at`. Coordinators hold those items in a retry heap until that deadline even if a successful sibling response clears a backend cooldown earlier. Server `Retry-After` deadlines and ordinary per-item retry delays must remain hard lower bounds; only synthetic backend-selection deferrals may be expedited.
4. **The coordinators cancel useful transfers at the first shared pause exception.** Text `downloader.py:1828,1838,1868` sets a combined internal cancellation flag and cancels queued futures. Media `downloader.py:511-516` does the same. Shared gates already stop future wire admissions, so this can throw away ongoing healthy body transfers, incur `.part` repair work, and require a fresh request. Preserve real user-stop cancellation and fatal-storage cancellation.
5. **Healthy ordinary streaming responses reset failure streaks late.** `HttpClient._response_progress` currently returns before processing a non-probe permit; ordinary transfer health is noted at completed responses. A burst of unrelated connection failures may reach the outage threshold while a healthy large body is still streaming. Resetting failure evidence on trustworthy HTTP progress can avoid false common-outage inference, while opening a shared gate still requires the current probe and does not bypass payload validation.

## Existing correctness improvements worth retaining

- Distinguish live Wayback 429/503 service responses from archived origin errors and external redirect failures.
- Treat `Retry-After` as a server minimum, including an explicit zero, and persist active service eligibility across resume.
- Coalesce old in-flight throttle replies into the existing incident; reject stale permits so one recovered gate cannot reopen on a duplicate no-header response.
- Admit only one actual recovery probe, validate its response activity before reopening, and do not equate payload completeness with connectivity recovery.
- Preserve captured manifests, queued selections, partial files, ordinary retry limits, bounded task creation, spool backpressure, classification, and process-pool scan isolation.
- Keep the fixed replay request-start interval shared across active clients and count every backend/redirect wire attempt. Eight starts/second is not an eight-completions/second guarantee.

## Focused regression coverage needed for v1.1.0

- A healthy transfer already started but unfinished when a sibling requests connectivity recovery must finish and commit without cancellation, for both text and media. The existing media sibling test only completes the healthy sibling before injecting the pause.
- Three initially failing requests with healthy parallel response progress should not falsely close the common connection gate.
- After all backends cool, one eligible queued probe must make a real wire attempt promptly at host eligibility. A synthetic no-backend result must release the probe lease without escalating the shared outage delay.
- A successful fallback or renewed drained pool must allow fresh work immediately; stale synthetic cooldown heap entries should not dominate the run.
- Recover after repeated outages while preserving every pending capture and `.part` prefix; explicitly stop during recovery and assert clean pending state.
- Assert that adaptive pacing disabled has a fixed interval even after repeated 429 incidents, while the host gate and server waits still hold. Assert opt-in adaptive state recovery, configuration round trip, CLI/help/UI testing label, and clients with different pacing preferences.
- Use deterministic offline timing/event barriers, local fixtures, and fake clocks instead of dependence on live Internet Archive availability. A long-run offline replay benchmark can verify repeated-outage overhead; it must be described as synthetic, not live eight-per-second validation.

The original cause of initial connection failures is still unconfirmed. These paths explain how current software can amplify failures into additional idle time; they do not establish whether the triggering failures came from Wayback, DNS, a proxy, TLS, the local network, or connection pooling.
