# Network performance

Archive Scout deliberately optimizes overlap rather than raw request frequency.

The default CDX request-start interval remains conservative while multiple bounded workers can wait on independent Wayback responses. Persistent HTTP pools, alternate transports, resumable windows, and bulk database commits reduce wasted time without turning a large project into an uncontrolled burst of requests.

Text and media downloads have their own bounded worker count and fixed delay. Media responses are streamed to disk rather than retained as complete in-memory bodies.

External embedded-media exact lookups also share the CDX request limiter. Known excluded/robots-blocked media hosts are short-circuited so project speed is not dominated by repeated requests that the archive has already declared unavailable.

## v1.2.5 recovery patch

Some replay responses advertise gzip but deliver an uncompressed payload. The
streaming decoder accepts that exceptional representation only after its complete
bytes match the selected capture's base32 CDX SHA-1 digest and HTTP framing checks.
Without that evidence, it records a representation failure and schedules recovery.
Normal gzip CRC/end-marker checks and truncated-transfer rejection remain active.
Recovered text saves retain SHA-256/size evidence and a resolved validation note.
CDX `length` is never used as an uncompressed body-size oracle.

Healthy identity/gzip transfers keep bounded streaming and their existing pacing.
Only the header-mismatch path adds SHA-1 calculation. Representation failures
return to durable retry scheduling rather than cycling through every backend
and immediate logical retry. A worker whose previously selected backend became
unavailable can use a healthy alternative without waiting out the failed route's
cooldown. All routes cooling still use exclusive recovery probes and interruptible
waits. Fixed CDX/replay pacing and Retry-After admission remain in force.

Embedded lookup retry rounds reuse discovery checkpoints, rather than reopening
every saved body each round. Legacy scan scope validation uses indexed TEMP rows,
including overlapping target membership, instead of an unbounded SQL OR tree.
Offline fixture benchmarks measure local work only; they do not establish live
Wayback throughput or multi-day reliability.

## v1.2.5 connection and transfer add-on

Apply this add-on after the Recovery/Redirects/Operations patch. Public version
1.2.5 and schema 10 remain unchanged. Curl errors retain their numeric exit code;
wrapped urllib3 reset errors are inspected through exception arguments as well
as causes. Connection failures can no longer be recorded as successful backend
health checks just because curl's message omitted the words SSL or TLS.

HTTPX, urllib3 and curl incomplete-body failures are normalized before retry
policy runs. A malformed body or exhausted connection turn returns to its
persistent acquisition/indexing owner rather than multiplying the same failure
through four immediate logical turns. A healthy alternate backend is still
eligible during a connection trial. Ordinary read timeouts, pool exhaustion,
rate limits and storage errors retain their separate handling. Partial text
transfers are rejected; this change does not waive framing, digest or gzip
checks, or discard validated resumable media prefixes.

Streamed HTTP success and archive redirect-policy refusals now complete host
recovery probes and reset the optional connection-failure evidence. Body failures
can establish connectivity without being accepted as saved representations.
Fixed request spacing, Retry-After admission, existing backend cooldowns and the
optional coordinated-recovery setting are preserved. Local scanning remains
independent of the network gate.

Text progress now separates unique acquisition obligations satisfied, retained
captures, exclusions/routing, attempt outcomes, and errors. Repeated failed
attempts do not advance the capture bar. Acquisition rate excludes preparation;
the additional batch rate includes queue setup and client initialization.
These are batch metrics, not a claim about whole-run indexing and report time.
Scope counters are aggregated once per download batch; outcome updates use
constant-size in-memory arithmetic and ID reads on failures, not historical
database counts for every successful save.
