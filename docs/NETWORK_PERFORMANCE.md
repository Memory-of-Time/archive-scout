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
