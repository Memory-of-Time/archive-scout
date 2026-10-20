# Network performance

Archive Scout deliberately optimizes overlap rather than raw request frequency.

The default CDX request-start interval remains conservative while multiple bounded workers can wait on independent Wayback responses. Persistent HTTP pools, alternate transports, resumable windows, and bulk database commits reduce wasted time without turning a large project into an uncontrolled burst of requests.

Text and media downloads have their own bounded worker count and fixed delay. Media responses are streamed to disk rather than retained as complete in-memory bodies.

External embedded-media exact lookups also share the CDX request limiter. Known excluded/robots-blocked media hosts are short-circuited so project speed is not dominated by repeated requests that the archive has already declared unavailable.
