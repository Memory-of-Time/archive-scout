# Archive Scout 1.0.6

This release addresses connection churn and repeated transfers identified in the connection-prevention audit. Apply the patch files over the prepared v1.0.5 repository, preserving their relative paths. Existing projects remain on schema 12; no database migration or capture relocation is needed.

The release requalifies recovered pooled backends without concurrent probe storms, separates response failures from connection setup, preserves small partial bodies on all transports, and handles curl throttle/redirect headers while transfers are running. Early binary validation also applies to curl; resumed suffixes are classified using the saved file prefix, not as independent new file headers.

Text-acquisition backoff is now coordinator-scheduled so a waiting retry releases its worker. Queues stay bounded, ordinary attempt budgets are retained, and pending/partial state survives cancellation. Existing automatic shared-host recovery, server deadlines, atomic final files, text/media routing, local scanning and Hitlist checking remain intact.

Additional fixes bound urllib3 pool acquisition and prevent an unavailable allowed external redirect destination from being mistaken for a common Wayback connection outage. Curl local file-write failures surface as storage errors. There are no new runtime dependencies or changes to request ceilings. Media uses the improved shared transports and its existing retry scheduler.

Validation covers stalled 429/503 bodies, archived origin 429, proxy/interim headers, binary-prefix rejection, valid Range resumption on every backend, isolated read/reset failures, scoped backend health, concurrent requalification, stale candidates, late curl completion, redirect policy, bounded pool waits, fresh work during delayed retry, retry limits and cancellation preserving partial files. Exact test and benchmark results are recorded in validation-1.0.6.json.

The controlled tests use mocks and loopback HTTP only. This release does not claim a measured live Wayback speedup or an eight-successful-files-per-second guarantee. Native OS package builds are performed by the repository workflows; local verification here covers source tests, compilation and the offline benchmark.
