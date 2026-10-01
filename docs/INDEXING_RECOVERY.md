# Indexing recovery

Archive Scout persists CDX work so a single slow or failed interval does not require a full restart.

Broad targets can use page-based retrieval and other targets can use resume keys. Work state tracks target, date window, query signature, progress, resume key, completion state, and associated errors.

Transient failures can rotate network backends/endpoints and split the failed date interval into smaller windows. Successful sibling windows remain complete and are not repeated.

Connection-failure circuits prevent every queued item from independently repeating the same unavailable-network sequence. Rate-limit signals are shared through the request gate so concurrent workers stop issuing stale permits while recovery is in progress.

Permanent Wayback policy conditions are surfaced as site issues. Confirmed excluded targets can be skipped for remaining work rather than retried as ordinary timeouts.
