# Indexing recovery

Archive Scout persists CDX traversal evidence, including target scope, date bounds, selection/collapse identity, cursor/page checkpoints, and incomplete/error state. A saved cursor is only advanced after the associated rows and validation evidence are durably committed.

A persistently failing index scope receives a progressively increasing persisted cooldown. Other eligible scopes can progress without cycling continuously through unsuccessful years. Endpoint aliases at the same Wayback host are not independent providers, and gateway 502/503/504 responses must not trigger a redundant same-host request cascade. Host-wide genuine rate limits are distinguished from query-specific errors.

An unresolved collapsed query **must not** be split into different date scopes or silently converted to numbered pages merely to obtain an HTTP response; that could change the selected representative capture. A transition to alternate paging is permitted only when equivalent selection and terminal evidence have been established. An exhausted scope stays visibly incomplete, even when the report writer succeeds.

Read-only cached inventory and saved-body scanning may continue during an index outage. v1.2.5 requires every selected initial index scope to be committed complete before any *new* replay acquisition is admitted. A deferred or blocked index does not authorize partial-scope downloading. This intentionally may delay the first download but adds no inventory-wide query per successfully acquired capture.
