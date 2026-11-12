# Project recovery

Archive Scout is designed so interrupted work remains inspectable and resumable.

When a project is reopened after an unclean shutdown, captures/media left in a downloading state are returned to pending, running scans from a previous process are marked interrupted, and stale operation runs are closed. An active worker in the current process is not incorrectly marked crashed just because a second project connection is opened by the UI.

The application also provides project backup, integrity checking, repair, diagnostic export, and selective retry tools. Repair and migration use safety backups where possible.

Startup failures in packaged builds are recorded to a startup error log; macOS also attempts to display a native alert pointing to that log.


v1.3.0 also rebuilds exact dashboard summaries during explicit Repair. A fatal local scanner-supervisor failure stops further acquisition; validated files and pending work remain recoverable. Inspect the reported scanner error before resuming. Optional diagnostics retain `history.jsonl` plus two rotated files, at most 4 MiB each per text/media diagnostic stream, sampled no more often than every five minutes. `latest.json` remains the short-interval snapshot. Turning off error messages in Activity does not disable this evidence or database error records.
