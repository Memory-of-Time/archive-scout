# Archive Scout v1.2.4 — official source release

The official v1.2.4 repository consolidates the October 18 indexing recovery source with the operation/report handoff audit. It retains the `archive_scout` package name, legacy CLI aliases, schema-9 project files, historical captures, scan results and healthy acquisition pacing.

**Integration corrections:** Resume preserves direct or external supplemental-media configuration; incomplete work is surfaced consistently through GUI and CLI instead of being re-labeled complete; unfinished scans prevent media handoff; and report failures cannot revoke already completed scan scores. Media download/retry statuses reflect unresolved selected captures.

**Reports:** The Reports tab offers **Write report now** during active or idle operations. A lightweight, read-only SQLite WAL snapshot is rendered in a background thread to a unique `reports/manual/` folder. It does not take the main project lease, start acquisition/scanning, clear Stop, overwrite normal latest exports, or mutate operation/scan statuses. Interrupted/failed manual exports are discarded without publishing a partial report. Media report file and field selections are honored.

**Organization:** Superseded patch-install documents and historical one-off validation JSON were removed. Source modules, compatibility tests, packaging/installers, project recovery documentation and platform workflows are retained.

**Measured local verification:** The canonical suite and release verifier pass from the packaged repository; the unchanged healthy localhost acquisition/scan path is measured separately. This does **not** establish hosted Windows/macOS CI results, production Wayback throughput, a 72-hour endurance guarantee, full cross-page `collapse=urlkey` reconciliation or all-region power-failure durability. These open requirements remain listed in `docs/RELEASE_CHECKLIST.md`.
