# Changelog

## Scout 1.2.1 — Operations and reliability (patch)

- Restored index/download-only and acquisition-only retry operations, project safety and import portability.
- Corrected bounded error recovery, embedded media filtering and the Media-page layout.
- Added passive stall diagnostics and a default-off experimental host recovery option.
- On-demand media integrity/hash verification, WAL-safe restoration and project operation leases.
- CDX outages no longer shrink resume windows/page sizes or churn already-fragmented queues; genuine size-pressure recovery retained.
- Endpoint fallback diagnostics now include cause, elapsed time and selected next service.
- Schema-13 migration and retired historical scanning controls remain deferred pending compatibility/performance validation.

## Scout 1.2.0 — New public era

- Rebranded the desktop app, CLI, installers and release packages as Scout.
- Preserved the validated 1.2.4 indexing, resilient acquisition, local scanning, reports and schema-9 project formats.
- Consolidated source repository; removed superseded patches, test output and old audit attachments.
- Added three stable, OS-specific latest-release links backed by the release workflow.
- Retained the `archive_scout` Python import path and legacy CLI aliases to reduce migration risk.

The version reset is a branding decision, not a rewrite of the downloader. See `RELEASE_NOTES.md`.
