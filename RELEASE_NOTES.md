# Archive Scout v1.2.3 — organized release

This repository is a **rebranded, cleaned release of the supplied later Scout v1.2.4 implementation**. Its public version is 1.2.3 by request. This does **not** undo the previously implemented acquisition, indexing, scanning, recovery, repair, merge, or report behavior.

## Packaging and maintenance

- Public application title, release ZIP/tar names, Windows/macOS/Linux installer labels and executable names now use Archive Scout.
- Python package `archive_scout`, database schema 9, project configuration and files, operation codes, legacy `scout` command aliases, and existing keyword/scan semantics are preserved.
- Removed historical patch manifests, hotfix READMEs, obsolete checksum listings, and one-time CI repair scripts from the distributable source.
- Source, tests, build scripts, current documentation and GitHub Actions workflows remain included.

## Existing features inherited from the baseline

Durable retries, identity-bound streamed text bodies, the independent SQLite-backed scanner, evidence hashing, repeatable project merge identity maps, bounded diagnostics, collapse-safe CDX indexing recovery, and previous safety fixes remain present. No changes to these algorithms are intended by this maintenance release.

## Upgrade note

Stop Archive Scout, take a project backup, and replace **application files only**. Reopen the existing project. A change of public version from the baseline's internal 1.2.4 identifier to 1.2.3 should not be interpreted as a historical downgrade or as a schema migration. On platforms with a previous `Scout` application installed, the new app executable/installer has a different product name; older binaries may need to be removed separately. Do not delete research project directories.

## Verification scope

`python scripts/run_tests.py`, `python scripts/verify_release.py`, and CI workflow definitions are the release gates. Local tests cannot establish native installer success or long-duration live Wayback stability until GitHub-hosted runners and real-service tests execute.
