# Scout 1.2.0 release notes

This public-release source snapshot is based on the validated Archive Scout 1.2.4 application with no intentional changes to CDX indexing, replay download scheduling, scanning, or the database schema. The new version number identifies the Scout rebrand.

**Compatibility:** existing schema-9 projects remain supported. Never manually change a database schema version. Historical schema-13 databases remain unsupported. Make a backup before opening an older project.

**Distribution:** the GitHub Actions release workflow builds Windows x64 ZIP, macOS Universal ZIP, and Linux x64 tar.gz, then publishes them on a tag such as `v1.2.0`. The top README links will function only after the artifacts actually exist. Windows signing is optional but recommended and configurable using Azure credentials.

**Validation:** local suite, compilation, release-link checks, source packaging integrity, and offline loopback checks are executed during preparation. Actual hosted Windows/macOS builds and long-running live Wayback throughput must be independently checked before announcing general availability.
