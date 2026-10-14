# Project migration and compatibility

Archive Scout v1.2.3 uses **database schema 9**. The supplied source baseline used the same schema under its internal v1.2.4 version string. The requested public rebrand/version change does not rewrite, upgrade, or downgrade existing project databases.

Always back up valuable project folders before upgrading. Stop an active operation and preserve the complete project folder, including SQLite WAL/SHM files if present and the raw capture/media directories. Reopen the **same** `project.json` after installing the new app; do not restart by creating a new project.

Historical schema-13 projects are **not supported**. Do not edit `schema_info`, rename a database, or pretend a legacy schema has migrated. The supported older schema migration paths are built into the application and should be allowed to run with a safety backup.

The Python import path `archive_scout`, CLI command aliases `scout` and `scout-gui`, and project filenames remain unchanged. User-facing application binaries are now `ArchiveScout.exe`, `Archive Scout.app`, and `ArchiveScout` on Linux, with `ArchiveScoutCLI` for bundled command-line executables. Previously installed applications may coexist until manually uninstalled. Existing GUI settings stored in a legacy `Scout` application-data location remain usable.
