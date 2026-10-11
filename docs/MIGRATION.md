# Project migration and compatibility

Scout 1.2.0 uses **database schema 9**, inherited unchanged from the validated Archive Scout 1.2.4 development release. The public version reset does not reset the database schema.

Scout retains the existing migration code for supported older schemas and creates a safety copy before migrations where supported. Always make an independent backup of valuable projects.

**Important:** historical schema-13 projects are **not** supported by this release. If you see `unsupported Archive Scout schema version: 13` (or the Scout equivalent), do not edit SQLite `user_version`, rename database files, or open the project with the assumption that it has migrated. Use a compatible historical build or wait for a tested migration path.

Upgrading Scout software or moving project folders must not reset durable indexing, download, or scanner state. The project folder, not the application installation directory, owns that state.

The Python import path remains `archive_scout` and existing CLI aliases are preserved. Windows/macOS/Linux executable names have changed to Scout and ScoutCLI. The published package version is reset to 1.2.0; when replacing an older 1.2.4 installation from source, use a reinstall rather than relying on package-manager version comparison.
