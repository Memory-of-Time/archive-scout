# Project migration

Archive Scout 1.0 uses database schema version 6.

Supported earlier Archive Scout project databases are upgraded automatically when opened with migration enabled. Before a schema upgrade, Archive Scout attempts to create a project safety backup.

Schema 6 adds tables for:

- AI relevance runs and results;
- persistent external-media discovery state;
- site-specific Wayback issue tracking.

Existing captures, documents, keyword sets, scan runs, matches, reviews, media captures, forum/analysis data, backups, errors, and recovery state are preserved.

Very old legacy project layouts are imported into the modern project structure through the existing migration path. Important projects should still be copied or backed up before any software upgrade.
