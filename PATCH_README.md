# Archive Scout v1.3.1 source patch

Apply this patch to `archive-scout-main(20261117-225344).zip`.

1. Stop Archive Scout and extract a clean copy of the supplied repository.
2. Extract the patch ZIP into the repository root containing `pyproject.toml`, replacing the matching files.
3. Run `python -m pip install .` for a source installation, or rebuild your platform executable with the existing release workflow.
4. Open your existing project normally. The main project remains on SQLite schema 12. Existing inventory does not require a reindex.

The ZIP contains changed and new source files only. It does not include a replacement executable or any project data. Read [Fast URL discovery and large project resume](docs/V131_DISCOVERY_AND_RESUME.md) for usage, limits, and validation.
