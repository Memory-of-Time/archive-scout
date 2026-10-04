# Development

Archive Scout supports Python 3.11 and newer. The current public package version is 1.0.4.

## Principles

- Preserve project compatibility unless a schema migration is deliberate and tested.
- Keep network concurrency bounded and request starts rate-limited.
- Keep SQLite access on the owning thread/connection.
- Avoid project-sized Python lists when a persistent queue or keyset iterator can be used.
- Prefer atomic writes for user-visible reports and configuration.
- Never persist API keys in project data or ordinary application settings.
- Treat archived HTML as untrusted input.
- Keep AI analysis separate from deterministic scoring and human review.
- Do not bypass Wayback exclusions or robots restrictions.

## Validation

Before release:

- compile source/tests;
- run the complete unittest suite;
- run offline benchmark smoke tests;
- validate both GitHub workflow YAML files;
- verify packaging scripts;
- ensure no generated build, bytecode, egg-info, cache, credential, or local-project files are present;
- build and smoke-test native packages on each platform.

The Tests workflow covers Linux, Windows, Intel macOS, and Apple Silicon macOS on Python 3.11 and 3.12.
