# Archive Scout automation contract

Archive Scout 1.0.2 ships a separate console executable (`ArchiveScoutCLI`) alongside the desktop GUI. Source installs expose the same interface as `archive-scout`.

## Commands

```text
archive-scout init project.json --output-dir ./research --target "example.com/*" --keyword video --format json
archive-scout run project.json --mode all --format jsonl
archive-scout status project.json --format json
archive-scout search project.json --query "google video" --format json
archive-scout results project.json --limit 500 --format jsonl
archive-scout errors project.json --format json
archive-scout ai-review project.json --query "find likely eyewitness references" --limit 50 --format json
archive-scout research project.json --query "where did this filename spread?" --format json
archive-scout research project.json --query "where did this filename spread?" --ai --format json
archive-scout research-index project.json --format jsonl
```

The legacy `archive-scout project.json --mode all` form remains accepted.

## Output formats
- `text`: human-readable console messages.
- `json`: one final JSON document on stdout. Progress is suppressed.
- `jsonl`: streaming JSON records. Progress records contain the same `stage`, `message`, `current`, `total`, and `detail` fields used by the GUI.

Diagnostics and unexpected failures are written to stderr. Never parse human-readable text when JSON/JSONL is available.

## Exit codes
- `0`: completed successfully.
- `1`: unexpected application failure.
- `2`: invalid arguments, missing project, or invalid configuration.
- `3`: safely deferred by network/rate-limit conditions. Queues remain resumable.
- `130`: interrupted by the user. Ctrl+C sets the shared stop event so in-flight work can save its queue state.

## Bot-safety notes
`status`, `search`, `results`, and `errors` are read-only. They open the project database with SQLite query-only mode. `search` operates on the already-built Research Intelligence index and does not contact an AI provider. `research --ai` and `ai-review` are the only CLI research commands that require external AI credentials.
