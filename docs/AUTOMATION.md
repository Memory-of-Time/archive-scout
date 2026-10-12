# Scout automation interface

The public CLI is `scout` (or the bundled `ScoutCLI` executable). `archive-scout` is retained as a compatibility alias. JSON/JSONL output is intended for scripts and integrations; progress diagnostics must go to stderr.

```bash
scout --help
scout init project.json --output-dir ./research --target "example.com/*" --keyword video --format json
scout run project.json --mode all --format jsonl
scout status project.json --format json
scout search project.json --query "google video" --format json
scout results project.json --limit 500 --format jsonl
scout errors project.json --format json
scout research-index project.json --format jsonl
```

Other operations, including resumable Hitlist, media, repair, and backups, are described by `scout --help`. Retain CLI argument compatibility when modifying commands.
