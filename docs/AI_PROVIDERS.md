# AI providers

AI is optional post-processing. Downloading, CDX indexing, deterministic scans, local Research Intelligence retrieval, review state, and reports continue to work without an API key.

## Providers
Archive Scout uses one internal provider interface with separate adapters for OpenAI and OpenRouter. OpenAI uses the Responses API with strict structured output. OpenRouter uses its chat-completions-compatible endpoint and validates/parses the requested JSON object locally.

Project files may store provider and exact model names for reproducibility. Credentials are never stored in project files or SQLite.

## Credentials
For source/developer use, copy `.env.example` to `.env` and set one key. Real process environment variables take precedence over `.env` values.

```text
AI_PROVIDER=openai
AI_MODEL=gpt-5-mini
OPENAI_API_KEY=
OPENROUTER_API_KEY=
```

Packaged builds intentionally do not search for a colocated `.env`. Supply credentials through the process environment or the GUI's session-only API-key field.

Never put a key in `project.json`, settings, logs, reports, diagnostics, issue reports, or command-line arguments.

## Research integrity
Archived text is inserted only as untrusted evidence. Model instructions explicitly forbid following commands contained in archived pages. Grounded Research Intelligence answers are restricted to supplied evidence document IDs, and claims without valid evidence IDs are discarded before persistence.
