# AI relevance review

AI relevance helps prioritize the matches already produced by a normal Archive Scout scan.

## What it does

Choose a completed scan and describe the research goal in plain language. Archive Scout selects a bounded candidate set from the existing report, sends compact evidence for those matches to the OpenAI Responses API, and stores an independent relevance ranking.

Each result contains:

- relevance score from 0 to 100;
- confidence from 0 to 1;
- short category;
- concise reason;
- paraphrased evidence summary;
- link back to the original Archive Scout match.

AI relevance never changes the deterministic match score or human review record.

## Candidate selection

Prompt terms are used with the project full-text index when available. Prompt-relevant report matches get first consideration, then the remaining candidate budget is filled from the ordinary Archive Scout score order. Pages that were not matches in the selected scan are not silently promoted into the report by AI review.

## Security and prompt injection

Archived page text is untrusted source material. The API instructions explicitly tell the model not to obey instructions embedded in pages and to judge only relevance to the researcher's goal. Structured output is validated against the exact match IDs supplied in each batch; incomplete or mismatched batches are rejected.

## API key

Archive Scout does not include an OpenAI API key. A key entered in the AI page is kept only in memory for the session and is not written into the project or application settings. `OPENAI_API_KEY` can also be supplied by the launch environment.

## Data sent

AI review sends the research prompt and a bounded representation of selected report matches: URL, timestamp, title, Archive Scout score, keyword hits, matching snippets, and a limited page excerpt. It does not upload the entire project database or project folder.

Normal Archive Scout features do not call OpenAI.

## Reports

Each AI run produces CSV, JSON, and Markdown reports under the project reports directory. Previous AI runs remain selectable from the interface.
