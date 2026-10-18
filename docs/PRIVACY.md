# Privacy and external services

Normal Archive Scout indexing and download operations communicate with the Internet Archive Wayback Machine and the archived URLs necessary to retrieve public captures.

AI relevance is optional and is the only built-in feature that sends report evidence to OpenAI. Archive Scout sends only the selected research prompt and bounded candidate evidence for that AI run; it does not send the complete project database.

An OpenAI API key typed into the UI is held for the current session and is not stored in `project.json` or normal application settings. The application sends AI requests with `store` disabled. OpenAI's API data controls and retention terms are controlled by the user's OpenAI project/account and may change independently of Archive Scout; researchers handling sensitive material should review the current OpenAI API data controls before using AI relevance.

Diagnostic exports intentionally omit raw project targets, keyword rules, executable paths, and raw error content where the existing diagnostic design can summarize them instead.
