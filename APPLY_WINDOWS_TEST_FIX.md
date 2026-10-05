# Archive Scout v1.0.6 Windows test fix

Apply this small patch AFTER the v1.0.6 patch files already provided.

Replace this repository file, preserving its path:

- tests/unit/test_v106_connection_stability.py

The application version stays 1.0.6.

## What this fixes

The curl header test wrote explicit HTTP CRLF line endings through Python
text mode. On Windows, text-mode newline translation inserted extra carriage
returns, incorrectly turning each header line into a complete header block
and causing the assertion failure reported in the Windows Python 3.12 log.

The fixture now writes and appends exact binary HTTP header bytes, preserving
the original assertions for incomplete headers, proxy headers, HTTP 429, and
Retry-After. Application source and runtime behavior are unchanged.

## Validation

- Reproduced the exact reported failure by enabling Windows text-mode newline
  conversion on the previous test file.
- Confirmed the corrected test passes with that conversion enabled.
- Full local suite: 411 tests, 410 passed, 1 skipped, 0 failures.
- Source, tests, and scripts compile check passed.

A native Windows runner was not available locally. Commit the replacement
file and let GitHub Actions rerun its Windows jobs.
