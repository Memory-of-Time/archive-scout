# Archive Scout 1.1.1 validation

The assembled patch was applied to a fresh copy of the previously delivered v1.1.0 source with its CI add-on. **598 tests passed**, with zero failures, errors or skips, on Windows Python 3.12.14. This includes 23 new cooldown/HTTP/stop/resume tests and two additional source-patch verification regressions. The complete test log and JSON report accompany the ZIP.

Installed package metadata, source/Windows release identity, canonical/mirrored workflow placement, compilation, dependency consistency and spawned CLI encoding-stress scans passed. The release verifier retains LF/CRLF/BOM tolerance while still rejecting real workflow differences.

The real current fixed scheduler and the inspected v1.0.5 scheduler each passed two million virtual healthy admissions at exactly 0.125-second spacing. Current scheduling also passed two million admissions with injected server waits, explicit server-eligibility and post-recovery no-burst checks, opt-in/off samples and headerless fixed fallback. The 100,000-row offline benchmark passed with zero unnecessary unchanged CDX writes. These fixtures make no live throughput claim.

Patch preflight, source backup, apply and idempotence passed. Replacement bytes match the tested source; a sentinel database remained untouched. The helper accepts known LF/CRLF/BOM variants and blocks meaningful local changes. No project-schema migration is required.

The Tests workflow installs the package, checks metadata, runs the full suite, verifies release identity and spawned scans, runs benchmark smoke checks, and uploads validation logs even on failure. Workflow version assertions are 1.1.1 in both canonical and mirrored copies. Hosted CI was not triggered here; its cross-platform results are not claimed.

An initial validation pass exposed a stale Windows-version assertion and an intermittent parallel directory-creation PermissionError. The assertion and coordinator directory preparation were corrected before the successful final suite.
