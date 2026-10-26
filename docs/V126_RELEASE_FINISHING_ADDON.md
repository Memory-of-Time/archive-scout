# v1.2.6 finishing add-on

This small add-on addresses confirmed indexing recovery costs and missing
diagnostics. Public version remains 1.2.6 and schema remains 11.

- Preserve a committed CDX resume cursor across ordinary timeout recovery.
  CDX ordering follows URL keys, so replacing its date range with fresh date
  windows can repeat previously fetched inventory. Evidenced response-size
  pressure can still reduce the page limit at that same cursor. After three
  failed turns, the existing eligible fallback remains available as an explicit
  last resort; an unusable cursor must not strand the remaining inventory.
  Recovery for untouched ranges remains available. Collapsed selection rules
  and incomplete checkpoint protection remain unchanged.
- Count each deferred indexing failure once. A threshold notice no longer
  increments the saved failure counter again or advances exponential backoff.
- Keep sparse CDX failure and committed-page timing notices in the existing
  bounded GUI notice queue. Show a bounded underlying exception reason before
  a long query can hide it; omit query URLs from that summary. Ordinary progress
  remains coalesced, and acquisition display settings retain their meaning.
- At the existing acquisition-health interval, record worker occupancy,
  configured spacing, queue-preparation time, and owner commit/publication
  timings in `diagnostics/text-acquisition/latest.json`. Owner commit includes
  writer-lock waiting. Missing timing samples remain unknown. No extra sampling
  task, per-capture file write, network request or queue is introduced.
- Include the earlier Windows analysis-fixture correction: write the exact
  UTF-8 bytes represented by the recorded hash. Production evidence checks stay
  intact; LF/CRLF integrity regression coverage is included.

No inventory membership, collapse policy, text/media routing, domain extraction,
user research settings, healthy request pacing or completion checks change.
Rejecting invalid responses and retaining their unfinished work remain required.

The activity at 700,000 LiveLeak rows still lacks its underlying validation
reason. This add-on makes that reason visible; it does not establish or promise
a fix for an unobserved service response. Likewise, the later zero-error run at
approximately 3.3 saves/s lacks enough stage evidence to justify changing worker
counts or delays. The added health fields support that next diagnosis.

Folder names do not select targets. The uploaded final activity used
`*.gamerrade.com/*` even when its output folder was named `911archive.net`.
Check the selected target if that was unintended. Automatic Research
Intelligence work remains a separately configured feature and is not required
to prove acquisition completeness.

Native Windows and overnight live Wayback performance remain unverified by
local fixture tests. The delivered validation records identify the checks run.
