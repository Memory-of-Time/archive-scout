# v1.3.0 URL filename add-on

Apply after `ArchiveScout-v1.3.0-Simple-Recovery-Addon.zip`. This is incremental,
not cumulative. Version stays **1.3.0**, schema **12**. No UI/settings changes.

New text and media saves use the original URL as the filename label, including
the host, path and query. URL characters unsafe in portable filenames are
percent-escaped. For example, `https://example.org/page?id=1` becomes a name
starting `https%3A%2F%2Fexample.org%2Fpage%3Fid=1`. An original percent sign is
encoded as `%25`, so existing URL escapes can be distinguished from filename
escaping. This is the original captured URL, not the final replay/redirect URL.

The label is followed by the capture timestamp, capture ID and a stable hash.
Text keeps `.txt`; media keeps its selected/inferred extension. Host/path/query,
case differences and different captures of the same URL receive distinct names.
Names are capped at 140 ASCII characters to leave room for transfer, receipt
and collision suffixes. Long URL labels are truncated; their hashes still bind
the complete URL/timestamp/capture identity. Exact URLs remain in project records
and the existing configured reports. A shortened label cannot reconstruct a
long URL by itself. This is not an arbitrary-length Windows path guarantee.

Names are independent of project-root spelling/length, so moving a project does
not change its expected new filenames. Directory structure remains unchanged:
text under `captures/YYYY/MM`, media under `media/images` or `media/videos`.
No extra permanent per-file metadata sidecars, project-wide filesystem scan,
bulk rename, new cache, query, index or database migration is added.

Existing files are kept where they are. Text path lookup recognizes the old
ID/SHA-1 filename, its `.transfer` stage and its `.validated.json` receipt. Media
uses a recorded existing path and can recover an older flat basename or its
collision variant after checking the same CDX SHA-1/committed SHA-256 proof as
before. Unknown/unrelated old bytes are never accepted on name alone. Such
files are retained, while a new requested capture uses its URL filename.
These compatibility checks use bounded deterministic candidates, not directory
enumeration. Legacy names may therefore remain for reused or resumed captures.

Long quarantine names use a compact original-name identity plus the full body
hash, retaining questionable bytes without exceeding component limits. Retained
content, digest checks, receipts and database success/checkpoint ordering are
unchanged. Media collision safeguards remain active.

Stop Scout, merge the ZIP's `files/` contents into the source tree after the
previous add-on, then rebuild or run the updated source. Reopen the same project.
**Delete nothing.** Source edits do not update an existing executable by themselves.

The delivered validation evidence includes the full regression suite, clean
patch application, focused URL/collision/legacy recovery tests, release metadata
checks, the required offline benchmark and a wheel install/payload/CLI check.
Tests cover Unicode, query strings, percent escapes, Windows-invalid characters,
device names, very long URL labels, relocation, old recovery files, verified
media reuse, unrelated legacy bytes and exact text publication. Live Wayback,
native Windows/macOS builds, million-file/day-long runs and sustained eight
validated saves per second were not tested for this filename-only add-on.
