# Archive Scout v1.1.1 patch files

Apply to your existing **v1.1.0 source with the previously delivered CI fix**. This ZIP is a separate follow-up patch; the prior v1.1.0 ZIPs remain unchanged. It contains 43 changed/new repository files plus independent test evidence. It includes the corrected release verifier too. Project schema stays 13; no databases or project files are replaced.

1. Extract this ZIP into a new folder.
2. Copy the listed repository files into your Archive Scout source, preserving their paths. Keep `.github/workflows/` and `github/workflows/` in their respective directories. `PATCH_MANIFEST.json` lists every replacement. Alternatively, use the optional verifier below for hash checks, automatic copying and a source-file backup.
3. Upload the changed repository files to GitHub. The **Tests** workflow runs on a push to `main`, pull request, or manual dispatch, and uploads its logs and summary. The build workflow produces the GUI and CLI packages. Rebuild the application to use these source changes; an existing v1.1.0 executable does not acquire the fix from source files alone.
4. Keep **Adaptive rate limiting (experimental — still in testing)** off to use fixed request pacing and headerless 429/503 recovery waits of at most five seconds. Normal replay scheduling stays at eight starts per second. Explicit server Retry-After and genuine connection recovery still apply.

Optional verified application from the extracted patch folder:

```powershell
py -3 scripts/verify_patch.py --project "C:\path\to\archive-scout" --apply
```

The helper requires Python 3.11+, validates all files before writing, accepts known Git line-ending/BOM conversions, blocks meaningful local edits, backs up replaced source, and supports repeated application. It never opens a project database. Its manifest supports the delivered v1.1.0 base, including the CI add-on. Keep this guide and `validation/` evidence separately if you only upload repository code; they are not required at runtime.

Validation: **598 tests passed locally**, zero failures/errors/skips, on a fresh patch-applied source tree. Compilation, installed release metadata, workflow placement, dependency checks, spawned CLI scans, offline benchmark and virtual v1.0.5/current pacing comparison passed. Test logs and reports are included. Hosted CI has not been run here.

New logs identify server-requested waits and app fallback waits. Newly saved optional cooldowns can be shortened when opting out. A wait saved by older versions without this distinction is preserved because it could be a server deadline. The actual cause of connection failures remains unconfirmed; live saved-download speed is not guaranteed by virtual scheduling checks.
