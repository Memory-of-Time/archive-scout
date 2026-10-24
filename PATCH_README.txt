Archive Scout v1.2.5 Error Recovery Patch

Base: archive-scout-main(20261024-162650).zip only.
Stop Scout, extract this ZIP, merge archive_scout/tests/docs into the source
repository root, accept file replacements, restart and Resume your project.
Do not flatten the folders or delete your project database/captures.
Standalone executable users need a normal rebuild from the patched source.

Read docs/V125_ERROR_RECOVERY_PATCH.md for exact changes, test evidence and
limits. In particular, the live digest failures and sustained live 8/s are
not claimed solved. The new diagnostics provide the evidence still needed.
