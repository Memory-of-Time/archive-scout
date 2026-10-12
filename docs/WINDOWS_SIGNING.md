# Windows signing

`Build All Platforms` supports Azure Artifact Signing as an optional step. Configure repository variable `ENABLE_WINDOWS_ARTIFACT_SIGNING=true`, variables `WINDOWS_SIGNING_ENDPOINT`, `WINDOWS_SIGNING_ACCOUNT_NAME`, `WINDOWS_SIGNING_PROFILE_NAME`, and secrets `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`.

The workflow signs the built `Scout.exe` and `ScoutCLI.exe`, verifies them, then creates release ZIPs. If signing is disabled, it creates unsigned ZIPs; disclose this accurately to users. Code signing does not guarantee that antivirus scanners will approve the file.

Check `Get-AuthenticodeSignature` and the release ZIP SHA-256 checksum before distributing a signed executable. Do not recommend disabling Defender or SmartScreen globally.
