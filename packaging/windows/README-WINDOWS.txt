Archive Scout for Windows

VERIFY THE DOWNLOAD
1. Download ArchiveScout-Windows-x64.zip and its matching .sha256 file from the official GitHub release.
2. In PowerShell, run Get-FileHash on the ZIP with SHA256.
3. Confirm the displayed hash matches the published checksum.
4. For an official signed release, open ArchiveScout.exe Properties > Digital Signatures and confirm Windows reports a valid signature from the published Archive Scout signer.

MARK OF THE WEB / UNBLOCK
Windows normally marks ZIP files downloaded from the internet. After verifying the source, checksum, and signature, right-click the ZIP, choose Properties, and select Unblock when that option is present before extracting it.

RUN WITHOUT INSTALLING
Open the extracted ArchiveScout folder and run ArchiveScout.exe for the desktop app.
For automation, run ArchiveScoutCLI\ArchiveScoutCLI.exe --help from a console.

OPTIONAL INSTALLER
The included Install Archive Scout.cmd copies both the GUI and automation CLI to the current user's local Programs folder and creates GUI shortcuts. It also creates archive-scout.cmd beside the installed GUI for scripted use. It does not require administrator access.

DEFENDER FALSE POSITIVES
Do not disable Defender globally. Record the detection, verify the checksum and signature, and submit the exact flagged release file to Microsoft as a clean-software false positive. A signature and checksum establish publisher/file integrity but cannot force an antivirus classification.

Microsoft Defender false-positive submission: https://www.microsoft.com/wdsi/filesubmission
