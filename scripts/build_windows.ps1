param(
    [switch]$SkipBuild,
    [switch]$SkipPackaging,
    [switch]$RequireSigned
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

$Executable = Join-Path $PWD "dist\ArchiveScout\ArchiveScout.exe"
$CLIExecutable = Join-Path $PWD "dist\ArchiveScoutCLI\ArchiveScoutCLI.exe"

if (-not $SkipBuild) {
    Remove-Item build,dist -Recurse -Force -ErrorAction SilentlyContinue
    if (-not $SkipPackaging) {
        Remove-Item release -Recurse -Force -ErrorAction SilentlyContinue
    }
    python -m PyInstaller `
        --noconfirm `
        --clean `
        --windowed `
        --onedir `
        --noupx `
        --name ArchiveScout `
        --icon assets/archivescout.ico `
        --version-file packaging/windows/version_info.txt `
        --manifest packaging/windows/ArchiveScout.manifest `
        --add-data "assets/archivescout.png;assets" `
        --add-data "assets/archivescout.ico;assets" `
        --collect-all truststore `
        --collect-all urllib3 `
        --collect-all httpx `
        --collect-all httpcore `
        --collect-all dotenv `
        --collect-all selectolax `
        --collect-all ahocorasick_rs `
        run_app.py
    python -m PyInstaller `
        --noconfirm `
        --clean `
        --console `
        --onedir `
        --noupx `
        --name ArchiveScoutCLI `
        --version-file packaging/windows/version_info.txt `
        --manifest packaging/windows/ArchiveScout.manifest `
        --collect-all truststore `
        --collect-all urllib3 `
        --collect-all httpx `
        --collect-all httpcore `
        --collect-all dotenv `
        --collect-all selectolax `
        --collect-all ahocorasick_rs `
        run_cli.py
}

if (-not (Test-Path $Executable)) {
    throw "ArchiveScout.exe was not built at $Executable"
}
if (-not (Test-Path $CLIExecutable)) {
    throw "ArchiveScoutCLI.exe was not built at $CLIExecutable"
}

if ($RequireSigned) {
    foreach ($Target in @($Executable, $CLIExecutable)) {
        $Signature = Get-AuthenticodeSignature $Target
        $Signature | Format-List Status,StatusMessage,SignerCertificate,TimeStamperCertificate,Path
        if ($Signature.Status -ne "Valid") {
            throw "$Target must have a valid Authenticode signature before packaging. Current status: $($Signature.Status)"
        }
    }
}

if ($SkipPackaging) {
    Write-Host "Windows application files built; packaging deferred until after signing."
    exit 0
}

New-Item -ItemType Directory -Path release -Force | Out-Null
$Package = Join-Path $PWD "release\ArchiveScout-Windows-x64"
Remove-Item $Package -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Path $Package | Out-Null
Copy-Item dist\ArchiveScout $Package\ArchiveScout -Recurse
Copy-Item dist\ArchiveScoutCLI $Package\ArchiveScoutCLI -Recurse
Copy-Item packaging\windows\install.ps1 $Package
Copy-Item 'packaging\windows\Install Archive Scout.cmd' $Package
Copy-Item packaging\windows\uninstall.ps1 $Package
Copy-Item 'packaging\windows\Uninstall Archive Scout.cmd' $Package
Copy-Item packaging\windows\README-WINDOWS.txt $Package
Copy-Item README.md $Package

$Zip = Join-Path $PWD "release\ArchiveScout-Windows-x64.zip"
Remove-Item $Zip -Force -ErrorAction SilentlyContinue
Compress-Archive -Path "$Package\*" -DestinationPath $Zip -CompressionLevel Optimal
$Hash = (Get-FileHash $Zip -Algorithm SHA256).Hash.ToLower()
"$Hash  ArchiveScout-Windows-x64.zip" | Set-Content "release\ArchiveScout-Windows-x64.zip.sha256" -Encoding ascii
