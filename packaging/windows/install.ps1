$ErrorActionPreference = "Stop"
$Source = Join-Path $PSScriptRoot "Scout"
$CLISource = Join-Path $PSScriptRoot "ScoutCLI"
$Destination = Join-Path $env:LOCALAPPDATA "Programs\Scout"
if (-not (Test-Path $Source)) { throw "Scout application folder was not found." }
if (-not (Test-Path $CLISource)) { throw "ScoutCLI application folder was not found." }
if (Test-Path $Destination) { Remove-Item $Destination -Recurse -Force }
New-Item -ItemType Directory -Path $Destination -Force | Out-Null
Copy-Item "$Source\*" $Destination -Recurse -Force
$CLIDestination = Join-Path $Destination "ScoutCLI"
Copy-Item $CLISource $CLIDestination -Recurse -Force
$CLIWrapper = Join-Path $Destination "scout.cmd"
$CLIWrapperText = "@echo off`r`n`"%~dp0ScoutCLI\ScoutCLI.exe`" %*`r`n"
$CLIWrapperText | Set-Content $CLIWrapper -Encoding ascii
$CLIWrapperText | Set-Content (Join-Path $Destination "archive-scout.cmd") -Encoding ascii
$Shell = New-Object -ComObject WScript.Shell
$DesktopShortcut = $Shell.CreateShortcut((Join-Path ([Environment]::GetFolderPath("Desktop")) "Scout.lnk"))
$DesktopShortcut.TargetPath = Join-Path $Destination "Scout.exe"
$DesktopShortcut.WorkingDirectory = $Destination
$DesktopShortcut.Save()
$StartDirectory = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\Scout"
New-Item -ItemType Directory -Path $StartDirectory -Force | Out-Null
$StartShortcut = $Shell.CreateShortcut((Join-Path $StartDirectory "Scout.lnk"))
$StartShortcut.TargetPath = Join-Path $Destination "Scout.exe"
$StartShortcut.WorkingDirectory = $Destination
$StartShortcut.Save()
$UninstallShortcut = $Shell.CreateShortcut((Join-Path $StartDirectory "Uninstall Scout.lnk"))
$UninstallShortcut.TargetPath = "powershell.exe"
$UninstallShortcut.Arguments = "-NoProfile -File `"$(Join-Path $Destination 'uninstall.ps1')`""
$UninstallShortcut.WorkingDirectory = $Destination
$UninstallShortcut.Save()
Copy-Item (Join-Path $PSScriptRoot "uninstall.ps1") $Destination -Force
Write-Host "Scout was installed successfully."
