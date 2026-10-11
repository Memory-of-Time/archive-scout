$ErrorActionPreference = "SilentlyContinue"
$Destination = Join-Path $env:LOCALAPPDATA "Programs\Scout"
$DesktopShortcut = Join-Path ([Environment]::GetFolderPath("Desktop")) "Scout.lnk"
$StartDirectory = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\Scout"
Remove-Item $DesktopShortcut -Force
Remove-Item $StartDirectory -Recurse -Force
Start-Process powershell.exe -ArgumentList "-NoProfile -Command Start-Sleep -Seconds 2; Remove-Item -LiteralPath '$Destination' -Recurse -Force" -WindowStyle Hidden
Write-Host "Scout was uninstalled. Research project folders were not removed."
