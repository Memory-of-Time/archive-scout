#!/usr/bin/env bash
set -euo pipefail
PACKAGE_DIR="$(cd "$(dirname "$0")" && pwd)"
GUI_SOURCE="$PACKAGE_DIR/ArchiveScout"
CLI_SOURCE="$PACKAGE_DIR/ArchiveScoutCLI"
DEST_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/archive-scout"
BIN_DIR="$HOME/.local/bin"
APPLICATIONS_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/applications"
mkdir -p "$DEST_DIR" "$BIN_DIR" "$APPLICATIONS_DIR"
rm -rf "$DEST_DIR/ArchiveScout" "$DEST_DIR/ArchiveScoutCLI"
cp -R "$GUI_SOURCE" "$DEST_DIR/ArchiveScout"
cp -R "$CLI_SOURCE" "$DEST_DIR/ArchiveScoutCLI"
ln -sf "$DEST_DIR/ArchiveScoutCLI/ArchiveScoutCLI" "$BIN_DIR/archive-scout"
cat > "$APPLICATIONS_DIR/archive-scout.desktop" <<DESKTOP
[Desktop Entry]
Name=Archive Scout
Comment=Wayback Machine archive research tool
Exec=$DEST_DIR/ArchiveScout/ArchiveScout
Terminal=false
Type=Application
Categories=Utility;Education;
DESKTOP
chmod +x "$APPLICATIONS_DIR/archive-scout.desktop"
printf 'Archive Scout was installed. The GUI is in your application menu and the automation CLI is %s/archive-scout.\n' "$BIN_DIR"
