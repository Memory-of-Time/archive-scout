#!/usr/bin/env bash
set -euo pipefail
PACKAGE_DIR="$(cd "$(dirname "$0")" && pwd)"
GUI_SOURCE="$PACKAGE_DIR/Scout"
CLI_SOURCE="$PACKAGE_DIR/ScoutCLI"
DEST_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/scout"
BIN_DIR="$HOME/.local/bin"
APPLICATIONS_DIR="${XDG_DATA_HOME:-$HOME/.local/share}/applications"
mkdir -p "$DEST_DIR" "$BIN_DIR" "$APPLICATIONS_DIR"
rm -rf "$DEST_DIR/Scout" "$DEST_DIR/ScoutCLI"
cp -R "$GUI_SOURCE" "$DEST_DIR/Scout"
cp -R "$CLI_SOURCE" "$DEST_DIR/ScoutCLI"
ln -sf "$DEST_DIR/ScoutCLI/ScoutCLI" "$BIN_DIR/scout"
ln -sf "$DEST_DIR/ScoutCLI/ScoutCLI" "$BIN_DIR/archive-scout"
cat > "$APPLICATIONS_DIR/scout.desktop" <<DESKTOP
[Desktop Entry]
Name=Scout
Comment=Wayback Machine archive research tool
Exec=$DEST_DIR/Scout/Scout
Terminal=false
Type=Application
Categories=Utility;Education;
DESKTOP
chmod +x "$APPLICATIONS_DIR/scout.desktop"
printf 'Scout was installed. The GUI is in your application menu and the automation CLI is %s/scout.\n' "$BIN_DIR"
