#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

rm -rf build dist release
export MACOSX_DEPLOYMENT_TARGET="12.0"

module_is_universal2() {
  local module="$1"
  local package_dir
  package_dir="$(python - "$module" <<'PYMODULE'
import importlib, pathlib, sys
module = importlib.import_module(sys.argv[1])
path = pathlib.Path(module.__file__).resolve()
print(path if path.suffix in {".so", ".dylib"} else path.parent)
PYMODULE
)"
  local found=0
  local binary archs
  while IFS= read -r binary; do
    found=1
    archs="$(lipo -archs "$binary" 2>/dev/null || true)"
    [[ "$archs" == *"x86_64"* && "$archs" == *"arm64"* ]] || return 1
  done < <(find "$package_dir" -type f \( -name '*.so' -o -name '*.dylib' \) -print 2>/dev/null)
  [[ "$found" -eq 1 ]]
}

SELECTOLAX_FLAGS=(--exclude-module selectolax)
if module_is_universal2 selectolax; then
  SELECTOLAX_FLAGS=(--collect-all selectolax)
else
  echo "selectolax wheel is not universal2; packaged macOS app will use the built-in HTMLParser fallback."
fi

AHO_FLAGS=(--exclude-module ahocorasick_rs)
if module_is_universal2 ahocorasick_rs; then
  AHO_FLAGS=(--collect-all ahocorasick_rs)
else
  echo "ahocorasick-rs wheel is not universal2; packaged macOS app will use the built-in Aho-Corasick fallback."
fi

python -m PyInstaller \
  --noconfirm --clean --windowed \
  --name "Archive Scout" \
  --icon assets/archivescout.icns \
  --add-data "assets/archivescout.png:assets" \
  --target-arch universal2 \
  --collect-all truststore --collect-all urllib3 --collect-all httpx --collect-all httpcore --collect-all dotenv \
  "${SELECTOLAX_FLAGS[@]}" "${AHO_FLAGS[@]}" \
  run_app.py

python -m PyInstaller \
  --noconfirm --clean --console --onefile \
  --name "ArchiveScoutCLI" \
  --target-arch universal2 \
  --collect-all truststore --collect-all urllib3 --collect-all httpx --collect-all httpcore --collect-all dotenv \
  "${SELECTOLAX_FLAGS[@]}" "${AHO_FLAGS[@]}" \
  run_cli.py

APP="dist/Archive Scout.app"
CLI="dist/ArchiveScoutCLI"
PACKAGE="release/ArchiveScout-macOS-Universal"
ZIP="release/ArchiveScout-macOS-Universal.zip"
TEMP_ROOT="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
VERIFY_ROOT="$(mktemp -d "$TEMP_ROOT/archive-scout-macos-verify.XXXXXX")"
EXTRACTED_APP="$VERIFY_ROOT/ArchiveScout-macOS-Universal/Archive Scout.app"
EXTRACTED_CLI="$VERIFY_ROOT/ArchiveScout-macOS-Universal/ArchiveScoutCLI"

cleanup() { rm -rf "$VERIFY_ROOT"; }
trap cleanup EXIT

# Keep the product/bundle name Archive Scout while exposing the smallest
# possible process identity change for Discord's automatic activity detector.
# This is intentionally not Rich Presence and adds no Discord dependency.
INNER_EXECUTABLE="$APP/Contents/MacOS/Wayback Machine Downloader"
mv "$APP/Contents/MacOS/Archive Scout" "$INNER_EXECUTABLE"
/usr/libexec/PlistBuddy -c "Set :CFBundleExecutable Wayback Machine Downloader" "$APP/Contents/Info.plist"

python scripts/verify_macos_bundle.py "$APP" --expected-executable "Wayback Machine Downloader"
codesign --force --deep --sign - "$APP"
codesign --force --sign - "$CLI"
codesign --verify --deep --strict --verbose=2 "$APP"
codesign --verify --strict --verbose=2 "$CLI"

STARTUP_LOG="$HOME/Library/Logs/Archive Scout/startup-error.log"
rm -f "$STARTUP_LOG"
ARCHIVE_SCOUT_STARTUP_PROBE=1 "$APP/Contents/MacOS/Wayback Machine Downloader"
if [[ -s "$STARTUP_LOG" ]]; then
  cat "$STARTUP_LOG" >&2
  exit 1
fi
"$CLI" --help >/dev/null

mkdir -p "$PACKAGE"
rm -rf "$PACKAGE" "$ZIP" "$ZIP.sha256"
mkdir -p "$PACKAGE"
ditto "$APP" "$PACKAGE/Archive Scout.app"
cp "$CLI" "$PACKAGE/ArchiveScoutCLI"
cp README.md "$PACKAGE/README.md"

ditto -c -k --sequesterRsrc --keepParent "$PACKAGE" "$ZIP"
ditto -x -k "$ZIP" "$VERIFY_ROOT"
python scripts/verify_macos_bundle.py "$EXTRACTED_APP" --expected-executable "Wayback Machine Downloader"
codesign --verify --deep --strict --verbose=2 "$EXTRACTED_APP"
codesign --verify --strict --verbose=2 "$EXTRACTED_CLI"
"$EXTRACTED_CLI" --help >/dev/null

(
  cd release
  shasum -a 256 ArchiveScout-macOS-Universal.zip > ArchiveScout-macOS-Universal.zip.sha256
)
trap - EXIT
rm -rf "$VERIFY_ROOT"
