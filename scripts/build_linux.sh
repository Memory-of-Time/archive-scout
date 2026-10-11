#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf build dist release
python -m PyInstaller --noconfirm --clean --windowed --onedir --name Scout --icon archive_scout/assets/scout.png --add-data "archive_scout/assets/scout.png:assets" --collect-all truststore --collect-all urllib3 --collect-all httpx --collect-all httpcore --collect-all dotenv --collect-all selectolax --collect-all ahocorasick_rs run_app.py
python -m PyInstaller --noconfirm --clean --console --onedir --name ScoutCLI --collect-all truststore --collect-all urllib3 --collect-all httpx --collect-all httpcore --collect-all dotenv --collect-all selectolax --collect-all ahocorasick_rs run_cli.py
mkdir -p release/Scout-Linux-x64
cp -R dist/Scout release/Scout-Linux-x64/Scout
cp -R dist/ScoutCLI release/Scout-Linux-x64/ScoutCLI
cp packaging/linux/install.sh packaging/linux/uninstall.sh README.md release/Scout-Linux-x64/
tar -C release -czf release/Scout-Linux-x64.tar.gz Scout-Linux-x64
(
  cd release
  sha256sum Scout-Linux-x64.tar.gz > Scout-Linux-x64.tar.gz.sha256
)
