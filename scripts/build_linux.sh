#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf build dist release
python -m PyInstaller --noconfirm --clean --windowed --onedir --name ArchiveScout --icon assets/archivescout.png --add-data "assets/archivescout.png:assets" --collect-all truststore --collect-all urllib3 --collect-all httpx --collect-all httpcore --collect-all dotenv --collect-all selectolax --collect-all ahocorasick_rs run_app.py
python -m PyInstaller --noconfirm --clean --console --onedir --name ArchiveScoutCLI --collect-all truststore --collect-all urllib3 --collect-all httpx --collect-all httpcore --collect-all dotenv --collect-all selectolax --collect-all ahocorasick_rs run_cli.py
mkdir -p release/ArchiveScout-Linux-x64
cp -R dist/ArchiveScout release/ArchiveScout-Linux-x64/ArchiveScout
cp -R dist/ArchiveScoutCLI release/ArchiveScout-Linux-x64/ArchiveScoutCLI
cp packaging/linux/install.sh packaging/linux/uninstall.sh README.md release/ArchiveScout-Linux-x64/
tar -C release -czf release/ArchiveScout-Linux-x64.tar.gz ArchiveScout-Linux-x64
(
  cd release
  sha256sum ArchiveScout-Linux-x64.tar.gz > ArchiveScout-Linux-x64.tar.gz.sha256
)
