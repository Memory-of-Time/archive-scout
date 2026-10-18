# Archive Scout for macOS

The release build runs on Intel macOS with Universal2 Python. `scripts/build_macos.sh` packages **Archive Scout.app** and **ArchiveScoutCLI** inside the symlink-preserving `ArchiveScout-macOS-Universal.zip`.

The builder uses `ditto`, extracts its own output, verifies bundled Python resources and symlink integrity, then checks ad-hoc code signatures. An ad-hoc signature is **not** notarization and may still trigger Gatekeeper warnings.

Open `Archive Scout.app` for the desktop interface, or run `./ArchiveScoutCLI --help` from Terminal. Do not rename or move an app bundle while it is running.
