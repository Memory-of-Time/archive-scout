# Scout for macOS

The official macOS build uses an Intel-hosted GitHub runner with Universal2 Python. `scripts/build_macos.sh` packages `Scout.app` and `ScoutCLI` into the symlink-preserving `Scout-macOS-Universal.zip`.

The builder uses `ditto`, extracts its own output, verifies framework `base_library.zip` and symbolic links, and checks the ad-hoc code signatures. An ad-hoc signature is not Apple Developer ID notarization: users may see Gatekeeper warnings until a trusted signing/notarization pipeline is configured.

Open `Scout.app` for the desktop interface, or run `./ScoutCLI --help` from Terminal.
