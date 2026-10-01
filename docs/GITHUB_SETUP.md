# GitHub setup

The repository contains two permanent workflows.

## Tests

`.github/workflows/tests.yml` runs on pushes to `main`, pull requests, and manual dispatch. The matrix covers Ubuntu, Windows, Intel macOS, and Apple Silicon macOS with Python 3.11 and 3.12.

## Build All Platforms

`.github/workflows/build-and-release.yml` can be run manually for test artifacts and runs for version tags.

Platform outputs:

- ArchiveScout-Windows-x64.zip
- ArchiveScout-Linux-x64.tar.gz
- ArchiveScout-macOS-Universal.zip

Tagged Windows releases require the configured Artifact Signing variables/secrets. A manual build may create an unsigned Windows test package when signing is disabled; that package is not intended to substitute for an official signed tagged release.

For the first official release, create tag `v1.0.0` only after the complete test matrix and native package smoke tests succeed on the exact release commit.
