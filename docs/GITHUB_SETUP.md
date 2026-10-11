# Publishing Scout

The README links to assets in `Memory-of-Time/archive-scout-testing` and must be updated if you move this project to the final public repository.

1. Commit the complete Scout source (not a patch overlay) to the chosen GitHub repository.
2. Run **Tests** and confirm all Windows, macOS and Linux entries pass.
3. Verify your operating-system signing strategy and public release notes.
4. Tag `v1.2.0` and push the tag. The **Build All Platforms** workflow builds and uploads the three platform bundles and matching SHA-256 checksum files.
5. Confirm that all three latest-release links in the README download actual packages.

If releasing from another repository, replace `Memory-of-Time/archive-scout-testing` in README.md before tagging. Avoid changing the asset filenames without changing both the build workflow and the README links.
