# Archive Scout v1.3.2 source patch

Apply this overlay to Archive Scout v1.3.1 with the Project Errors add-on and its CI fix.

1. Stop Archive Scout and extract the patch into the repository root containing `pyproject.toml`. Replace the matching files.
2. Run `python -m pip install .` for a source installation, or rebuild your platform executable with the release workflow.
3. Open your existing schema 13 project and select Resume. Scout uses the saved inventory, cursors, bodies, and retry schedules.

The supplied Phoenix trace recorded 6,746 text connection failures and 19 validated saves. The last 81 minutes produced zero saves. The available HTTP stacks all reported failures reaching the same Wayback endpoint. The saved-body checks and retry changes are covered by the loopback fixtures. Sustained throughput on live Wayback is unverified.

The project's `errors` folder contains the transport recovery state and the selected representation retry beside the capture outcome.
