from __future__ import annotations

import subprocess
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path


def _startup_log_path() -> Path:
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Logs" / "Archive Scout" / "startup-error.log"
    return Path.home() / ".archive-scout" / "startup-error.log"


def _show_macos_startup_alert(log_path: Path) -> None:
    if sys.platform != "darwin":
        return
    script = (
        "on run argv\n"
        "display alert (item 1 of argv) message (item 2 of argv) as critical\n"
        "end run"
    )
    message = (
        "Archive Scout could not start. The error was saved to:\n\n"
        + str(log_path)
        + "\n\nPlease include that file when reporting the problem."
    )
    try:
        subprocess.run(
            ["/usr/bin/osascript", "-e", script, "--", "Archive Scout startup error", message],
            check=False,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        pass


def _record_startup_error() -> Path:
    path = _startup_log_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    report = (
        "Archive Scout startup failure\n"
        + datetime.now(timezone.utc).isoformat()
        + "\n\n"
        + traceback.format_exc()
    )
    path.write_text(report, encoding="utf-8", errors="replace")
    return path


try:
    from archive_scout.app import main

    main()
except BaseException:
    startup_log = _record_startup_error()
    _show_macos_startup_alert(startup_log)
    raise
