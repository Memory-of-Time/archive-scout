from __future__ import annotations

import os
import sys
from pathlib import Path

_LOADED = False


def load_environment(root: Path | None = None) -> None:
    """Load developer .env values without overriding real environment variables.

    Packaged applications intentionally do not read a colocated .env file. Release
    builds should receive credentials from the process environment or an OS-level
    secret store. Source checkouts may use python-dotenv as a development
    convenience; python-dotenv's default precedence preserves already-defined
    environment variables.
    """
    global _LOADED
    if _LOADED:
        return
    _LOADED = True
    if getattr(sys, "frozen", False):
        return
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    base = Path(root or Path.cwd())
    candidate = base / ".env"
    if candidate.is_file():
        load_dotenv(candidate, override=False)


def environment_value(name: str, default: str = "") -> str:
    load_environment()
    return os.environ.get(name, default)
