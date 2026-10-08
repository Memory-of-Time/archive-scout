"""One scanner control for retained scans, rescans and deliberate retries."""
from __future__ import annotations

import os


def scanner_workers(configured: int | None = 0, *, overlap: bool = False) -> int:
    automatic = min(2 if overlap else 4, max(1, (os.cpu_count() or 4) - 1))
    return max(1, min(8 if overlap else 32, int(configured or automatic)))


def scanner_options(config):
    # Keep the existing extension call signature for default settings.
    return ({"scan_backend": config.scan_backend, "scan_memory_mb": config.scan_memory_mb}
            if config.scan_backend != "auto" or config.scan_memory_mb != 256 else {})
