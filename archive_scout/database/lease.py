from __future__ import annotations

import functools
import os
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def project_lease(root):
    """Serialize whole operations and database replacement across processes.

    Advisory OS locks are released on crashes; the tiny lock file is reusable.
    This is acquired once per operation, never per capture or read-only query.
    """
    root = Path(root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    handle = (root / "archive_scout.lock").open("a+b")
    locked = False
    try:
        if os.name == "nt":
            import msvcrt
            if handle.seek(0, 2) == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise RuntimeError("This project has an active operation or maintenance task") from exc
        else:
            import fcntl
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise RuntimeError("This project has an active operation or maintenance task") from exc
        locked = True
        yield
    finally:
        try:
            if locked:
                if os.name == "nt":
                    import msvcrt
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle, fcntl.LOCK_UN)
        finally:
            handle.close()


def guard_project(function):
    @functools.wraps(function)
    def guarded(project, *args, **kwargs):
        with project_lease(getattr(project, "output_dir", project)):
            return function(project, *args, **kwargs)
    return guarded
