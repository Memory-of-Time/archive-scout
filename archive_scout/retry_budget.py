"""Per-invocation retry limits; durable attempts and checkpoints stay intact."""
from __future__ import annotations

import time


class NoProgressBudget:
    def __init__(self, config, *, clock=None):
        self.limit = max(1, int(config.max_attempts))
        self.seconds = max(30.0, min(300.0, float(config.network.retry_max_seconds)))
        self.clock = clock or time.monotonic
        self.attempts = 0
        self.started = self.clock()
        self.active = False

    def attempted(self):
        if not self.active:
            self.started = self.clock()
            self.active = True
        self.attempts += 1

    def progress(self):
        self.attempts = 0
        self.started = self.clock()
        self.active = False

    def waiting(self):
        if not self.active:
            self.started = self.clock()
            self.active = True

    @property
    def exhausted(self):
        return self.attempts >= self.limit or (self.active and self.clock() - self.started >= self.seconds)

    def wait_seconds(self, requested):
        remaining = self.seconds - (self.clock() - self.started) if self.active else self.seconds
        return max(0.0, min(float(requested), remaining))


def incomplete_retry_message(label):
    return (f"{label}: retry budget reached without progress. Unfinished work and checkpoints "
            "are retained; continuing to reports and other eligible work. Use Resume to retry.")


class CursorHistory:
    def __init__(self):
        from collections import deque
        self.recent = deque(maxlen=64)

    def check(self, cursor):
        import hashlib
        if cursor and hashlib.sha256(cursor.encode()).digest() in self.recent:
            from .cdx.client import MalformedCDXResponse
            raise MalformedCDXResponse('CDX continuation cycled to an already committed cursor; checkpoint retained',
                                       splittable=False)

    def committed(self, cursor):
        if cursor:
            import hashlib
            self.recent.append(hashlib.sha256(cursor.encode()).digest())
