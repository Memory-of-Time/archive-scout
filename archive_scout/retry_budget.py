"""Per-invocation retry limits; durable attempts and checkpoints stay intact."""
from __future__ import annotations

import time


class NoProgressBudget:
    def __init__(self, config, *, clock=None, count_only=False):
        self.limit = max(1, int(config.max_attempts))
        self.seconds = max(30.0, min(300.0, float(config.network.retry_max_seconds)))
        self.clock = clock or time.monotonic
        self.attempts = 0
        self.started = self.clock()
        self.active = False
        self.elapsed = 0.0
        self.paused = False
        self.count_only = count_only

    def pause(self):
        """Suspend this owner's clock while another target owns the scheduler."""
        if not self.paused and self.active:
            self.elapsed += self.clock() - self.started
        self.paused = True

    def resume(self):
        if self.paused:
            self.started = self.clock()
        self.paused = False

    def _elapsed(self):
        return self.elapsed + (self.clock() - self.started if self.active and not self.paused else 0.0)

    def attempted(self):
        if not self.active:
            self.started = self.clock()
            self.elapsed = 0.0
            self.active = True
        self.attempts += 1

    def progress(self):
        self.attempts = 0
        self.started = self.clock()
        self.active = False
        self.elapsed = 0.0

    def waiting(self):
        if not self.active:
            self.started = self.clock()
            self.active = True

    @property
    def exhausted(self):
        return self.attempts >= self.limit or (not self.count_only and self.active and self._elapsed() >= self.seconds)

    def wait_seconds(self, requested):
        if self.count_only:
            return max(0.0, float(requested))
        remaining = self.seconds - self._elapsed() if self.active else self.seconds
        return max(0.0, min(float(requested), remaining))

    def details(self):
        return {'attempts_without_progress': self.attempts, 'attempt_limit': self.limit,
                'active_no_progress_seconds': self._elapsed(),
                'active_seconds_limit': None if self.count_only else self.seconds}


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
