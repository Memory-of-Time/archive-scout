"""Small result batches; no SQLite transaction stays open while workers run."""
from __future__ import annotations

import sys
import time
from collections.abc import Callable


def result_size(value: object) -> int:
    pending = [value]
    seen: set[int] = set()
    size = 0
    while pending:
        item = pending.pop()
        if id(item) in seen:
            continue
        seen.add(id(item))
        size += sys.getsizeof(item)
        if isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, (list, tuple, set, frozenset)):
            pending.extend(item)
    return size


class BoundedResultWriter:
    def __init__(self, persist: Callable[[list], None], *, max_count: int = 16,
                 max_bytes: int = 1024 * 1024, max_delay: float = 0.25,
                 clock: Callable[[], float] = time.monotonic):
        self.persist = persist
        self.max_count = max(1, max_count)
        self.max_bytes = max(1, max_bytes)
        self.max_delay = max(0.0, max_delay)
        self.clock = clock
        self.items: list = []
        self.bytes = 0
        self.started = 0.0

    def add(self, result: object) -> None:
        size = result_size(result)
        if self.items and self.bytes + size > self.max_bytes:
            self.flush()
        if not self.items:
            self.started = self.clock()
        self.items.append(result)
        self.bytes += size
        if len(self.items) >= self.max_count or self.bytes >= self.max_bytes:
            self.flush()
        else:
            self.flush_if_due()

    def flush_if_due(self) -> None:
        if self.items and self.clock() - self.started >= self.max_delay:
            self.flush()

    def flush(self) -> None:
        if not self.items:
            return
        results, self.items = self.items, []
        self.bytes = 0
        # On a write failure, propagate it: durable queues/checkpoints still own
        # recovery. Never report these results as completed before commit.
        self.persist(results)
