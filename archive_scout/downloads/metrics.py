"""Bounded, committed-save throughput; one aggregate per monotonic second."""
from __future__ import annotations

import time
from collections import deque


class CommittedThroughput:
    def __init__(self, clock=time.monotonic):
        self.clock = clock
        self.started = clock()
        self.buckets = deque(maxlen=301)
        self.fresh = self.adopted = self.bytes = 0

    def commit(self, sizes, adopted):
        now = self.clock()
        fresh_count = fresh_bytes = 0
        for size, existing in zip(sizes, adopted):
            if existing:
                self.adopted += 1
            else:
                fresh_count += 1
                fresh_bytes += int(size)
        self.fresh += fresh_count
        self.bytes += fresh_bytes
        second = int(now)
        if self.buckets and self.buckets[-1][0] == second:
            _, previous_count, previous_bytes = self.buckets.pop()
            fresh_count += previous_count
            fresh_bytes += previous_bytes
        self.buckets.append((second, fresh_count, fresh_bytes))

    def snapshot(self):
        now = self.clock()
        elapsed = max(.001, now - self.started)
        result = {'fresh_committed': self.fresh, 'adopted_existing': self.adopted,
                  'committed_bytes': self.bytes, 'fresh_average_rate': self.fresh / elapsed}
        for window in (10, 60, 300):
            count = size = 0
            for second, completed, byte_count in reversed(self.buckets):
                if second < int(now) - window + 1:
                    break
                count += completed
                size += byte_count
            period = max(.001, min(window, elapsed))
            result[f'fresh_rate_{window}s'] = count / period
            result[f'bytes_rate_{window}s'] = size / period
        return result
