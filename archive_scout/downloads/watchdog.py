from __future__ import annotations

import json
import os
import sys
import threading
import time
from pathlib import Path


class OwnerWatchdog:
    def __init__(self, directory, *, enabled=True, clock=None,
                 stall_seconds=120.0, repeat_seconds=300.0):
        self.directory = Path(directory)
        self.enabled = enabled
        self.clock = clock or time.monotonic
        self.stall_seconds = stall_seconds
        self.repeat_seconds = repeat_seconds
        self.lock = threading.Lock()
        self.stopped = threading.Event()
        self.thread = None
        self.owner = threading.get_ident()
        self.pulse = (self.clock(), 'starting')
        self.last_report = float('-inf')

    def beat(self, stage):
        if self.enabled:
            with self.lock:
                self.pulse = (self.clock(), stage)

    def start(self):
        if self.enabled and self.thread is None:
            self.owner = threading.get_ident()
            self.beat('starting acquisition')
            self.stopped.clear()
            self.thread = threading.Thread(target=self._run,
                name='archive-acquisition-watchdog', daemon=True)
            self.thread.start()

    def close(self):
        self.stopped.set()
        thread = self.thread
        if thread is not None:
            thread.join(timeout=1)
            if not thread.is_alive():
                self.thread = None

    def _run(self):
        while not self.stopped.wait(5.0):
            try:
                self.check()
            except Exception:
                pass

    def check(self):
        if not self.enabled or self.stopped.is_set():
            return False
        with self.lock:
            last, stage = self.pulse
        now = self.clock()
        if now-last < self.stall_seconds or now-self.last_report < self.repeat_seconds:
            return False
        frames = sys._current_frames()
        names = {thread.ident: thread.name for thread in threading.enumerate()}
        threads = []
        try:
            ids = [self.owner] + [ident for ident in frames if ident != self.owner]
            for ident in ids[:24]:
                frame = frames.get(ident)
                stack = []
                while frame is not None and len(stack) < 20:
                    stack.append({'file': frame.f_code.co_filename[-200:],
                                  'line': frame.f_lineno,
                                  'function': frame.f_code.co_name[:100]})
                    frame = frame.f_back
                threads.append({'name': names.get(ident, 'unknown'),
                                'owner': ident == self.owner, 'stack': stack})
        finally:
            del frames
        record = {'observed_at_epoch': time.time(), 'owner_stage': stage,
                  'owner_silent_seconds': now-last, 'threads': threads,
                  'notice': 'Thread locations only; no frame locals or archived content. '
                            'A suspended computer can also cause a heartbeat gap.'}
        encoded = json.dumps(record).encode()
        while len(encoded) > 64 * 1024 and len(threads) > 1:
            threads.pop()
            encoded = json.dumps(record).encode()
        self.directory.mkdir(parents=True, exist_ok=True)
        latest = self.directory/'stall-latest.json'
        previous = self.directory/'stall-previous.json'
        temporary = self.directory/'stall-latest.tmp'
        temporary.write_bytes(encoded)
        if latest.exists():
            os.replace(latest, previous)
        os.replace(temporary, latest)
        self.last_report = now
        return True
