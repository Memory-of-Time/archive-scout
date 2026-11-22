from __future__ import annotations

import json
import os
import sys
import threading
import time
from pathlib import Path


class OwnerWatchdog:
    def __init__(self, directory, *, enabled=True, clock=None, build_id=None, run_id=None,
                 diagnostic_run_id=None, stall_seconds=120.0, repeat_seconds=300.0):
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
        self.build_id = build_id
        self.run_id = run_id
        self.diagnostic_run_id = diagnostic_run_id
        self.last_gap = None

    def beat(self, stage):
        if self.enabled:
            with self.lock:
                now = self.clock()
                last, previous_stage = self.pulse
                if self.thread is not None and not self.stopped.is_set() and now - last >= self.stall_seconds:
                    self.last_gap = {'ended_at_epoch': time.time(), 'seconds': now-last,
                                     'previous_stage': previous_stage, 'next_stage': stage}
                self.pulse = (now, stage)

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
                    name = frame.f_code.co_filename.replace('\\', '/')
                    file = 'archive_scout/' + name.split('/archive_scout/', 1)[1] if '/archive_scout/' in name else name.rsplit('/', 1)[-1]
                    stack.append({'file': file[-200:],
                                  'line': frame.f_lineno,
                                  'function': frame.f_code.co_name[:100]})
                    frame = frame.f_back
                threads.append({'name': names.get(ident, 'unknown'),
                                'owner': ident == self.owner, 'stack': stack})
        finally:
            del frames
        record = {'observed_at_epoch': time.time(), 'owner_stage': stage,
                  'build_id': self.build_id, 'acquisition_run_id': self.run_id,
                  'diagnostic_run_id': self.diagnostic_run_id,
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
