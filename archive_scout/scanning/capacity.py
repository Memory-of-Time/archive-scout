from __future__ import annotations

import os
import sys
from pathlib import Path


def available_memory():
    try:
        if sys.platform == 'win32':
            import ctypes
            class Memory(ctypes.Structure):
                _fields_ = [('length', ctypes.c_ulong), ('load', ctypes.c_ulong)] + [
                    (name, ctypes.c_ulonglong) for name in
                    ('total', 'available', 'page_total', 'page_available', 'virtual_total', 'virtual_available', 'extended')]
            value = Memory()
            value.length = ctypes.sizeof(value)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(value)):
                return int(value.available)
        if sys.platform.startswith('linux'):
            fields = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
            available = int(fields['MemAvailable'].split()[0]) * 1024
            limit = Path('/sys/fs/cgroup/memory.max')
            current = Path('/sys/fs/cgroup/memory.current')
            if limit.is_file() and current.is_file() and limit.read_text().strip().isdigit():
                available = min(available, max(0, int(limit.read_text()) - int(current.read_text())))
            return available
    except (OSError, ValueError, KeyError, AttributeError):
        pass
    return None


def scan_capacity(requested, byte_budget):
    cpus = os.cpu_count() or 1
    try:
        cpus = min(cpus, len(os.sched_getaffinity(0)))
    except (AttributeError, OSError):
        pass
    workers = max(1, min(32, int(requested or min(8, cpus)), cpus))
    available = available_memory()
    budget = int(byte_budget)
    if available is not None:
        budget = min(budget, max(1024 * 1024, available // 8))
        workers = min(workers, max(1, budget // (4 * 1024 * 1024)))
    return workers, budget
