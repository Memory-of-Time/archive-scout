"""Bounded spawn workers: SQLite and evidence ownership stay in the parent."""
from __future__ import annotations

import concurrent.futures
import multiprocessing
import sys
from pathlib import Path

from .jobs import ScanJob

_CONFIG = _JOBS = _REPORT = None
_METRICS = None
_SLOT = 0


def _initialize(config, definitions, report, metrics, next_slot):
    global _CONFIG, _JOBS, _REPORT, _METRICS, _SLOT
    _METRICS = metrics
    with next_slot.get_lock():
        _SLOT = next_slot.value
        next_slot.value += 1
    _CONFIG, _REPORT = config, report
    _JOBS = [ScanJob.create(*definition) for definition in definitions]


def _capture(row, path):
    from ..downloads.downloader import _scan_saved_capture
    try:
        return _scan_saved_capture(row, path, _CONFIG, _JOBS)
    finally:
        _measure_worker()


def _document(row):
    from .rescanner import _analyze_saved_document
    try:
        return _analyze_saved_document(row, _JOBS, _REPORT)
    finally:
        _measure_worker()


def _measure_worker():
    try:
        import resource
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peak /= 1024 * 1024 if sys.platform == 'darwin' else 1024
    except ImportError:
        import ctypes
        from ctypes import wintypes
        class Counters(ctypes.Structure):
            _fields_ = [('cb', wintypes.DWORD), ('PageFaultCount', wintypes.DWORD)] + [(name, ctypes.c_size_t) for name in (
                'PeakWorkingSetSize', 'WorkingSetSize', 'QuotaPeakPagedPoolUsage', 'QuotaPagedPoolUsage',
                'QuotaPeakNonPagedPoolUsage', 'QuotaNonPagedPoolUsage', 'PagefileUsage', 'PeakPagefileUsage')]
        counter = Counters()
        counter.cb = ctypes.sizeof(counter)
        current = ctypes.windll.kernel32.GetCurrentProcess
        current.restype = wintypes.HANDLE
        measure = ctypes.windll.psapi.GetProcessMemoryInfo
        measure.argtypes = (wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD)
        measure.restype = wintypes.BOOL
        if not measure(current(), ctypes.byref(counter), counter.cb):
            return
        peak = counter.PeakWorkingSetSize / (1024 * 1024)
    if _METRICS is not None:
        _METRICS[_SLOT] = max(_METRICS[_SLOT], peak)


class ScanExecutor:
    def __init__(self, workers, jobs, *, config=None, report=None, total=0, backend='auto'):
        regex_work = any(pattern.rule.kind == 'regex' for job in jobs for pattern in job.patterns)
        self.backend = 'process' if backend == 'process' or (backend == 'auto' and ((workers > 1 and total >= 256) or regex_work)) else 'thread'
        self.workers = workers
        self.metrics = None
        if self.backend == 'process':
            context = multiprocessing.get_context('spawn')
            self.metrics = context.Array('d', workers, lock=False)
            next_slot = context.Value('i', 0)
            self.pool = concurrent.futures.ProcessPoolExecutor(max_workers=workers,
                mp_context=context, initializer=_initialize,
                initargs=(config, [(job.scan_run_id, job.keyword_set_name, job.rules) for job in jobs], report, self.metrics, next_slot))
        else:
            self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix='archive-local-scan')

    def submit(self, function, *args):
        if self.backend == 'thread':
            return self.pool.submit(function, *args)
        if function.__name__ == '_scan_saved_capture':
            return self.pool.submit(_capture, args[0], args[1])
        if function.__name__ == '_analyze_saved_document':
            return self.pool.submit(_document, args[0])
        raise ValueError('Unsupported process scan task')

    def metrics_snapshot(self):
        return {"scan_backend": self.backend, "scan_worker_peak_rss_sum_mib": sum(self.metrics) if self.metrics is not None else None}

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        self.shutdown(cancel=kind is not None)

    def shutdown(self, *, cancel=False):
        if cancel and self.backend == 'process':
            # Python 3.11-3.13 lack terminate_workers. Workers own no SQLite or
            # payload mutations, so termination cannot corrupt committed evidence.
            processes = tuple((getattr(self.pool, '_processes', None) or {}).values())
            for process in processes:
                if process.is_alive():
                    process.terminate()
            for process in processes:
                process.join(timeout=1)
                if process.is_alive():
                    process.kill()
            self.pool.shutdown(wait=True, cancel_futures=True)
        else:
            self.pool.shutdown(wait=True, cancel_futures=cancel)


class ScanByteBudget:
    def __init__(self, megabytes=256):
        self.limit = max(32, float(megabytes)) * 1024 * 1024
        self.used = 0

    def estimate(self, row):
        # DOM, decoded/normalized strings, result and IPC copies can coexist.
        # This is a reservation, never a truncation or a content-size cutoff.
        size = max(int(row.get('bytes_saved') or row.get('size_bytes') or 0), 0)
        path = row.get('local_path') or row.get('path')
        if path:
            try:
                size = max(size, Path(str(path)).stat().st_size)
            except OSError:
                pass
        return max(64 * 1024, size * 20)

    def accepts(self, size):
        # One complete oversize document is allowed with exclusive admission.
        return self.used == 0 or self.used + size <= self.limit

    def reserve(self, size):
        self.used += size

    def release(self, size):
        self.used = max(0, self.used - size)
