"""Bounded failure evidence and in-memory request timing; never completion proof."""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
import sys
import marshal
import importlib
from collections import Counter, deque
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

from ..constants import VERSION
from ..events import ProgressEvent
from ..network.transports import exception_chain
from ..utils import utc_now


@lru_cache(maxsize=1)
def build_fingerprint():
    root = Path(__file__).resolve().parents[1]
    digest = hashlib.sha256()
    found = False
    for name in ('downloads/downloader.py', 'downloads/diagnostics.py', 'downloads/schedule.py',
                 'downloads/validation.py', 'network/transports.py', 'cdx/client.py',
                 'media/indexer.py', 'media/downloader.py', 'media/recovery.py', 'media/embedded_pages.py', 'scanning/pending.py', 'domains.py', 'scanning/scoring.py', 'operations.py', 'retry_budget.py'):
        path = root / name
        if path.is_file():
            try:
                content = path.read_bytes()
            except OSError:
                continue
            found = True
            digest.update(name.encode()); digest.update(content)
        else:
            # PyInstaller usually ships code objects rather than .py files.
            # Hash those as well, so packaged builds are attributable instead
            # of every patched executable saying frozen-unfingerprinted.
            try:
                module = importlib.import_module('archive_scout.' + name[:-3].replace('/', '.'))
                code = module.__spec__.loader.get_code(module.__name__)
                if code is not None:
                    digest.update(name.encode()); digest.update(marshal.dumps(code))
                    found = True
            except (AttributeError, ImportError, OSError):
                continue
    return VERSION + ':' + (digest.hexdigest()[:16] if found else 'frozen-unfingerprinted')


def _safe_url(value):
    try:
        value = re.sub(r'(https?://)[^/@\s]+@', r'\1', str(value))
        parsed = urlsplit(value)
        host = parsed.netloc.rsplit('@', 1)[-1]
        query = [(k, '[redacted]' if any(word in k.casefold() for word in
                  ('token', 'password', 'secret', 'api_key', 'authorization')) else v)
                 for k, v in parse_qsl(parsed.query, keep_blank_values=True)]
        return urlunsplit((parsed.scheme, host, parsed.path, urlencode(query), ''))[:1500]
    except ValueError:
        return '[invalid URL]'


def _process_memory():
    """One cheap process sample per health summary, with no new dependency."""
    try:
        if sys.platform == 'win32':
            import ctypes
            from ctypes import wintypes
            class Counters(ctypes.Structure):
                _fields_ = [('cb', wintypes.DWORD), ('faults', wintypes.DWORD)] + [
                    (name, ctypes.c_size_t) for name in ('peak_working_set', 'working_set',
                    'peak_paged', 'paged', 'peak_nonpaged', 'nonpaged', 'pagefile', 'peak_pagefile')]
            value = Counters(); value.cb = ctypes.sizeof(value)
            if ctypes.windll.psapi.GetProcessMemoryInfo(ctypes.c_void_p(-1), ctypes.byref(value), value.cb):
                return {'rss_bytes': value.working_set, 'peak_rss_bytes': value.peak_working_set}
        else:
            import resource
            value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            return {'peak_rss_bytes': int(value * (1 if sys.platform == 'darwin' else 1024))}
    except Exception:
        pass
    return {}  # Unsupported metrics must not affect acquisition.


class AcquisitionDiagnostics:
    MAX_SAMPLES = 16
    MAX_SAMPLE_BYTES = 20 * 1024

    def __init__(self, root, callback, *, enabled=True, kind="text"):
        self.root, self.callback, self.enabled = Path(root), callback, enabled
        self.kind = "media" if kind == "media" else "text"
        self.build = build_fingerprint()
        self.lock = threading.Lock()
        self.responses = Counter()
        self.timings = Counter()
        self.timing_counts = Counter()
        self.rejected_responses = Counter()
        self.recent_responses = deque(maxlen=2048)
        self.controls = deque(maxlen=8)
        self.failures = Counter()
        self.samples = set()
        self.phase_saved_base = 0
        self.batch_saved = 0
        self.batch_metrics = None
        self.last_summary = time.monotonic()
        self.summary_path = self.root / 'diagnostics' / (self.kind + '-acquisition')

    def close(self):
        pass  # memory-only operation observations

    def begin_batch(self, callback):
        self.phase_saved_base += self.batch_saved
        self.batch_saved = 0
        self.callback = callback
        self.batch_metrics = None

    def _emit(self, event):
        if self.callback:
            try:
                self.callback(event)
            except Exception:
                pass

    def response(self, detail):
        # HTTP worker callback: memory only, no SQLite or disk access.
        if not self.enabled:
            return
        with self.lock:
            self.responses[str(detail['status'])] += 1
            self.recent_responses.append((time.monotonic(), dict(detail)))
            if detail.get('validation_rejected'):
                self.rejected_responses[str(detail['status'])] += 1
            for key in ('gate_wait_seconds', 'request_turn_seconds', 'backend_elapsed_seconds'):
                if key in detail:
                    self.timings[key] += max(0.0, float(detail[key]))
                    self.timing_counts[key] += 1
            if detail['status'] in (429, 503):
                self.controls.append({**detail, 'url': _safe_url(detail.get('url', '')),
                                      'observed_at': utc_now()})

    def failure(self, row, category, exc):
        self.failures[category] += 1
        if not self.enabled:
            return
        if self.callback and self.failures[category] == 1:
            self._emit(ProgressEvent('acquisition_health',
                f'{self.kind.title()} acquisition failure [{category}] for capture {row["id"]}; '
                f'build {self.build}. Bounded diagnostic sampling is enabled.'))
        slot = int(hashlib.sha256((category + ':' + str(row['id'])).encode()).hexdigest()[:8], 16) % self.MAX_SAMPLES
        if slot in self.samples:
            return
        self.samples.add(slot)
        evidence = {}
        for error in reversed(list(exception_chain(exc))):
            evidence.update(getattr(error, 'representation_diagnostic', {}))
            evidence.update(getattr(error, 'transfer_diagnostic', {}))
        if 'final_url' in evidence:
            evidence['final_url'] = _safe_url(evidence['final_url'])
        value = {'build_id': self.build, 'observed_at': utc_now(), 'category': category,
                 'capture_id': int(row['id']), 'capture_timestamp': str(row['timestamp']),
                 'original_url': _safe_url(row['original_url']),
                 'expected_cdx_digest': str(dict(row).get('digest') or '')[:100],
                 'exception_type': type(exc).__name__, 'exception_message': _safe_url(str(exc))[:1500],
                 'exception_chain': [{'type':type(error).__name__, 'message':_safe_url(str(error))[:500]} for error in list(exception_chain(exc))[:8]], 'evidence': evidence,
                 'notice': 'Unvalidated diagnostic prefixes. Never use as saved capture or resume proof.'}
        if self.kind == 'media':
            value['media_capture_id'] = value.pop('capture_id')
        encoded = json.dumps(value, ensure_ascii=True, sort_keys=True).encode()
        if len(encoded) > self.MAX_SAMPLE_BYTES:
            return
        try:
            self.summary_path.mkdir(parents=True, exist_ok=True)
            path = self.summary_path / f'sample-{slot:02}.json'
            if path.exists():
                try:
                    if json.loads(path.read_text()).get('build_id') != self.build:
                        path.unlink()
                except (OSError,ValueError):
                    pass
            temporary = path.with_suffix('.tmp')
            temporary.write_bytes(encoded)
            os.replace(temporary, path)
        except OSError:
            pass  # Optional diagnostics must never downgrade committed evidence.

    def snapshot(self):
        with self.lock:
            cutoff = time.monotonic() - 30.0
            while self.recent_responses and self.recent_responses[0][0] < cutoff:
                self.recent_responses.popleft()
            recent = [item for _, item in self.recent_responses]
            def mean(key):
                values = [float(item[key]) for item in recent if key in item]
                return sum(values)/len(values) if values else None
            return {'build_id': self.build, 'response_counts': dict(self.responses),
                    'validation_rejected_response_counts': dict(self.rejected_responses),
                    'failure_categories': dict(self.failures),
                    'avg_gate_wait_seconds': self.timings['gate_wait_seconds']/self.timing_counts['gate_wait_seconds'] if self.timing_counts['gate_wait_seconds'] else None,
                    'avg_request_turn_seconds': self.timings['request_turn_seconds']/self.timing_counts['request_turn_seconds'] if self.timing_counts['request_turn_seconds'] else None,
                    'avg_backend_elapsed_seconds': self.timings['backend_elapsed_seconds']/self.timing_counts['backend_elapsed_seconds'] if self.timing_counts['backend_elapsed_seconds'] else None,
                    'recent_30s': {'responses':len(recent),
                        'mean_decoded_bytes':mean('decoded_bytes'),
                        'mean_gate_wait_seconds':mean('gate_wait_seconds'),
                        'mean_backend_elapsed_seconds':mean('backend_elapsed_seconds')},
                    'process_cpu_seconds': time.process_time(),
                    'process_threads': threading.active_count(),
                    'memory': _process_memory(),
                    'timing_sample_counts':dict(self.timing_counts), 'recent_control_responses': list(self.controls)}

    @staticmethod
    def _timing(value):
        return "unknown (no samples)" if value is None else f"{value:.3f}s"

    def summary(self, saved, *, force=False):
        self.batch_saved = saved
        total_saved = self.phase_saved_base + saved
        if not self.enabled or (not force and time.monotonic()-self.last_summary < 30):
            return
        self.last_summary = time.monotonic()
        detail = self.snapshot()
        if self.batch_metrics is not None:
            try:
                detail['acquisition'] = self.batch_metrics()
            except Exception:
                pass  # Diagnostics must not affect saved evidence or retries.
        detail['new_saves'] = total_saved
        detail['batch_new_saves'] = saved
        try:
            self.summary_path.mkdir(parents=True, exist_ok=True)
            path = self.summary_path / 'latest.json'
            temporary = path.with_suffix('.tmp')
            temporary.write_text(json.dumps(detail, sort_keys=True), encoding='utf-8')
            os.replace(temporary, path)
        except OSError:
            pass
        if self.callback:
            metrics = detail.get('acquisition', {})
            stages = (f"workers {metrics['configured_workers']}; "
                f"running {metrics['running_tasks']}; completed awaiting owner {metrics['completed_waiting_owner']}; "
                f"mean owner commit/write wait {self._timing(metrics['avg_owner_commit_seconds'])}; "
                f"mean payload publication {self._timing(metrics['avg_payload_publication_seconds'])}; "
                f"queue preparation {metrics['queue_preparation_seconds']:.3f}s. "
                if metrics else '')
            self._emit(ProgressEvent('acquisition_health',
                f"{self.kind.title()} acquisition health: build {self.build}; new saves {total_saved:,}; "
                f"failures {dict(self.failures)}; responses {detail['response_counts']}; "
                f"mean gate wait {self._timing(detail['avg_gate_wait_seconds'])}; "
                f"mean backend transfer/write time {self._timing(detail['avg_backend_elapsed_seconds'])}. "
                + stages + 'Unfinished captures remain checkpointed.', detail=detail))
