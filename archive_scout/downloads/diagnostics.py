"""Bounded failure evidence and in-memory request timing; never completion proof."""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
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
                 'downloads/validation.py', 'network/transports.py', 'cdx/client.py'):
        path = root / name
        if path.is_file():
            try:
                content = path.read_bytes()
            except OSError:
                continue
            found = True
            digest.update(name.encode()); digest.update(content)
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


class AcquisitionDiagnostics:
    MAX_SAMPLES = 16
    MAX_SAMPLE_BYTES = 20 * 1024

    def __init__(self, root, callback, *, enabled=True):
        self.root, self.callback, self.enabled = Path(root), callback, enabled
        self.build = build_fingerprint()
        self.lock = threading.Lock()
        self.responses = Counter()
        self.timings = Counter()
        self.controls = deque(maxlen=8)
        self.failures = Counter()
        self.samples = set()
        self.last_summary = time.monotonic()
        self.summary_path = self.root / 'diagnostics' / 'text-acquisition'

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
            for key in ('gate_wait_seconds', 'request_turn_seconds', 'backend_elapsed_seconds'):
                self.timings[key] += max(0.0, float(detail.get(key, 0)))
            if detail['status'] in (429, 503):
                self.controls.append({**detail, 'url': _safe_url(detail.get('url', '')),
                                      'observed_at': utc_now()})

    def failure(self, row, category, exc):
        self.failures[category] += 1
        if not self.enabled:
            return
        if self.callback and self.failures[category] == 1:
            self._emit(ProgressEvent('acquisition_health',
                f'Text acquisition failure [{category}] for capture {row["id"]}; '
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
                 'exception_type': type(exc).__name__, 'evidence': evidence,
                 'notice': 'Unvalidated diagnostic prefixes. Never use as saved capture or resume proof.'}
        encoded = json.dumps(value, ensure_ascii=True, sort_keys=True).encode()
        if len(encoded) > self.MAX_SAMPLE_BYTES:
            return
        try:
            self.summary_path.mkdir(parents=True, exist_ok=True)
            path = self.summary_path / f'sample-{slot:02}.json'
            temporary = path.with_suffix('.tmp')
            temporary.write_bytes(encoded)
            os.replace(temporary, path)
        except OSError:
            pass  # Optional diagnostics must never downgrade committed evidence.

    def snapshot(self):
        with self.lock:
            count = max(1, sum(self.responses.values()))
            return {'build_id': self.build, 'response_counts': dict(self.responses),
                    'failure_categories': dict(self.failures),
                    'avg_gate_wait_seconds': self.timings['gate_wait_seconds']/count,
                    'avg_request_turn_seconds': self.timings['request_turn_seconds']/count,
                    'avg_backend_elapsed_seconds': self.timings['backend_elapsed_seconds']/count,
                    'recent_control_responses': list(self.controls)}

    def summary(self, saved, *, force=False):
        if not self.enabled or (not force and time.monotonic()-self.last_summary < 30):
            return
        self.last_summary = time.monotonic()
        detail = self.snapshot()
        detail['new_saves'] = saved
        try:
            self.summary_path.mkdir(parents=True, exist_ok=True)
            path = self.summary_path / 'latest.json'
            temporary = path.with_suffix('.tmp')
            temporary.write_text(json.dumps(detail, sort_keys=True), encoding='utf-8')
            os.replace(temporary, path)
        except OSError:
            pass
        if self.callback:
            self._emit(ProgressEvent('acquisition_health',
                f"Text acquisition health: build {self.build}; new saves {saved:,}; "
                f"failures {dict(self.failures)}; responses {detail['response_counts']}; "
                f"mean gate wait {detail['avg_gate_wait_seconds']:.3f}s; "
                f"mean backend transfer/write time {detail['avg_backend_elapsed_seconds']:.3f}s. "
                'Unfinished captures remain checkpointed.', detail=detail))
