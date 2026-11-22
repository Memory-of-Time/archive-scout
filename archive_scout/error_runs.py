from __future__ import annotations

import base64
import hashlib
import json
import os
import platform
import re
import shutil
import sys
import threading
import time
import urllib.request
from collections import Counter, OrderedDict, deque
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

from .constants import SCHEMA_VERSION, VERSION
from .events import Stopped
from .utils import atomic_write_text, utc_now


active_error_run = ContextVar('archive_scout_error_run', default=None)
_RUN_NAME = re.compile(r'run-\d{8}T\d{12}-[0-9a-f]{12}')
_URL = re.compile(r'https?://[^\s<>"\x27]+', re.I)
_REPLAY = re.compile(r'^/web/(\d{14})([a-z_]{0,8})/(https?://.+)$', re.I)
_CREDENTIAL = re.compile(
    r'(?i)(\b(?:proxy-authorization|authorization|set-cookie|cookie|password|passwd|'
    r'api[_-]?key|(?:access|refresh)[_-]?token|(?:client[_-]?)?secret|credential|'
    r'session[_-]?id|sid)\b\s*[:=]\s*)[^\r\n,;]+')
_SAFE_QUERY_VALUES = frozenset({
    'from', 'to', 'page', 'pagesize', 'limit', 'offset', 'matchtype', 'output',
    'shownumpages', 'showresumekey', 'resolverevisits', 't', 'f', 'p', 'id',
    'threadid', 'topicid', 'docid', 'fid', 'tid', 'start', 'pn',
})
_SAFE_CDX_ENUMS = frozenset({'exact', 'prefix', 'host', 'domain', 'json', 'text', 'true', 'false'})
_SAFE_SETTING_FIELDS = (
    'from_date', 'to_date', 'workers', 'cdx_delay', 'download_delay', 'page_size',
    'connect_timeout', 'read_timeout', 'max_attempts', 'retries', 'download_scope',
    'max_file_mb', 'rate_limit_base_pause', 'rate_limit_max_pause', 'rate_limit_attempts',
    'rate_limit_max_wait', 'cdx_match_type', 'text_collapse_scope',
    'download_external_redirects', 'skip_slow_index_targets', 'index_target_minutes',
)


def safe_url(value, *, replay=True):
    raw = str(value or '')[:8192]
    if not raw:
        return ''
    try:
        url = urlsplit(raw)
        if url.scheme not in ('http', 'https') or not url.hostname:
            return '[unavailable URL]'
        host = url.netloc.rsplit('@', 1)[-1]
        match = _REPLAY.match(url.path + ('?' + url.query if url.query else '')) if replay else None
        if match:
            prefix = urlunsplit((url.scheme, host, f'/web/{match[1]}{match[2]}/', '', ''))
            return (prefix + safe_url(match[3], replay=False))[:1500]
        path = re.sub(r'(?i)(/(?:token|password|secret|apikey|sessionid)/)[^/]+', r'\1[redacted]', url.path)
        query = []
        for key, item in parse_qsl(url.query, keep_blank_values=True, max_num_fields=64):
            if not item and re.fullmatch(r'(?:tid|fid)-\d+\.html', key, re.I):
                query.append(key)
                continue
            if not re.fullmatch(r'[a-zA-Z0-9_.-]{1,60}', key):
                key = '[redacted key]'
            stable_value = re.fullmatch(r'-?\d{1,24}', item, re.ASCII) or item.casefold() in _SAFE_CDX_ENUMS
            if key.casefold() not in _SAFE_QUERY_VALUES or not stable_value:
                item = '[redacted]'
            query.append(urlencode([(key, item)]))
        return urlunsplit((url.scheme, host, path, '&'.join(query), ''))[:1500]
    except ValueError:
        return '[invalid URL]'


def endpoint_details(value):
    try:
        url = urlsplit(str(value or ''))
        host = (url.hostname or '')[:253]
        port = url.port or {'https': 443, 'http': 80}.get(url.scheme)
    except ValueError:
        return {}
    return {'host': host, 'port': port} if host else {}


def url_identity(value):
    raw = str(value or '')
    return {'original_url': safe_url(raw), 'original_host': endpoint_details(raw).get('host', ''),
            'original_url_sha256': hashlib.sha256(raw.encode('utf-8', 'replace')).hexdigest()}


def request_identity(value):
    result = {'request_url': safe_url(value), 'network_endpoint': endpoint_details(value)}
    try:
        url = urlsplit(str(value or ''))
        replay = _REPLAY.match(url.path + ('?' + url.query if url.query else ''))
        if replay:
            return {**result, **url_identity(replay[3]), 'capture_timestamp': replay[1],
                    'replay_modifier': replay[2] or 'default'}
        for name, parameter in parse_qsl(url.query, max_num_fields=64):
            if name == 'url':
                target = parameter if '://' in parameter else 'http://' + parameter.lstrip('*.')
                return {**result, **url_identity(target),
                        'query_sha256': hashlib.sha256(str(value).encode('utf-8', 'replace')).hexdigest()}
    except ValueError:
        pass
    return result


class ErrorRun:
    KEEP_RUNS = 4
    QUEUE_LIMIT = 512
    EXAMPLE_LIMIT = 64
    FILE_LIMITS = {'failures': 4 * 1024 * 1024, 'timeline': 1024 * 1024}
    FILE_GENERATIONS = 3
    RECORD_LIMIT = 8192
    FLUSH_SECONDS = 1.0
    SNAPSHOT_SECONDS = 30.0

    def __init__(self, config, requested_mode, mode):
        self.root = Path(config.output_dir)
        self.enabled = config.network.diagnostics and mode not in {'diagnostics', 'report', 'backup', 'integrity'}
        self.config = config
        self.mode = mode
        self.requested_mode = requested_mode
        self.directory = None
        self.watchdog = None
        self.status = 'running'
        self.operation_id = None
        self.started = time.monotonic()
        self.queue = deque(maxlen=self.QUEUE_LIMIT)
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.dropped = 0
        self.write_errors = 0
        self.rotations = Counter()
        self.counts = Counter()
        self.examples = OrderedDict()
        self.example_evictions = 0
        self.last_events = {}
        self.stage = 'startup'

    def __enter__(self):
        self.context_token = active_error_run.set(None)
        if not self.enabled:
            return self
        from .downloads.diagnostics import build_fingerprint
        from .downloads.watchdog import OwnerWatchdog

        parent = self.root / 'errors'
        name = f'run-{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}-{uuid4().hex[:12]}'
        self.build = build_fingerprint()
        self.run_info = {'format': 'archive-scout-error-run-1', 'diagnostic_run_id': name,
                         'build_id': self.build, 'started_at': utc_now(), 'app_version': VERSION,
                         'schema_version': SCHEMA_VERSION,
                         'requested_mode': self.requested_mode, 'effective_mode': self.mode}
        try:
            if parent.is_symlink():
                return self
            directory = parent / name
            directory.mkdir(parents=True)
            self.directory = directory
            self._write('run.json', {**self.run_info, **self._settings()})
            atomic_write_text(parent / 'latest-run.json', json.dumps({**self.run_info, 'directory': name}, indent=2) + '\n')
        except (OSError, ValueError):
            self.directory = None
            return self
        active_error_run.set(self)
        self.watchdog = OwnerWatchdog(directory, build_id=self.build, diagnostic_run_id=name)
        self.watchdog.start()
        self.writer = threading.Thread(target=self._run, name='archive-error-journal', daemon=True)
        self.writer.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.directory is None:
                return
            if exc is not None and not isinstance(exc, Stopped):
                frames = []
                while tb is not None and len(frames) < 24:
                    code = tb.tb_frame.f_code
                    frames.append({'file': Path(code.co_filename).name, 'line': tb.tb_lineno, 'function': code.co_name})
                    tb = tb.tb_next
                self.record('failures', 'operation_exception', {'error_type': exc_type.__name__,
                    'message': self.safe_text(exc), 'frames': frames, 'stage': self.stage}, group=(self.stage, exc_type.__name__))
            if self.status == 'running':
                self.status = 'interrupted' if isinstance(exc, Stopped) else 'failed' if exc else 'finished'
            self.watchdog.close()
            self.stop.set()
            self.writer.join(timeout=2)
        finally:
            active_error_run.reset(self.context_token)

    def _settings(self):
        cfg = self.config
        proxies = {}
        if cfg.network.trust_environment:
            try:
                proxies = {name: endpoint_details(value) for name, value in urllib.request.getproxies().items()
                           if name in {'http', 'https', 'all'}}
            except (OSError, ValueError):
                pass
        return {'system': {'os': platform.system(), 'release': platform.release(), 'machine': platform.machine(),
                           'python': platform.python_version(), 'cpu_count': os.cpu_count(),
                           'frozen': bool(getattr(sys, 'frozen', False))},
                'settings': {**{name: getattr(cfg, name) for name in _SAFE_SETTING_FIELDS},
                             'network': cfg.network.to_payload(), 'proxy_endpoints': proxies,
                             'target_count': len(cfg.targets),
                             'sampled_targets': [safe_url('http://' + t.lstrip('*.'))[:256] for t in cfg.targets[:16]],
                             'media_enabled': cfg.media.enabled,
                             'keyword_set_count': len(cfg.keyword_sets),
                             'cdx_filter_count': len(cfg.cdx_filters), 'cdx_extra_param_count': len(cfg.cdx_extra_params)}}

    def safe_text(self, value):
        text = str(value or '')[:8192]
        text = text.replace(str(self.root), '[project]')
        text = _URL.sub(lambda m: safe_url(m[0].rstrip('),;')), text)
        text = _CREDENTIAL.sub(r'\1[redacted]', text)
        return text[:1400]

    def exception_details(self, exc):
        from .network.transports import exception_chain

        causes = []
        evidence = {}
        for error in exception_chain(exc):
            details = {'type': type(error).__name__, 'message': self.safe_text(error)[:500]}
            for name in ('errno', 'winerror', 'curl_code', 'status'):
                value = getattr(error, name, None)
                if type(value) is int:
                    details[name] = value
            if len(causes) < 8:
                causes.append(details)
            for name in ('transfer_diagnostic', 'representation_diagnostic'):
                for key, value in getattr(error, name, {}).items():
                    evidence.setdefault(key, value)
        result = {'exception_type': type(exc).__name__, 'exception_chain': causes}
        retained = {}
        for key in ('response_status', 'raw_bytes_received', 'decoded_bytes_received', 'received_sha256',
                    'expected_cdx_digest', 'actual_candidate_digest', 'content_encoding'):
            if key in evidence:
                value = evidence[key]
                retained[key] = value if isinstance(value, (int, float)) else self.safe_text(value)[:100]
        if 'final_url' in evidence:
            retained['final_url'] = safe_url(evidence['final_url'])
        if evidence.get('response_headers'):
            from .network.transports import response_diagnostic

            headers = response_diagnostic(0, evidence['response_headers'], '')['response_headers']
            retained['response_headers'] = {key: self.safe_text(value)[:300] for key, value in headers.items()}
        if evidence.get('raw_prefix_base64'):
            try:
                prefix = base64.b64decode(evidence['raw_prefix_base64'][:16], validate=True)
                retained['wire_prefix_kind'] = 'gzip' if prefix.startswith(b'\x1f\x8b') else 'other'
            except (TypeError, ValueError):
                pass
        if retained:
            result['representation'] = retained
        return result

    def capture_failure(self, row, category, exc, *, kind):
        url = url_identity(row['original_url'])
        data = {**url, 'category': category, 'capture_timestamp': str(row['timestamp']),
                'media_capture_id' if kind == 'media' else 'capture_id': int(row['id']),
                **self.exception_details(exc)}
        for name in ('digest', 'mimetype', 'download_attempts', 'state'):
            if name in row.keys() and row[name] is not None:
                data[name] = row[name] if type(row[name]) is int else self.safe_text(row[name])[:100]
        if 'alternate_replay' in row.keys():
            data['alternate_requested'] = bool(row['alternate_replay'])
        if getattr(exc, 'source', None) and getattr(exc, 'destination', None):
            data.update(redirect_source=safe_url(exc.source), redirect_destination=safe_url(exc.destination))
        self.record('failures', kind + '_capture_outcome', data, group=(category, url['original_host']))

    def backend_failure(self, url, actual_url, backend, phase, exc, elapsed, status, *, probe, expected_digest,
                        dial_target, connected_peer):
        data = request_identity(url)
        data.update(backend=backend, phase=phase, backend_seconds=round(elapsed, 3), recovery_probe=probe,
                    response_status=status, attempted_url=safe_url(actual_url),
                    attempted_endpoint=endpoint_details(actual_url), **self.exception_details(exc))
        if dial_target is not None:
            data['dial_target'] = dial_target
        if connected_peer is not None:
            data['connected_peer'] = connected_peer
        if expected_digest:
            data['expected_cdx_digest'] = expected_digest[:100]
        if getattr(exc, 'source', None) and getattr(exc, 'destination', None):
            data.update(redirect_source=safe_url(exc.source), redirect_destination=safe_url(exc.destination),
                        redirect_policy=getattr(exc, 'category', ''))
        self.record('failures', 'backend_failure', data, group=(backend, phase, data.get('original_host', '')))

    def error_row(self, error_id, operation, category, message, capture_id, document_id, media_capture_id,
                  http_status, retryable, index_scope, attempts):
        self.record('failures', 'error_row', {'error_id': error_id, 'operation': operation, 'category': category,
                    'message': self.safe_text(message), 'capture_id': capture_id, 'document_id': document_id,
                    'media_capture_id': media_capture_id, 'http_status': http_status, 'retryable': retryable,
                    'index_scope': index_scope, 'attempt_count': attempts}, group=(operation, category))

    def retry_scheduled(self, capture_id, category, status, next_eligible_at, count, *, kind):
        self.record('failures', 'retry_scheduled', {'category': category, 'http_status': status,
            'next_eligible_epoch': next_eligible_at, 'failure_count': count, 'kind': kind,
            'media_capture_id' if kind == 'media' else 'capture_id': capture_id})

    def heartbeat(self, stage):
        if self.watchdog is not None:
            self.stage = stage
            self.watchdog.beat(stage)

    def progress(self, event, *, owner):
        if self.directory is None:
            return
        stage = str(event.stage)[:80]
        if owner:
            self.heartbeat(stage)
        now = time.monotonic()
        with self.lock:
            if now - self.last_events.get(stage, -60.0) < 5.0:
                return
            if len(self.last_events) < 128:
                self.last_events[stage] = now
        data = {'stage': stage, 'current': event.current, 'total': event.total, 'owner': owner}
        if stage.startswith(('index', 'scan', 'media_', 'download', 'network', 'rate_limit', 'database', 'resume', 'stopped')):
            data['message'] = self.safe_text(event.message)
        data['metrics'] = {k: v for k, v in (event.detail or {}).items()
                           if isinstance(v, (int, float, bool)) and isinstance(k, str) and len(k) < 60}
        self.record('timeline', 'progress', data)

    def record(self, stream, event, data, *, group=()):
        if self.directory is None or self.stop.is_set():
            return
        record = {**data, 'event': event, 'observed_at': utc_now(),
                  'elapsed_seconds': round(time.monotonic() - self.started, 3), 'operation_run_id': self.operation_id}
        with self.lock:
            if len(self.queue) == self.queue.maxlen:
                self.dropped += 1
            self.queue.append((stream, record))
            key = '|'.join((event, *(str(x)[:253] for x in group)))
            self.counts[key if key in self.counts or len(self.counts) < 256 else 'other_groups'] += 1
            if group:
                example = self.examples.get(key)
                if example is None:
                    if len(self.examples) == self.EXAMPLE_LIMIT:
                        self.examples.popitem(last=False)
                        self.example_evictions += 1
                    example = self.examples[key] = {'first': record, 'count': 0}
                self.examples.move_to_end(key)
                example['latest'] = record
                example['count'] += 1

    def _write(self, name, value):
        atomic_write_text(self.directory / name, json.dumps(value, ensure_ascii=True, sort_keys=True) + '\n')

    def _append(self, stream, records):
        chunks = []
        for record in records:
            line = json.dumps(record, ensure_ascii=True, separators=(',', ':')).encode('ascii') + b'\n'
            if len(line) > self.RECORD_LIMIT:
                minimal = {k: v for k, v in record.items() if k in {
                    'event', 'observed_at', 'elapsed_seconds', 'operation_run_id', 'capture_id',
                    'media_capture_id', 'capture_timestamp', 'error_id', 'category', 'backend', 'phase', 'original_host'}}
                line = json.dumps({**minimal, 'detail_too_large': True}, ensure_ascii=True).encode('ascii') + b'\n'
            chunks.append(line)
        path = self.directory / (stream + '.jsonl')
        limit = self.FILE_LIMITS[stream]
        size = path.stat().st_size if path.exists() else 0
        while chunks:
            if len(chunks[0]) > limit:
                chunks.pop(0)
                self.write_errors += 1
                continue
            if size + len(chunks[0]) > limit:
                for n in range(self.FILE_GENERATIONS - 1, 0, -1):
                    previous = path if n == 1 else path.with_name(f'{path.name}.{n - 1}')
                    if previous.exists():
                        os.replace(previous, path.with_name(f'{path.name}.{n}'))
                self.rotations[stream] += 1
                size = 0
            length = size
            end = 0
            for line in chunks:
                if length + len(line) > limit:
                    break
                length += len(line)
                end += 1
            with path.open('ab') as handle:
                handle.writelines(chunks[:end])
            chunks = chunks[end:]
            size = length

    def _snapshot(self, *, final):
        from .downloads.diagnostics import _process_memory

        with self.lock:
            examples = {key: dict(value) for key, value in self.examples.items()}
            counts = dict(self.counts)
            dropped, evictions = self.dropped, self.example_evictions
        disk = shutil.disk_usage(self.root)
        db_sizes = {}
        for name in ('archive_scout.sqlite3', 'archive_scout.sqlite3-wal'):
            try:
                db_sizes[name] = (self.root / name).stat().st_size
            except OSError:
                pass
        overview = {**self.run_info, 'operation_run_id': self.operation_id, 'status': self.status,
                    'stage': self.stage, 'observed_at': utc_now(), 'elapsed_seconds': round(time.monotonic() - self.started, 3),
                    'final': final, 'event_counts': counts, 'queue_drops': dropped, 'file_write_errors': self.write_errors,
                    'example_evictions': evictions, 'journal_rotations': dict(self.rotations),
                    'runtime': {'cpu_seconds': time.process_time(), 'threads': threading.active_count(),
                                'memory': _process_memory(), 'project_disk_free_bytes': disk.free, 'database_bytes': db_sizes}}
        self._append('timeline', [{k: v for k, v in overview.items() if k != 'event_counts'} | {'event': 'runtime'}])
        self._write('overview.json', overview)
        if examples:
            self._write('failure-examples.json', examples)

    def _prune(self):
        parent = self.directory.parent
        owned = []
        for entry in parent.iterdir():
            if not _RUN_NAME.fullmatch(entry.name) or entry.is_symlink() or not entry.is_dir():
                continue
            marker = entry / 'run.json'
            try:
                if marker.is_symlink() or marker.stat().st_size > 65536:
                    continue
                manifest = json.loads(marker.read_bytes())
                if manifest.get('format') == self.run_info['format'] and manifest.get('diagnostic_run_id') == entry.name:
                    owned.append(entry)
            except (OSError, ValueError, AttributeError):
                continue
        others = sorted((entry for entry in owned if entry != self.directory), reverse=True)
        for entry in others[self.KEEP_RUNS - 1:]:
            shutil.rmtree(entry, ignore_errors=True)

    def _run(self):
        try:
            self._prune()
        except OSError:
            self.write_errors += 1
        next_snapshot = time.monotonic()
        while True:
            final = self.stop.wait(self.FLUSH_SECONDS)
            with self.lock:
                records = list(self.queue)
                self.queue.clear()
            for stream in self.FILE_LIMITS:
                selected = [value for key, value in records if key == stream]
                if selected:
                    try:
                        self._append(stream, selected)
                    except (OSError, ValueError, TypeError):
                        self.write_errors += 1
            if final or time.monotonic() >= next_snapshot:
                try:
                    self._snapshot(final=final)
                except (OSError, ValueError, TypeError):
                    self.write_errors += 1
                next_snapshot = time.monotonic() + self.SNAPSHOT_SECONDS
            if final:
                return
