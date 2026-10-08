"""Optional phase plans and bounded, measured history for dashboard estimates.

This observer uses existing progress. It never creates a worker or persists an
item history. Future unknown work stays unknown, particularly CDX/media discovery.
"""
from __future__ import annotations

import hashlib
import json
import math
import time
from collections import deque


_KEY = 'operation_eta_rates_v1'
_IGNORED = {'network', 'retry', 'download_retry', 'warning', 'site_issue', 'log',
            'network_waiting', 'rate_limit_waiting', 'rate_limit', 'network_pause'}


def phase_name(stage):
    if stage.startswith('report'):
        return 'report'
    return {'download_only': 'download', 'media_retry': 'media_download'}.get(stage, stage)


class OperationForecast:
    def __init__(self, database, config, mode):
        self.database, self.config = database, config
        self.plan = self._plan(mode)
        self.position = -1
        self.phase = ''
        self.samples = deque(maxlen=32)
        self.rates = {}
        # Different scan rules/settings must not borrow an unrelated scan rate.
        contract = [config.scan_workers, config.scan_backend,
                    [item.rules for item in config.selected_keyword_sets()]]
        self.contract = hashlib.sha256(json.dumps(contract, sort_keys=True).encode()).hexdigest()[:16]
        row = database.execute('SELECT value FROM project_meta WHERE key=?', (_KEY,)).fetchone()
        if row:
            try:
                entries = json.loads(row[0])
                if isinstance(entries, dict):
                    for key, value in list(entries.items())[-32:]:
                        if (isinstance(value, dict) and isinstance(value.get('rate'), (int, float))
                                and math.isfinite(value['rate']) and value['rate'] > 0
                                and isinstance(value.get('observed_at'), (int, float))
                                and 0 <= time.time() - value['observed_at'] <= 30 * 86400):
                            self.rates[key] = value
            except (ValueError, TypeError):
                pass
        self.quantities = {}

    def _plan(self, mode):
        text = {'all', 'external_media_after_scan', 'download', 'retry_errors', 'rescan'}
        if mode == 'backup':
            return ['backup_copy', 'backup_compress', 'backup_verify']
        if mode == 'integrity':
            return ['integrity_database', 'integrity', 'integrity_references', 'integrity_files', 'integrity_report']
        if mode in text:
            plan = (['index'] if mode in {'all', 'external_media_after_scan'} else [])
            plan += (['rescan'] if mode == 'rescan' else ['download', 'scan'])
            if mode == 'external_media_after_scan' or (mode == 'all' and self.config.media.enabled):
                plan += ['media_index', 'media_download']
            plan += ['report']
            if self.config.research.enabled and self.config.research.auto_build and self.config.text_retention != 'discard_after_scan':
                plan += ['research_index', 'research_duplicates', 'research_duplicates_publish', 'research_graph']
            return plan
        if mode == 'download_only':
            return ['download'] + (['media_index', 'media_download'] if self.config.media.enabled else [])
        if mode == 'media_all':
            return ['media_index', 'media_download']
        if mode in {'analysis', 'forum_rebuild'}:
            plan = ['analysis']
            if mode == 'analysis':
                plan += ['duplicates', 'duplicates_publish']
                if self.config.analysis.compare_snapshots:
                    plan += ['snapshot_differences']
                if self.config.analysis.search_external_assets:
                    plan += ['asset_search']
            return plan + ['report']
        if mode == 'research_index':
            return ['research_index', 'research_duplicates', 'research_duplicates_publish', 'research_graph']
        return [phase_name({'import_folder': 'import', 'retry_download_errors': 'download',
                            'media_retry': 'media_download'}.get(mode, mode))]

    def _key(self, phase):
        return phase + ':' + self.contract

    def _remember(self):
        if len(self.samples) >= 2:
            (start, first), (end, last) = self.samples[0], self.samples[-1]
            if end - start >= .05 and last > first:
                key = self._key(self.phase)
                self.rates.pop(key, None)
                self.rates[key] = {'rate': (last - first) / (end - start), 'observed_at': time.time()}
                while len(self.rates) > 32:
                    self.rates.pop(next(iter(self.rates)))

    def observe(self, event, *, now=None):
        if event.stage in _IGNORED:
            return None
        now = time.monotonic() if now is None else now
        phase = phase_name(event.stage)
        # Counters can restart for another keyword set/report. Never combine
        # them into invented completed work.
        if phase != self.phase or (self.samples and event.current is not None and event.current < self.samples[-1][1]):
            self._remember()
            self.samples.clear()
            self.phase = phase
            if phase in self.plan:
                self.position = self.plan.index(phase)
                self._refresh_quantities(phase)
        if event.current is not None and event.total is not None and event.total > 0:
            if not self.samples or event.current != self.samples[-1][1]:
                self.samples.append((now, event.current))
        if phase == 'download' and event.total is not None:
            # During overlap only the durable backlog still needs a future scan.
            self.quantities['scan'] = max(0, event.total - int((event.detail or {}).get('scan_completed', 0)))
        if phase not in self.plan:
            return None
        future = self.plan[self.position + 1:]
        seconds = 0.0
        unknown = []
        for name in future:
            quantity = self.quantities.get(name)
            rate = self.rates.get(self._key(name), {}).get('rate')
            if quantity == 0:
                continue
            if quantity is None or rate is None:
                unknown.append(name)
            else:
                seconds += quantity / rate
        return {'future_phases': future, 'future_seconds': None if unknown else seconds,
                'unknown_phases': unknown, 'basis': 'measured project history; work may change'}

    def _refresh_quantities(self, phase):
        # At most once per phase transition, never once per progress/item event.
        # Discovery can change totals: no whole-run estimate before it finishes.
        if phase in {'index', 'media_index'}:
            self.quantities.clear()
            return
        if phase == 'backup_copy':
            self.quantities['backup_compress'] = (self.config.output_dir / 'archive_scout.sqlite3').stat().st_size
            self.quantities['backup_verify'] = self.quantities['backup_compress'] * 2
            return
        if phase in {'download', 'scan', 'rescan', 'analysis', 'duplicates'}:
            count = self.database.execute('SELECT COUNT(*) FROM documents').fetchone()[0]
            self.quantities['duplicates'] = self.quantities['research_index'] = count
            self.quantities['snapshot_differences'] = None
            # Reports have multiple outputs/keyword sets; a row count alone is
            # not a complete report work plan. Keep the remaining report unknown.
            self.quantities['report'] = None

    def persist(self):
        self._remember()
        self.database.execute('INSERT INTO project_meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                              (_KEY, json.dumps(self.rates, separators=(',', ':'))))
