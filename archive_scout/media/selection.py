"""Exact timestamp proofs for direct media while broad enumeration is unfinished.

Work and proofs use private media_discovery_queue signature namespaces. Proof
identities include the selected timestamp; they never certify domain coverage.
"""
from __future__ import annotations

import concurrent.futures
import fnmatch
import json
from urllib.parse import urlsplit

from ..cdx.client import HttpClient, MalformedCDXResponse, RateLimitDeferred, summarize_cdx_error
from ..cdx.parameters import cdx_endpoints
from ..database.repositories import upsert_media_captures
from ..downloads.rate_limit import SharedFixedRateLimiter, shared_host_gate
from ..downloads.validation import classify_exception
from ..events import ProgressEvent, Stopped
from ..utils import utc_now


def selection_proof_prefix(signature):
    return 'direct-media-selection-v1:' + signature + ':'


def _url_scopes(config, url):
    candidate = urlsplit(url)
    scopes = {}
    for target in config.media.targets or config.targets:
        scoped = config.for_target(target) if target in config.targets else config
        pattern = urlsplit(target if '://' in target else 'http://' + target)
        host = (candidate.hostname or '').casefold().rstrip('.')
        wanted = (pattern.hostname or '').casefold().rstrip('.')
        mode = scoped.cdx_match_type
        host_ok = (host == wanted[2:] or host.endswith('.' + wanted[2:])) if wanted.startswith('*.') else (
            host == wanted or (mode == 'domain' and host.endswith('.' + wanted))
            or host.removeprefix('www.') == wanted.removeprefix('www.'))
        if not host_ok:
            continue
        if pattern.port is not None and pattern.port != (candidate.port or (443 if candidate.scheme == 'https' else 80)):
            continue
        if mode not in {'host', 'domain'}:
            path = (candidate.path or '/') + (('?' + candidate.query) if candidate.query else '')
            wanted_path = (pattern.path or '/') + (('?' + pattern.query) if pattern.query else '')
            if mode == 'prefix' and not wanted_path.endswith('*'):
                wanted_path += '*'
            if not fnmatch.fnmatchcase(path.casefold(), wanted_path.casefold()):
                continue
        key = json.dumps([scoped.from_date, scoped.to_date, scoped.cdx_filters, scoped.cdx_extra_params], sort_keys=True)
        scopes.setdefault(key, scoped)
    return list(scopes.values())


def _lookup_selection(config, client, url, known_timestamp):
    from .indexer import _embedded_lookup, _accept_media_rows
    scopes = _url_scopes(config, url)
    if not scopes:
        raise MalformedCDXResponse('Direct media URL has no matching configured scope', splittable=False)
    candidates = []
    for scoped in scopes:
        _, rows, error = _embedded_lookup(scoped, client, {'original_url': url},
                                         config.media.snapshot_strategy, cdx_endpoints(scoped))
        if error is not None:
            raise error
        if not rows or any(row[1] != url for row in rows):
            raise MalformedCDXResponse('Exact media selection returned no capture or a different URL spelling', splittable=False)
        candidates.extend(rows)
    choose = min if config.media.snapshot_strategy == 'earliest' else max
    chosen = choose(candidates, key=lambda row: row[0])
    if ((config.media.snapshot_strategy == 'earliest' and chosen[0] > known_timestamp)
            or (config.media.snapshot_strategy == 'latest' and chosen[0] < known_timestamp)):
        raise MalformedCDXResponse('Exact media selection contradicts an already indexed timestamp', splittable=False)
    accepted = _accept_media_rows([chosen], config.media)
    if not accepted:
        raise MalformedCDXResponse('Exact selected capture does not satisfy the configured media policy', splittable=False)
    return accepted[0]


def _selection_client(config, stop, callback):
    network = config.network.normalized()
    return HttpClient(SharedFixedRateLimiter(config.cdx_delay), 1,
        min(max(config.read_timeout, 30.0), 120.0), config.user_agent, stop,
        connect_timeout=min(max(config.connect_timeout, 5.0), 15.0),
        read_timeout=min(max(config.read_timeout, 30.0), 120.0), pool_size=network.cdx_workers,
        host_gate=shared_host_gate(config.rate_limit_base_pause, config.rate_limit_max_pause),
        rate_limit_attempts=config.rate_limit_attempts, rate_limit_max_wait=config.rate_limit_max_wait,
        network_backend=network.backend, trust_environment=network.trust_environment,
        network_callback=(lambda message: callback(ProgressEvent('network', message))) if callback else None)


def resolve_direct_media_batches(config, database, signature, stop, callback, states, selected_ids=None):
    prefix = selection_proof_prefix(signature)
    work_signature = prefix + 'work'
    comparison = '<' if config.media.snapshot_strategy == 'earliest' else '>'
    direction = 'ASC' if comparison == '<' else 'DESC'
    clauses = ["mc.query_signature=?", "mc.source_type='cdx'", "mc.target_id IS NOT NULL"]
    params = [signature]
    if states:
        clauses.append('mc.state IN (' + ','.join('?' for _ in states) + ')')
        params.extend(states)
    if selected_ids is not None:
        database.execute('DROP TABLE IF EXISTS temp.archive_scout_direct_selection_ids')
        database.execute('CREATE TEMP TABLE archive_scout_direct_selection_ids(id INTEGER PRIMARY KEY) WITHOUT ROWID')
        database.executemany('INSERT OR IGNORE INTO archive_scout_direct_selection_ids VALUES(?)', ((int(i),) for i in selected_ids))
        clauses.append('EXISTS(SELECT 1 FROM archive_scout_direct_selection_ids s WHERE s.id=mc.id)')
    clauses.append(f'''NOT EXISTS(SELECT 1 FROM media_captures better
        WHERE better.query_signature=mc.query_signature AND better.original_url=mc.original_url
        AND (better.timestamp {comparison} mc.timestamp OR (better.timestamp=mc.timestamp AND better.id {comparison} mc.id)))''')
    clauses.append("NOT EXISTS(SELECT 1 FROM media_discovery_queue p WHERE p.query_signature=?||mc.timestamp AND p.original_url=mc.original_url AND p.state='indexed')")
    params.append(prefix)
    now = utc_now()
    with database:
        database.execute('''INSERT INTO media_discovery_queue(query_signature,original_url,source_type,state,created_at,updated_at)
            SELECT ?,mc.original_url,'cdx_selection_work','pending',?,? FROM media_captures mc WHERE ''' + ' AND '.join(clauses) + '''
            ON CONFLICT(query_signature,original_url) DO UPDATE SET state='pending'
            WHERE media_discovery_queue.state='indexed' ''', (work_signature, now, now, *params))
    database.execute('DROP TABLE IF EXISTS temp.archive_scout_direct_selection_work_ids')
    database.execute('CREATE TEMP TABLE archive_scout_direct_selection_work_ids(id INTEGER PRIMARY KEY) WITHOUT ROWID')
    database.execute('''INSERT OR IGNORE INTO archive_scout_direct_selection_work_ids
        SELECT q.id FROM media_captures mc JOIN media_discovery_queue q
        ON q.query_signature=? AND q.original_url=mc.original_url WHERE ''' + ' AND '.join(clauses), (work_signature, *params))
    database.commit()
    pending = int(database.execute("SELECT COUNT(*) FROM archive_scout_direct_selection_work_ids s JOIN media_discovery_queue q ON q.id=s.id WHERE q.state IN ('pending','error')").fetchone()[0])
    if not pending:
        database.execute('DROP TABLE archive_scout_direct_selection_work_ids')
        return
    if callback:
        callback(ProgressEvent('media_selection', f'Broad media coverage is unfinished. Verifying {pending:,} known URLs for {config.media.snapshot_strategy} acquisition; ready batches will download independently.', 0, pending))
    workers = min(10, config.network.normalized().cdx_workers)
    batch_size = max(1, workers * 2)
    client = _selection_client(config, stop, callback)
    last_id = completed = 0
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix='archive-media-select') as pool:
            while True:
                batch = database.execute("SELECT q.id,q.original_url FROM archive_scout_direct_selection_work_ids s CROSS JOIN media_discovery_queue q ON q.id=s.id WHERE q.state IN ('pending','error') AND q.id>? ORDER BY q.id LIMIT ?", (last_id, batch_size)).fetchall()
                if not batch:
                    break
                last_id = int(batch[-1]['id'])
                remaining = list(batch)
                for attempt in range(max(1, config.max_attempts)):
                    if stop.is_set():
                        raise Stopped
                    futures = {}
                    for queued in remaining:
                        known = database.execute(f'SELECT timestamp,target_id FROM media_captures WHERE query_signature=? AND original_url=? ORDER BY timestamp {direction},id {direction} LIMIT 1', (signature, queued['original_url'])).fetchone()
                        if known:
                            futures[pool.submit(_lookup_selection, config, client, queued['original_url'], known['timestamp'])] = (queued, known['target_id'])
                    failed, ready = [], []
                    while futures:
                        if stop.is_set():
                            raise Stopped
                        done, _ = concurrent.futures.wait(futures, timeout=.25,
                            return_when=concurrent.futures.FIRST_COMPLETED)
                        for future in done:
                            queued, target_id = futures.pop(future)
                            if stop.is_set():
                                raise Stopped
                            try:
                                item = future.result()
                            except Stopped:
                                raise
                            except Exception as exc:
                                _category, _status, retryable = classify_exception(exc)
                                retryable = retryable or isinstance(exc, RateLimitDeferred)
                                with database:
                                    database.execute("UPDATE media_discovery_queue SET state='error',lookup_attempts=lookup_attempts+1,last_error=?,updated_at=? WHERE id=?", (summarize_cdx_error(exc), utc_now(), queued['id']))
                                if retryable and config.network.persistent_retries:
                                    failed.append(queued)
                                if callback:
                                    callback(ProgressEvent('media_selection', f"Exact media selection remains unfinished: {summarize_cdx_error(exc)}"))
                                continue
                            row, kind, extension = item
                            with database:
                                upsert_media_captures(database, [item], target_id, signature)
                                chosen = database.execute('SELECT id,state FROM media_captures WHERE query_signature=? AND original_url=? AND timestamp=?', (signature, row[1], row[0])).fetchone()
                                database.execute("UPDATE media_captures SET state='pending' WHERE id=? AND state='skipped_strategy'", (chosen['id'],))
                                database.execute("UPDATE media_captures SET state='skipped_strategy' WHERE query_signature=? AND original_url=? AND id!=? AND state='pending'", (signature, row[1], chosen['id']))
                                database.execute("INSERT OR REPLACE INTO media_discovery_queue(query_signature,original_url,source_type,kind_hint,state,result_count,created_at,updated_at) VALUES(?,?,'cdx_selection_proof',?,'indexed',1,?,?)", (prefix + row[0], row[1], kind, utc_now(), utc_now()))
                                database.execute("UPDATE media_discovery_queue SET state='indexed',lookup_attempts=lookup_attempts+1,result_count=1,last_error=NULL,updated_at=? WHERE id=?", (utc_now(), queued['id']))
                            ready.append(int(chosen['id']))
                            completed += 1
                        if ready and (len(ready) >= workers or not futures or not done):
                            if callback:
                                callback(ProgressEvent('media_selection', f'Exact timestamp selection verified for {completed:,}/{pending:,} known URLs; acquiring {len(ready):,} ready captures.', completed, pending))
                            yield ready
                            ready = []
                    if not failed or attempt + 1 >= config.max_attempts:
                        break
                    remaining = failed
                    if stop.wait(min(5.0, config.network.retry_base_seconds)):
                        raise Stopped
    finally:
        client.close()
        database.execute('DROP TABLE IF EXISTS temp.archive_scout_direct_selection_work_ids')
        database.execute('DROP TABLE IF EXISTS temp.archive_scout_direct_selection_ids')
