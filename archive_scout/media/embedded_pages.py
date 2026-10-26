"""Owner-committed pages for all-timestamp embedded media inventories.

One bounded response per worker. Each cursor commits with its media rows, so
an interruption halfway through a prolific embedded URL resumes that page,
instead of retaining every timestamp in RAM or starting the URL over again.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from contextlib import nullcontext
import json
import threading

from ..cdx.client import HttpClient, RateLimitDeferred
from ..cdx.indexer import PendingWindow, cdx_response_budget, validate_cdx_scope
from ..cdx.parallel import memory_limited_page_workers
from ..database.repositories import (iter_media_discovery_rows, upsert_media_captures,
    mark_media_discovery_lookup, record_error, resolve_errors, blocked_site_hosts,
    cdx_row_to_dict, record_site_issue)
from ..downloads.validation import classify_exception
from ..events import ProgressEvent, Stopped
from ..site_status import host_from_url, site_issue_message, should_surface_site_issue


def index_all_embedded_pages(config, database, client, stop_event, callback, signature):
    from .indexer import _request_media_resume, _embedded_row_kind
    media = config.media.normalized()
    proof_key = f'media_embedded_pages_proof_v1:{signature}'
    if not database.execute('SELECT 1 FROM project_meta WHERE key=?', (proof_key,)).fetchone():
        # Older all-snapshot lookups could accept a full page without proving
        # exhaustion. No raw terminal row count was stored, so even a small
        # accepted count cannot certify that later eligible timestamps exist
        # nowhere. Reverify once; preserve captures, payloads and attempt history.
        with database:
            database.execute("""UPDATE media_discovery_queue SET state='pending',result_count=0
                WHERE query_signature=? AND state IN ('indexed','unavailable')
                AND COALESCE(last_error,'')=''""", (signature,))
            database.execute('INSERT INTO project_meta(key,value) VALUES(?,?)', (proof_key, '1'))
    attempt_limit = 2**63-1 if config.network.persistent_retries else config.max_attempts
    candidates = iter(iter_media_discovery_rows(database, signature, attempt_limit))
    workers = memory_limited_page_workers(min(10, config.network.cdx_workers),
        3 * cdx_response_budget(config.page_size), config.network.cdx_memory_budget_mb)
    blocked = blocked_site_hosts(database)
    pool_cancel = threading.Event()

    def checkpoint_key(row):
        return f'media_embed_cursor_v1:{signature}:{row["id"]}'

    def window_for(row):
        saved = database.execute('SELECT value FROM project_meta WHERE key=?',
                                 (checkpoint_key(row),)).fetchone()
        value = json.loads(saved[0]) if saved else {}
        if not isinstance(value, dict):
            raise ValueError('Invalid embedded media cursor; checkpoint retained')
        return PendingWindow(config.from_date, config.to_date,
            resume_key=value.get('resume_key'), page_size=int(value.get('page_size') or 0))

    def fetch(row, window):
        scope = client.cancellation_scope(pool_cancel) if isinstance(client, HttpClient) else nullcontext()
        try:
            with scope:
                result = _request_media_resume(config, client, row['original_url'], window, [], exact=True)
        except RateLimitDeferred:
            pool_cancel.set()
            raise
        validate_cdx_scope(result[0], row['original_url'], window.start, window.end,
                           config.cdx_filters, 'exact')
        return result

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='archive-embedded-page') as pool:
        pending = {}

        def submit(row):
            window = window_for(row)
            pending[pool.submit(fetch, row, window)] = (row, window)

        def fill():
            while len(pending) < workers and not pool_cancel.is_set():
                if stop_event.is_set():
                    raise Stopped
                row = next(candidates, None)
                if row is None:
                    return
                if host_from_url(row['original_url']) in blocked:
                    with database:
                        mark_media_discovery_lookup(database, row['id'], 'unavailable',
                            error='Host is excluded or robots-blocked in Wayback', increment_attempt=False)
                    continue
                submit(row)

        try:
            fill()
            while pending:
                if stop_event.is_set():
                    raise Stopped
                done, _ = wait(pending, timeout=0.25, return_when=FIRST_COMPLETED)
                for future in done:
                    row, window = pending.pop(future)
                    try:
                        rows, finished, cursor = future.result()
                    except RateLimitDeferred:
                        raise
                    except Stopped:
                        if pool_cancel.is_set() and not stop_event.is_set():
                            continue
                        raise
                    except Exception as exc:
                        category, status, retryable = classify_exception(exc)
                        host = host_from_url(row['original_url'])
                        message = site_issue_message(category, row['original_url'], 'embedded-media indexing', status)
                        with database:
                            mark_media_discovery_lookup(database, row['id'],
                                'error' if retryable else 'unavailable',
                                error=str(exc), result_count=int(row['result_count'] or 0))
                            record_error(database, 'media_embed', category,
                                f'{row["original_url"]}: {exc}', document_id=row['source_document_id'],
                                http_status=status, retryable=retryable)
                            if should_surface_site_issue(category):
                                record_site_issue(database, host, 'embedded_media_index', category,
                                    message, target=row['original_url'], http_status=status)
                        if category in {'wayback_excluded', 'robots_blocked'}:
                            blocked.add(host)
                        if callback:
                            callback(ProgressEvent('media_embed',
                                f'Embedded lookup page remains unfinished [{category}]: {row["original_url"]}'))
                        fill()
                        continue
                    accepted = 0

                    def items():
                        nonlocal accepted
                        for compact in rows:
                            value = cdx_row_to_dict(compact)
                            allowed, kind, extension = _embedded_row_kind(value, row['kind_hint'], media)
                            if allowed and kind:
                                accepted += 1
                                yield value, kind, extension

                    # No in-memory cursor is advanced before this transaction
                    # commits. A process failure leaves either the entire page
                    # present or the previous exact resume key still in force.
                    with database:
                        upsert_media_captures(database, items(), None, signature,
                            row['source_document_id'], row['source_type'])
                        count = int(row['result_count'] or 0) + accepted
                        mark_media_discovery_lookup(database, row['id'],
                            ('indexed' if count else 'unavailable') if finished else 'pending',
                            result_count=count, increment_attempt=finished)
                        if finished:
                            database.execute('DELETE FROM project_meta WHERE key=?', (checkpoint_key(row),))
                            if row['source_document_id'] is not None and not database.execute(
                                "SELECT 1 FROM media_discovery_queue WHERE source_document_id=? AND "
                                "(state IN ('pending','error','resolving','blocked') OR "
                                "(state='unavailable' AND COALESCE(last_error,'')!='')) LIMIT 1",
                                (row['source_document_id'],)).fetchone():
                                resolve_errors(database, document_id=row['source_document_id'], operations=('media_embed',))
                        else:
                            database.execute('INSERT OR REPLACE INTO project_meta(key,value) VALUES(?,?)',
                                (checkpoint_key(row), json.dumps({'resume_key':cursor,'page_size':window.page_size})))
                    rows.clear()
                    if callback:
                        callback(ProgressEvent('media_embed',
                            f'Embedded media page committed: {count:,} captures; URL complete={finished}; {row["original_url"]}'))
                    if not finished:
                        continued = dict(row); continued['result_count'] = count
                        submit(continued)
                    fill()
        finally:
            pool_cancel.set()
            for future in pending:
                future.cancel()
