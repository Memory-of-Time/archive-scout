"""Distinguish a finished download pass from complete capture acquisition.

Only a latest, explicit archive-unavailable decision settles an error for the
pass. The capture, HTTP evidence and unresolved Errors row remain unchanged.
Unknown/legacy failures, local errors and user size/policy refusals stay pending.
"""
from __future__ import annotations

UNAVAILABLE_CATEGORIES = frozenset({'missing_capture', 'wayback_excluded', 'robots_blocked'})


def text_unavailable(alias='c'):
    return f"""({alias}.state='error' AND EXISTS (
        SELECT 1 FROM capture_routing outcome WHERE outcome.capture_id={alias}.id
        AND outcome.routing IN ('failed_nonretryable:missing_capture',
            'failed_nonretryable:wayback_excluded','failed_nonretryable:robots_blocked')
        AND EXISTS (SELECT 1 FROM errors evidence WHERE evidence.capture_id={alias}.id
            AND evidence.operation='download' AND evidence.retryable=0
            AND evidence.resolved=0 AND evidence.ignored=0
            AND outcome.routing='failed_nonretryable:'||evidence.category)))"""


def media_unavailable(alias='mc'):
    return f"""({alias}.state='error' AND EXISTS (
        SELECT 1 FROM media_retry_decisions outcome WHERE outcome.media_capture_id={alias}.id
        AND outcome.retryable=0 AND outcome.category IN ('missing_capture','wayback_excluded','robots_blocked')
        AND EXISTS (SELECT 1 FROM errors evidence WHERE evidence.media_capture_id={alias}.id
            AND evidence.operation='media_download' AND evidence.retryable=0
            AND evidence.resolved=0 AND evidence.ignored=0
            AND evidence.category=outcome.category)))"""


def media_unavailable_count(database, config):
    from ..media.indexer import media_query_signature
    return int(database.execute("SELECT COUNT(*) FROM media_captures mc WHERE mc.state='error' "
        "AND mc.query_signature=? AND " + media_unavailable(), (media_query_signature(config),)).fetchone()[0])


def text_outcome_counts(database, config):
    from .downloader import _populate_download_scopes
    _populate_download_scopes(database, config)
    row = database.execute("""SELECT
        COALESCE(SUM(c.state IN ('pending','downloading','error')),0),
        COALESCE(SUM(CASE WHEN c.state='error' THEN """ + text_unavailable() + """ ELSE 0 END),0)
        FROM captures c WHERE EXISTS(SELECT 1 FROM targets t JOIN archive_scout_download_scopes s
            ON s.pattern=t.pattern AND s.signature=c.query_signature AND c.timestamp BETWEEN s.start AND s.end
            WHERE t.id=c.target_id OR EXISTS(SELECT 1 FROM capture_target_membership m
                WHERE m.capture_id=c.id AND m.target_id=t.id))""").fetchone()
    return {'unfinished': int(row[0]) - int(row[1]), 'unavailable': int(row[1])}
