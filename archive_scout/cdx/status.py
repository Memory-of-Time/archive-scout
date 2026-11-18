"""Selected-scope indexing explanations, read only at stage/report boundaries."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json

from .parameters import index_scopes


def index_status_rows(database, config):
    from .indexer import uncovered_index_ranges
    for target, scoped, signature in index_scopes(config):
        found = database.execute('SELECT id FROM targets WHERE pattern=?', (target,)).fetchone()
        errors = []
        missing = 0
        next_retry = None
        def pending_error(state):
            nonlocal next_retry
            if state and state[-1]:
                try:
                    pending = json.loads(state[-1]).get("pending", [])
                    times = [float(w["next_eligible_at"]) for w in pending if w.get("next_eligible_at")]
                    if times:
                        candidate = min(times)
                        next_retry = min(next_retry, candidate) if next_retry else candidate
                except (AttributeError, KeyError, TypeError, ValueError):
                    pass
            return state[-2] if state else None
        if found is None:
            missing = 1
        elif scoped.text_collapse_scope == 'year':
            for year in range(scoped.from_year, scoped.to_year + 1):
                state = database.execute('SELECT complete,error_id,resume_key AS plan FROM index_state '
                    'WHERE target_id=? AND query_signature=? AND year=?',
                    (found[0], signature, year)).fetchone()
                if state is None or not state[0]:
                    missing += 1
                    errors.append(pending_error(state))
        else:
            gaps = uncovered_index_ranges(database, found[0], signature, scoped.from_date, scoped.to_date)
            missing = len(gaps)
            for start, end in gaps:
                state = database.execute('SELECT error_id,plan_json AS plan FROM index_coverage '
                    'WHERE target_id=? AND query_signature=? AND complete=0 '
                    'AND range_start<=? AND range_end>=? ORDER BY updated_at DESC LIMIT 1',
                    (found[0], signature, end, start)).fetchone()
                errors.append(pending_error(state))
        categories = set()
        explanations = []
        for error_id in errors:
            error = database.execute('SELECT category,http_status,message FROM errors WHERE id=?',
                                     (error_id,)).fetchone() if error_id else None
            if error:
                categories.add(str(error[0]) + (f' (HTTP {error[1]})' if error[1] else ''))
                explanations.append(str(error[2]))
            else:
                categories.add('no completed response/checkpoint')
        if missing and not categories:
            categories.add('no index checkpoint')
        yield ({'target': target, 'status': 'unfinished' if missing else 'complete',
                'unfinished_scopes': missing, 'reason': '; '.join(sorted(categories)),
                'last_error': '; '.join(dict.fromkeys(explanations)),
                'next_retry_utc': datetime.fromtimestamp(next_retry, timezone.utc).isoformat() if next_retry and 0 < next_retry < 253402300800 else ''}, categories if missing else ())


def text_index_status(database, config, *, collect=True, on_row=None):
    reasons = Counter()
    rows = []
    selected = unfinished = 0
    for row, categories in index_status_rows(database, config):
        selected += 1
        unfinished += row['status'] == 'unfinished'
        reasons.update(categories)
        if collect:
            rows.append(row)
        if on_row is not None:
            on_row(row)
    return {'selected_targets': selected, 'complete_targets': selected - unfinished,
            'unfinished_targets': unfinished, 'reasons': dict(reasons), 'targets': rows}


def format_index_status(status):
    message = (f"Indexing targets: {status['complete_targets']:,}/{status['selected_targets']:,} complete; "
               f"{status['unfinished_targets']:,} unfinished")
    if status['reasons']:
        message += '. Reasons (targets): ' + '; '.join(
            f'{reason}: {count:,}' for reason, count in sorted(status['reasons'].items()))
        message += '. Indexed captures can download; these targets may still have undiscovered captures.'
    return message
