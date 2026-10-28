"""Selected-scope indexing explanations, read only at stage/report boundaries."""
from __future__ import annotations

from collections import Counter

from .parameters import cdx_query_signature


def text_index_status(database, config):
    from .indexer import uncovered_index_ranges
    rows = []
    reasons = Counter()
    for target in dict.fromkeys(config.targets):
        scoped = config.for_target(target)
        signature = cdx_query_signature(scoped)
        found = database.execute('SELECT id FROM targets WHERE pattern=?', (target,)).fetchone()
        errors = []
        missing = 0
        if found is None:
            missing = 1
        elif scoped.text_collapse_scope == 'year':
            for year in range(scoped.from_year, scoped.to_year + 1):
                state = database.execute('SELECT complete,error_id FROM index_state '
                    'WHERE target_id=? AND query_signature=? AND year=?',
                    (found[0], signature, year)).fetchone()
                if state is None or not state[0]:
                    missing += 1
                    errors.append(state[1] if state else None)
        else:
            gaps = uncovered_index_ranges(database, found[0], signature, scoped.from_date, scoped.to_date)
            missing = len(gaps)
            for start, end in gaps:
                state = database.execute('SELECT error_id FROM index_coverage '
                    'WHERE target_id=? AND query_signature=? AND complete=0 '
                    'AND range_start<=? AND range_end>=? ORDER BY updated_at DESC LIMIT 1',
                    (found[0], signature, end, start)).fetchone()
                errors.append(state[0] if state else None)
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
        if missing:
            reasons.update(categories)
        rows.append({'target': target, 'status': 'unfinished' if missing else 'complete',
                     'unfinished_scopes': missing, 'reason': '; '.join(sorted(categories)),
                     'last_error': '; '.join(dict.fromkeys(explanations))})
    unfinished = sum(row['status'] == 'unfinished' for row in rows)
    return {'selected_targets': len(rows), 'complete_targets': len(rows) - unfinished,
            'unfinished_targets': unfinished, 'reasons': dict(reasons), 'targets': rows}


def format_index_status(status):
    message = (f"Indexing targets: {status['complete_targets']:,}/{status['selected_targets']:,} complete; "
               f"{status['unfinished_targets']:,} unfinished")
    if status['reasons']:
        message += '. Reasons (targets): ' + '; '.join(
            f'{reason}: {count:,}' for reason, count in sorted(status['reasons'].items()))
        message += '. Indexed captures can download; these targets may still have undiscovered captures.'
    return message
