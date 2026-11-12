"""Transactional dashboard summaries; refresh work is independent of project size."""
from __future__ import annotations

import sqlite3


def _bucket(table, values, delta):
    columns = ('state,routing,kind' if table == 'dashboard_capture_counts' else 'state,kind')
    if delta > 0:
        return f"INSERT INTO {table}({columns},n) SELECT {values},1 ON CONFLICT({columns}) DO UPDATE SET n=n+1;"
    keys = columns.split(',')
    return (f"UPDATE {table} SET n=n-1 WHERE ({columns})=({values});"
            f"DELETE FROM {table} WHERE n=0;")


def _capture_values(ref):
    return (f"{ref}.state,COALESCE((SELECT routing FROM capture_routing WHERE capture_id={ref}.id),''),"
            f"COALESCE((SELECT resource_class FROM capture_routing WHERE capture_id={ref}.id),'unknown')")


def _refresh_unavailable(identity):
    from ..downloads.outcomes import text_unavailable
    eligible = f"SELECT c.id FROM captures c WHERE c.id={identity} AND " + text_unavailable()
    return (f"DELETE FROM dashboard_unavailable_captures WHERE capture_id={identity} AND NOT EXISTS({eligible});"
            f"INSERT OR IGNORE INTO dashboard_unavailable_captures {eligible};")


def initialize_dashboard(database: sqlite3.Connection, *, rebuild=False) -> None:
    if not rebuild and database.execute("SELECT 1 FROM project_meta WHERE key='dashboard_summary_v1'").fetchone():
        return
    from ..downloads.outcomes import text_unavailable
    statements = ["BEGIN IMMEDIATE;"]
    if rebuild:
        for row in database.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name GLOB 'dashboard_*'"):
            statements.append('DROP TRIGGER "'+row[0].replace('"','""')+'";')
        for name in ('dashboard_capture_counts','dashboard_media_counts','dashboard_scalar_counts','dashboard_unavailable_captures'):
            statements.append(f'DROP TABLE IF EXISTS {name};')
        statements.append("DELETE FROM project_meta WHERE key='dashboard_summary_v1';")
    statements += ["""
        CREATE TABLE dashboard_capture_counts(state TEXT,routing TEXT,kind TEXT,n INTEGER NOT NULL,
            PRIMARY KEY(state,routing,kind)) WITHOUT ROWID;
        CREATE TABLE dashboard_media_counts(state TEXT,kind TEXT,n INTEGER NOT NULL,
            PRIMARY KEY(state,kind)) WITHOUT ROWID;
        CREATE TABLE dashboard_scalar_counts(name TEXT PRIMARY KEY,n INTEGER NOT NULL) WITHOUT ROWID;
        CREATE TABLE dashboard_unavailable_captures(capture_id INTEGER PRIMARY KEY);
        INSERT INTO dashboard_capture_counts SELECT c.state,COALESCE(r.routing,''),
            COALESCE(r.resource_class,'unknown'),COUNT(*) FROM captures c
            LEFT JOIN capture_routing r ON r.capture_id=c.id GROUP BY 1,2,3;
        INSERT INTO dashboard_media_counts SELECT state,media_kind,COUNT(*) FROM media_captures GROUP BY 1,2;
        INSERT INTO dashboard_scalar_counts VALUES
          ('matches',(SELECT COUNT(DISTINCT document_id) FROM document_matches WHERE score>0 AND excluded=0 AND required_missing=0)),
          ('errors',(SELECT COUNT(*) FROM errors WHERE resolved=0 AND ignored=0)),
          ('recovery_events',(SELECT COUNT(*) FROM network_events WHERE stage LIKE '%recovery%')),
          ('unavailable_captures',0);
        """
    ]
    def trigger(name, timing, action, table, body, when=''):
        statements.append(f"CREATE TRIGGER {name} {timing} {action} ON {table} "
                          + (f"WHEN {when} " if when else '') + f"BEGIN {body} END;")
    for action, timing, ref, delta in [('INSERT','AFTER','NEW',1),('DELETE','BEFORE','OLD',-1)]:
        body = _bucket('dashboard_capture_counts',_capture_values(ref),delta)
        if action == 'DELETE':
            body += "DELETE FROM dashboard_unavailable_captures WHERE capture_id=OLD.id;"
        trigger('dashboard_capture_'+action.lower(),timing,action,'captures',body)
    trigger('dashboard_capture_old','BEFORE','UPDATE OF state','captures',_bucket('dashboard_capture_counts',_capture_values('OLD'),-1), 'OLD.state IS NOT NEW.state')
    trigger('dashboard_capture_new','AFTER','UPDATE OF state','captures',_bucket('dashboard_capture_counts',_capture_values('NEW'),1)+_refresh_unavailable('NEW.id'), 'OLD.state IS NOT NEW.state')
    for action in ('INSERT','UPDATE','DELETE'):
        ref = 'OLD' if action == 'DELETE' else 'NEW'
        identity = f'{ref}.capture_id'
        state = f'(SELECT state FROM captures WHERE id={identity})'
        exists = f'EXISTS(SELECT 1 FROM captures WHERE id={identity})'
        old = f"{state}," + ("'', 'unknown'" if action == 'INSERT' else 'OLD.routing,OLD.resource_class')
        new = f"{state}," + ("'', 'unknown'" if action == 'DELETE' else 'NEW.routing,NEW.resource_class')
        trigger('dashboard_route_'+action.lower(),'AFTER',action,'capture_routing',
                _bucket('dashboard_capture_counts',old,-1)+_bucket('dashboard_capture_counts',new,1)+_refresh_unavailable(identity),exists)
    for table, label, values in [('media_captures','media','{ref}.state,{ref}.media_kind')]:
        for action, timing, ref, delta in [('INSERT','AFTER','NEW',1),('DELETE','BEFORE','OLD',-1),('UPDATE','BEFORE','OLD',-1),('UPDATE','AFTER','NEW',1)]:
            trigger(f'dashboard_{label}_{action.lower()}_{ref.lower()}',timing,
                    'UPDATE OF state,media_kind' if action=='UPDATE' else action,table,
                    _bucket('dashboard_media_counts',values.format(ref=ref),delta))
    for table, scalar, condition, columns in [
        ('errors','errors','{ref}.resolved=0 AND {ref}.ignored=0','resolved,ignored'),
        ('network_events','recovery_events',"{ref}.stage LIKE '%recovery%'",'stage')]:
        for action, timing, ref, delta in [('INSERT','AFTER','NEW',1),('DELETE','BEFORE','OLD',-1),('UPDATE','BEFORE','OLD',-1),('UPDATE','AFTER','NEW',1)]:
            trigger(f'dashboard_{scalar}_{action.lower()}_{ref.lower()}',timing,
                    'UPDATE OF '+columns if action=='UPDATE' else action,table,
                    f"UPDATE dashboard_scalar_counts SET n=n+({delta}) WHERE name='{scalar}';",condition.format(ref=ref))
    for action in ('INSERT','DELETE','UPDATE'):
        ref='OLD' if action=='DELETE' else 'NEW'
        body=_refresh_unavailable(f'{ref}.capture_id')
        if action=='UPDATE': body=_refresh_unavailable('OLD.capture_id')+body
        trigger('dashboard_error_evidence_'+action.lower(),'AFTER',
                'UPDATE OF capture_id,operation,category,retryable,resolved,ignored' if action=='UPDATE' else action,
                'errors',body)
    for action, ref, delta in [('INSERT','NEW',1),('DELETE','OLD',-1)]:
        trigger('dashboard_unavailable_'+action.lower(),'AFTER',action,'dashboard_unavailable_captures',
                f"UPDATE dashboard_scalar_counts SET n=n+({delta}) WHERE name='unavailable_captures';")
    for action, timing, ref, delta in [('INSERT','AFTER','NEW',1),('DELETE','BEFORE','OLD',-1),('UPDATE','BEFORE','OLD',-1),('UPDATE','AFTER','NEW',1)]:
        positive=f'{ref}.score>0 AND {ref}.excluded=0 AND {ref}.required_missing=0'
        other=(f"NOT EXISTS(SELECT 1 FROM document_matches WHERE document_id={ref}.document_id "
               f"AND id!={ref}.id AND score>0 AND excluded=0 AND required_missing=0)")
        trigger(f'dashboard_match_{action.lower()}_{ref.lower()}',timing,
                'UPDATE OF document_id,score,excluded,required_missing' if action=='UPDATE' else action,
                'document_matches',f"UPDATE dashboard_scalar_counts SET n=n+({delta}) WHERE name='matches';",positive+' AND '+other)
    statements += ["INSERT INTO dashboard_unavailable_captures SELECT c.id FROM captures c WHERE "+text_unavailable()+";",
                   "INSERT INTO project_meta VALUES('dashboard_summary_v1','1');", "COMMIT;"]
    try:
        database.commit()
        database.execute('BEGIN IMMEDIATE')
        if not rebuild and database.execute("SELECT 1 FROM project_meta WHERE key='dashboard_summary_v1'").fetchone():
            database.commit()
            return
        pending = ''
        for line in '\n'.join(statements[1:-1]).splitlines(keepends=True):
            pending += line
            if sqlite3.complete_statement(pending):
                database.execute(pending)
                pending = ''
        if pending.strip():
            raise RuntimeError('incomplete dashboard summary schema')
        database.commit()
    except BaseException:
        database.rollback()
        raise


def read_summary(database):
    if not database.execute("SELECT 1 FROM sqlite_master WHERE name='dashboard_capture_counts'").fetchone():
        return None
    if not database.execute("SELECT 1 FROM project_meta WHERE key='dashboard_summary_v1'").fetchone():
        return None
    return (list(database.execute('SELECT state,routing,kind,n FROM dashboard_capture_counts')),
            list(database.execute('SELECT state,kind,n FROM dashboard_media_counts')),
            dict(database.execute('SELECT name,n FROM dashboard_scalar_counts')))
