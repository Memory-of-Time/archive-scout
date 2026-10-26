"""Connection-local, disk-backed derived generations with atomic publication."""
from contextlib import contextmanager
import sqlite3


@contextmanager
def derived_generation(database, tables, stop_event=None):
    """TEMP shadows serve existing algorithms without copying captured evidence.

    The operation lease owns this connection. Intermediate algorithm commits
    touch TEMP only. Parent-first insertion and foreign_key_check protect the
    single main-database publication transaction.
    """
    database.commit()
    foreign_keys = bool(database.execute('PRAGMA foreign_keys').fetchone()[0])
    database.execute('PRAGMA foreign_keys=OFF')
    try:
        for table in tables:
            sql = database.execute("SELECT sql FROM main.sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()[0]
            database.execute(sql.replace('CREATE TABLE', 'CREATE TEMP TABLE', 1))
        if foreign_keys:
            database.execute('PRAGMA foreign_keys=ON')
        # TEMP foreign keys refer to TEMP parents; disable enforcement while
        # constructing derived rows, then validate MAIN after promotion.
        database.execute('PRAGMA foreign_keys=OFF')
        if stop_event is not None:
            database.set_progress_handler(lambda: int(stop_event.is_set()), 10000)
        yield
        from ..events import Stopped
        if stop_event is not None and stop_event.is_set():
            raise Stopped
        database.commit()
        # Publication cannot be interrupted by callbacks that commit progress.
        with database:
            for table in reversed(tables):
                database.execute(f'DELETE FROM main.{table}')
            for table in tables:
                database.execute(f'INSERT INTO main.{table} SELECT * FROM temp.{table}')
            for table in tables:
                if database.execute(f'PRAGMA main.foreign_key_check({table})').fetchone():
                    raise sqlite3.IntegrityError(f'invalid derived generation: {table}')
    except sqlite3.OperationalError:
        database.rollback()
        from ..events import Stopped
        if stop_event is not None and stop_event.is_set():
            raise Stopped
        raise
    finally:
        database.set_progress_handler(None, 0)
        database.rollback()
        for table in reversed(tables):
            database.execute(f'DROP TABLE IF EXISTS temp.{table}')
        database.commit()
        database.execute('PRAGMA foreign_keys=' + ('ON' if foreign_keys else 'OFF'))
