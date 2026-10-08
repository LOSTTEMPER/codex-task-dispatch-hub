"""Incremental, bounded history projections; source records remain in SQLite.

History pages hold at most 200 requests by stable SQLite rowid, never by mutable
status or time. No ledger rows are removed. Dirty markers commit with mutations.
"""
import contextlib

PAGE_SIZE = 200


def install(hub):
    db = hub.db
    with hub.transaction():
        for statement in (
            'CREATE INDEX IF NOT EXISTS events_request_seq ON events(request_id,seq)',
            'CREATE INDEX IF NOT EXISTS events_version_seq ON events(version,seq)',
            'CREATE TABLE IF NOT EXISTS projection_state(id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL, rendered INTEGER NOT NULL)',
            'CREATE TABLE IF NOT EXISTS projection_pages(page INTEGER PRIMARY KEY, revision INTEGER NOT NULL)',
        ):
            db.execute(statement)
        # All DDL and seeding share one write lock; executescript would commit it.
        first = not hub.meta('projection_atomic_install', False)
        db.execute('INSERT OR IGNORE INTO projection_state VALUES(1,1,0)')
        if first:
            # Also repair views missed by the previous non-atomic installer once.
            db.execute('UPDATE projection_state SET revision=revision+1 WHERE id=1')
            db.execute('INSERT OR REPLACE INTO projection_pages SELECT (rowid-1)/?,(SELECT revision FROM projection_state WHERE id=1) FROM requests', (PAGE_SIZE,))
        for table in ('members','versions','version_roles','requests','events','documents','revisions','product_updates'):
            for operation in ('INSERT','UPDATE','DELETE'):
                side = 'OLD' if operation == 'DELETE' else 'NEW'
                history = ''
                if table in ('requests', 'events'):
                    rowid = f'{side}.rowid' if table == 'requests' else f'(SELECT rowid FROM requests WHERE id={side}.request_id)'
                    history = f'''INSERT INTO projection_pages(page,revision)
                        SELECT ({rowid}-1)/{PAGE_SIZE},revision FROM projection_state WHERE {rowid} IS NOT NULL
                        ON CONFLICT(page) DO UPDATE SET revision=excluded.revision;'''
                db.execute(f'''CREATE TRIGGER IF NOT EXISTS projection_{table}_{operation.lower()}
                    AFTER {operation} ON {table} BEGIN
                    UPDATE projection_state SET revision=revision+1 WHERE id=1;
                    {history}
                    END;''')
        db.execute('''CREATE TRIGGER IF NOT EXISTS projection_current_version
            AFTER UPDATE ON meta WHEN NEW.key='current_version' AND OLD.value!=NEW.value BEGIN
            UPDATE projection_state SET revision=revision+1 WHERE id=1; END;''')
        if first:
            hub.set_meta('projection_atomic_install', True)


@contextlib.contextmanager
def batch(hub):
    """Read one consistent snapshot; never clear dirty data from a concurrent writer."""
    if hub.db.in_transaction:
        yield None  # Bootstrap renders once after the outer transaction commits.
        return
    hub.db.execute('BEGIN')
    try:
        state = hub.db.execute('SELECT * FROM projection_state WHERE id=1').fetchone()
        if state['revision'] == state['rendered']:
            yield None
            return
        pages = [r[0] for r in hub.db.execute('SELECT page FROM projection_pages ORDER BY page DESC')]
        yield pages
    except BaseException:
        hub.db.rollback()
        raise
    finally:
        if hub.db.in_transaction:
            hub.db.commit()
    with hub.transaction():
        hub.db.execute('UPDATE projection_state SET rendered=? WHERE id=1', (state['revision'],))
        hub.db.execute('DELETE FROM projection_pages WHERE revision<=?', (state['revision'],))
