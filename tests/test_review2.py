"""Second independent audit reproductions, using disposable ledgers only."""
import io
import json
from pathlib import Path
import socket
import sqlite3
import threading
import unittest
from unittest.mock import patch

import control
import projections
from lifecycle import ControlEndpoint, request_stop
from worker import Worker
import test_audit_regressions as fixtures


class LedgerFixture(unittest.TestCase):
    setUp = fixtures.LedgerAuditTests.setUp
    tearDown = fixtures.LedgerAuditTests.tearDown
    req = fixtures.LedgerAuditTests.req


class ArchiveTests(LedgerFixture):
    def test_r01_archive_retires_run_wait_and_all_end_paths(self):
        run = self.h.begin('a', {})['run_id']
        request = self.req('a')
        self.h.version_review('manager', dict(decision='archived', summary='stop v1'))
        for state in ('idle', 'submitted', 'needs_user', 'interrupted', 'waiting'):
            result = self.h.end('a', dict(run_id=run, state=state,
                wait_for=[request] if state == 'waiting' else []))
            self.assertEqual(result['state'], 'interrupted')
        self.assertEqual(self.h.get_request(request)['status'], 'cancelled')
        self.assertIsNone(self.h.active_run('a'))
        self.h.version_create('manager', dict(id='v2', title='new', goal='new', tasks={}))
        new = self.h.begin('a', {})['run_id']
        self.assertNotEqual(new, run)
        self.assertEqual(self.h.active_run('a')['version'], 'v2')
        with self.assertRaises(ValueError):
            self.h.update_request('a', dict(request_id=request, expected_revision=1, note='old'))
        self.h.evaluate()
        self.assertFalse(self.h.db.execute("SELECT 1 FROM outbox WHERE version='v1' AND state='pending'").fetchone())

    def test_r01_existing_wait_barrier_retired_and_archive_is_idempotent(self):
        run = self.h.begin('a', {})['run_id']; request = self.req('a')
        self.h.end('a', dict(run_id=run, state='waiting', wait_for=[request]))
        for _ in range(2):
            self.h.version_review('manager', dict(decision='archived', summary='stop'))
        self.assertFalse(self.h.db.execute("SELECT 1 FROM barriers WHERE version='v1' AND fired=0").fetchone())

    def test_r01_archive_failure_rolls_back_entire_interruption(self):
        run = self.h.begin('a', {})['run_id']; request = self.req('a')
        self.h.db.executescript("CREATE TRIGGER fail_archive BEFORE INSERT ON events WHEN NEW.kind='version_archived' BEGIN SELECT RAISE(ABORT,'fixture'); END;")
        with self.assertRaises(sqlite3.IntegrityError):
            self.h.version_review('manager', dict(decision='archived', summary='stop'))
        self.assertEqual(self.h.version()['state'], 'active')
        self.assertEqual(self.h.active_run('a')['id'], run)
        self.assertEqual(self.h.get_request(request)['status'], 'queued')


class AccountingTests(unittest.TestCase):
    setUp = fixtures.BudgetAuditTests.setUp
    tearDown = fixtures.BudgetAuditTests.tearDown
    thread = fixtures.BudgetAuditTests.thread
    event = fixtures.BudgetAuditTests.event
    tokens = fixtures.BudgetAuditTests.tokens
    bind = fixtures.BudgetAuditTests.bind
    used = fixtures.BudgetAuditTests.used

    def test_r05_cancel_before_send_removes_only_own_assignment(self):
        request = self.h.request('manager', dict(to='a', kind='notify', title='work', action='work',
            acceptance='saved', urgent=False, important=True, reason='test', idempotency_key='cancel', budget_id=self.b))
        row = self.h.db.execute('SELECT * FROM outbox WHERE request_id=?', (request['request_id'],)).fetchone()
        worker = Worker(self.h); calls = []
        class IPC:
            def start(self, *args): calls.append(args)
            def close(self): pass
        original = self.h.budgets.prepare_delivery
        def prepare(*args, **kwargs):
            original(*args, **kwargs)
            worker.monitor_stop.set()
        with patch.object(worker, 'live', return_value=(IPC(), 'owner', dict(status='idle', provider='openai'))), patch.object(self.h.budgets, 'prepare_delivery', side_effect=prepare):
            worker.dispatch(row)
        self.assertEqual(calls, [])
        current = self.h.db.execute('SELECT * FROM outbox WHERE id=?', (row['id'],)).fetchone()
        self.assertEqual(current['state'], 'pending')
        self.assertFalse(self.h.db.execute('SELECT 1 FROM budget_assignments WHERE delivery_id=?', (row['id'],)).fetchone())
        self.event('t-a', 'task_started', turn_id='manual'); self.tokens('t-a', 100)
        self.c.collect(); self.assertEqual(self.used(), 0)
        with self.h.transaction():
            self.h.db.execute('INSERT INTO budget_assignments VALUES(?,?,?,?,?,?)', ('t-a', self.b, 'v1', 'work', 'another-delivery', '2000'))
            self.h.budgets.cancel_unsent(row)
        self.assertEqual(self.h.db.execute("SELECT delivery_id FROM budget_assignments WHERE thread_id='t-a'").fetchone()[0], 'another-delivery')

    def test_r05_unknown_assignment_does_not_bind_next_manual_turn(self):
        with self.h.transaction():
            self.h.db.execute('INSERT INTO budget_assignments VALUES(?,?,?,?,?,?)', ('t-a', self.b, 'v1', 'work', 'unknown', '2000'))
        self.event('t-a', 'task_started', turn_id='manual'); self.tokens('t-a', 100)
        self.c.collect(); self.assertEqual(self.used(), 0)
        self.assertFalse(self.h.db.execute("SELECT 1 FROM budget_turns WHERE thread_id='t-a' AND turn_id='manual'").fetchone())

    def test_r05_first_scan_keeps_unmanaged_evidence_for_exact_late_receipt(self):
        with self.h.transaction():
            self.h.db.execute("DELETE FROM budget_sources WHERE thread_id='t-a'")
            self.h.db.execute('INSERT INTO budget_assignments VALUES(?,?,?,?,?,?)', ('t-a', self.b, 'v1', 'work', 'unknown', '2000'))
        self.event('t-a', 'task_started', turn_id='actual'); self.tokens('t-a', 100)
        self.c.collect(); self.assertEqual(self.used(), 0)
        self.assertEqual(self.h.db.execute("SELECT tokens FROM budget_usage WHERE thread_id='t-a'").fetchone()[0], 100)
        with self.h.transaction(): self.h.budgets.receipt_turn({'recipient':'a','id':'unknown'}, 'actual')
        self.c.collect(); self.assertEqual(self.used(), 100)

    def test_r07_invalid_counters_isolate_source_retain_cursor_and_healthy_50(self):
        for bad in (2**80, 2**63, True, -1):
            with self.subTest(counter=bad):
                self.bind(); self.bind('t-b', 'healthy')
                self.tokens('t-a', bad); self.tokens('t-b', 50)
                for _ in range(2): self.c.collect()
                self.assertEqual(self.used(), 50)
                s = self.h.db.execute("SELECT * FROM budget_sources WHERE thread_id='t-a'").fetchone()
                self.assertEqual(s['quality'], 'gap'); self.assertIsNone(s['total'])
                # Reset fixture between malformed inputs without changing production data.
                with self.h.transaction():
                    self.h.db.execute('DELETE FROM budget_usage'); self.h.db.execute('DELETE FROM budget_turns'); self.h.db.execute('DELETE FROM budget_sources')
                for role in ('a', 'b'): (self.root/('t-'+role+'.jsonl')).write_text('')
                self.c.collect()

    def test_r07_sqlite_max_accepted_and_aggregate_overflow_is_source_gap(self):
        self.bind(); self.tokens('t-a', 2**63-1); self.c.collect()
        self.assertEqual(self.used(), 2**63-1)
        self.bind('t-b', 'healthy'); self.tokens('t-b', 1)
        for _ in range(2): self.c.collect()
        self.assertEqual(self.used(), 2**63-1)
        source = self.h.db.execute("SELECT * FROM budget_sources WHERE thread_id='t-b'").fetchone()
        self.assertEqual(source['quality'], 'gap'); self.assertIsNone(source['total'])

    def test_r07_last_and_optional_counters_validate_before_baseline(self):
        self.bind(); self.tokens('t-a', 10); self.c.collect()
        safe = self.h.db.execute("SELECT offset FROM budget_sources WHERE thread_id='t-a'").fetchone()[0]
        self.event('t-a', 'token_count', info={'total_token_usage': {'total_tokens': 20, 'output_tokens': True}, 'last_token_usage': {'total_tokens': 10}})
        for _ in range(2): self.c.collect()
        source = self.h.db.execute("SELECT * FROM budget_sources WHERE thread_id='t-a'").fetchone()
        self.assertEqual(source['offset'], safe); self.assertEqual(source['total'], 10)
        self.assertEqual(source['quality'], 'gap'); self.assertEqual(self.used(), 10)


class ProjectionTests(LedgerFixture):
    def test_r06_migration_serializes_201st_request_with_trigger_install(self):
        db = self.h.db
        template = dict(db.execute('SELECT * FROM requests LIMIT 1').fetchone())
        with self.h.transaction():
            db.execute('DELETE FROM outbox'); db.execute('DELETE FROM requests'); db.execute('DELETE FROM events')
            for i in range(200):
                values = dict(template, id='r'+str(i), idempotency_key='r'+str(i))
                db.execute('INSERT INTO requests('+','.join(values)+') VALUES('+','.join('?' for _ in values)+')', list(values.values()))
        for row in db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'projection_%'").fetchall(): db.execute('DROP TRIGGER '+row[0])
        db.execute('DROP TABLE projection_state'); db.execute('DROP TABLE projection_pages')
        db.execute("DELETE FROM meta WHERE key='projection_atomic_install'"); db.commit()
        other = sqlite3.connect(self.h.state/'hub.sqlite3', timeout=0)
        injected = []; blocked = []
        values = dict(template, id='r200', idempotency_key='r200')
        def insert():
            other.execute('INSERT INTO requests('+','.join(values)+') VALUES('+','.join('?' for _ in values)+')', list(values.values()))
            other.execute("INSERT INTO events(request_id,version,actor,kind,summary,created) VALUES('r200','v1','a','test','race','2000')"); other.commit()
        class Connection:
            def __getattr__(self, name): return getattr(db, name)
            def before(self, sql):
                if 'CREATE TRIGGER' in sql and not injected:
                    injected.append(True)
                    try: insert()
                    except sqlite3.OperationalError: other.rollback(); blocked.append(True)
            def execute(self, sql, *args): self.before(sql); return db.execute(sql, *args)
            def executescript(self, sql): self.before(sql); return db.executescript(sql)
        try:
            self.h.db = Connection(); projections.install(self.h)
            self.assertEqual(blocked, [True], 'writer must not commit inside migration window')
            insert(); self.h.db = db
            self.h.render(); self.h.render()
            page = self.h.root/'docs/history/page-2.md'
            self.assertIn('r200', page.read_text())
        finally: self.h.db = db; other.close()


class StopTests(unittest.TestCase):
    def test_r04_stop_bypasses_hub_even_if_schema_or_write_lock_fails(self):
        with patch.object(control, 'Hub', side_effect=sqlite3.OperationalError('database is locked')) as hub, patch.object(control, 'request_stop', return_value={'stop_requested': True}) as stop, patch('sys.argv', ['control.py', 'stop']), patch('sys.stdout', io.StringIO()):
            control.main()
        hub.assert_not_called(); stop.assert_called_once_with(control.ROOT/'.state/worker-control.sock')

    def test_r04_real_endpoint_stop_with_database_write_lock(self):
        import tempfile
        from hub import Hub
        with tempfile.TemporaryDirectory(dir='/tmp') as root:
            root = Path(root); h = Hub(root); stopped = threading.Event()
            endpoint = ControlEndpoint(h.state/'worker-control.sock', stopped.set); endpoint.start()
            try:
                h.db.execute('BEGIN IMMEDIATE')
                with patch.object(control, 'ROOT', root), patch('sys.argv', ['control.py', 'stop']), patch('sys.stdout', io.StringIO()) as output:
                    control.main()
                self.assertTrue(json.loads(output.getvalue())['stop_requested']); self.assertTrue(stopped.is_set())
            finally: h.db.rollback(); endpoint.close(); h.close()

    def test_r08_client_accepts_fragmented_response_and_rejects_oversize(self):
        class Sock:
            def __init__(self, parts): self.parts = iter(parts)
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def settimeout(self, value): pass
            def connect(self, path): pass
            def sendall(self, data): pass
            def recv(self, size): return next(self.parts, b'')
        with patch('lifecycle.socket.socket', return_value=Sock([b'{"stop_', b'requested":true}', b'\n'])):
            self.assertTrue(request_stop('/tmp/unused')['stop_requested'])
        with patch('lifecycle.socket.socket', return_value=Sock([b'x'*1025])):
            with self.assertRaises((ValueError, ConnectionError)): request_stop('/tmp/unused')
        with patch('lifecycle.socket.socket', return_value=Sock([b'{"stop_requested":true}'])):
            with self.assertRaises(ConnectionError): request_stop('/tmp/unused')

    def test_r08_real_server_accepts_fragmented_command(self):
        import tempfile
        import time
        with tempfile.TemporaryDirectory(dir='/tmp') as root:
            path = Path(root)/'control.sock'; stopped = threading.Event()
            endpoint = ControlEndpoint(path, stopped.set); endpoint.start()
            try:
                with socket.socket(socket.AF_UNIX) as client:
                    client.settimeout(2); client.connect(str(path)); client.sendall(b'st')
                    time.sleep(.02); client.sendall(b'op\n')
                    reply = client.recv(1024)
                    self.assertTrue(json.loads(reply)['stop_requested'])
                self.assertTrue(stopped.is_set())
            finally: endpoint.close()
