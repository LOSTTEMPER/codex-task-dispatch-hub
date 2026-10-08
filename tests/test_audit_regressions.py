"""Synthetic regression fixtures for audit F01–F23. No real Codex execution."""
import io
import json
import os
from pathlib import Path
import socket
import sqlite3
import struct
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import bootstrap
import control
import test_bootstrap as bootstrap_fixture
import test_budget as budget_fixture
import test_hub as hub_fixture
import test_team_cycles as cycle_fixture
from hub import Hub, dump, now
from transport import DesktopIPC, RPCError
from usage import UsageCollector
from worker import Worker
from lifecycle import ControlEndpoint, request_stop


class FrameTests(unittest.TestCase):
    def ipc(self, data, chunk=4096):
        class Sock:
            def __init__(self): self.offset=0; self.timeouts=[]
            def recv(self,n):
                part=data[self.offset:self.offset+min(chunk,n)];self.offset+=len(part);return part
            def settimeout(self,n):self.timeouts.append(n)
            def sendall(self,n):pass
        ipc=DesktopIPC.__new__(DesktopIPC);ipc.socket=Sock();ipc.client='test';return ipc

    def test_f01_exact_fragmented_1_to_16_mib(self):
        for size in (1,4,8,16):
            data=b'01234567'*(size*1024*1024//8)
            for chunk in (4096,65536):
                with self.subTest(size=size,chunk=chunk):
                    self.assertEqual(self.ipc(data,chunk)._read_exact(len(data)),data)

    def test_f01_eof_and_frame_cap(self):
        with self.assertRaises(ConnectionError):self.ipc(b'abc',1)._read_exact(4)
        with self.assertRaises(RPCError):self.ipc(b'')._read_exact(DesktopIPC.MAX_FRAME+1)

    def test_f05_absolute_deadline_inside_fragmented_frame(self):
        ipc=self.ipc(b'x'*80000);clock=[0.0];recv=ipc.socket.recv
        def slow(n):clock[0]+=1;return recv(n)
        ipc.socket.recv=slow
        with patch('transport.time.monotonic',side_effect=lambda:clock[0]):
            with self.assertRaises(TimeoutError):ipc._read_exact(80000,12)
        self.assertEqual(clock[0],12)
        self.assertTrue(all(x<=.25 for x in ipc.socket.timeouts))

    def test_f05_expiry_after_json_decode_and_cancellation(self):
        ipc=self.ipc(struct.pack('<I',2)+b'{}');clock=[0.0]
        def decode(_):clock[0]=13;return {}
        with patch('transport.time.monotonic',side_effect=lambda:clock[0]), patch('transport.json.loads',side_effect=decode):
            with self.assertRaises(TimeoutError):ipc._frame(12)
        ipc.cancel=threading.Event();ipc.cancel.set()
        with self.assertRaises(InterruptedError):ipc._read_exact(1)

    def test_f05_partial_write_has_absolute_deadline(self):
        ipc=self.ipc(b'');clock=[0.0]
        def send(data):clock[0]+=1;return 1
        ipc.socket.send=send
        with patch('transport.time.monotonic',side_effect=lambda:clock[0]):
            with self.assertRaises(TimeoutError):ipc._write({'test':'message'},3)
        self.assertEqual(clock[0],3)

    def test_f05_cancel_interrupts_real_socket_wait(self):
        left,right=socket.socketpair();ipc=DesktopIPC.__new__(DesktopIPC);ipc.socket=left;ipc.cancel=threading.Event()
        timer=threading.Timer(.05,ipc.cancel.set);timer.start();start=time.monotonic()
        try:
            with self.assertRaises(InterruptedError):ipc._read_exact(100,time.monotonic()+5)
            self.assertLess(time.monotonic()-start,1)
        finally:left.close();right.close();timer.join()


class LedgerAuditTests(unittest.TestCase):
    setUp=hub_fixture.HubTests.setUp
    tearDown=hub_fixture.HubTests.tearDown
    start=hub_fixture.HubTests.start
    req=hub_fixture.HubTests.req

    def row(self, role='a'):
        return self.h.db.execute('SELECT * FROM outbox WHERE recipient=? ORDER BY created LIMIT 1',(role,)).fetchone()

    def test_f02_busy_active_run_zero_ipc_and_f15_native_requires_proof(self):
        self.start('a')
        with self.h.transaction():
            self.h.db.execute("UPDATE outbox SET state='delivered',route='desktop-owner' WHERE recipient='a'")
            self.h.db.execute("UPDATE outbox SET state='delivered',route='desktop-native',last_error='needs proof' WHERE recipient='b'")
        w=Worker(self.h)
        with patch('worker.DesktopIPC',side_effect=AssertionError('unnecessary snapshot')):
            for _ in range(5):w.refresh_active()
        self.assertEqual(self.row('b')['state'],'delivered');self.assertEqual(self.row('b')['last_error'],'needs proof')

    def test_f03_migration_index_no_change_no_rebuild_and_atomic_rollback(self):
        # Simulate pre-migration indexes/projection markers on an existing ledger.
        with self.h.transaction():
            self.h.db.execute('DROP INDEX events_request_seq')
            self.h.db.execute('UPDATE projection_state SET rendered=0')
        root=self.h.root;self.h.close();self.h=Hub(root);self.h.render()
        plan=self.h.db.execute("EXPLAIN QUERY PLAN SELECT * FROM events WHERE request_id=? ORDER BY seq",('x',)).fetchall()
        self.assertIn('events_request_seq',str([tuple(x) for x in plan]))
        paths=list((root/'docs').rglob('*.md'));before={p:p.read_bytes() for p in paths}
        with patch.object(self.h,'_render_dirty',side_effect=AssertionError('rebuilt clean projection')):
            self.h.render()
            with self.h.transaction():self.h.set_meta('worker',{'heartbeat':'new'})
            self.h.render()
        self.assertEqual(before,{p:p.read_bytes() for p in paths})
        state=tuple(self.h.db.execute('SELECT * FROM projection_state').fetchone())
        with self.assertRaises(ValueError):
            with self.h.transaction():
                self.h.event('manager','example','rolled back',self.row()['request_id'],'v1')
                raise ValueError('rollback')
        self.assertEqual(tuple(self.h.db.execute('SELECT * FROM projection_state').fetchone()),state)

    def test_f03_only_affected_history_page_rebuilt(self):
        seed=dict(self.h.get_request(self.row()['request_id']))
        with self.h.transaction():
            for i in range(210):
                item=dict(seed,id='synthetic-'+str(i),idempotency_key='synthetic-'+str(i))
                self.h.db.execute('INSERT INTO requests('+','.join(item)+') VALUES('+','.join('?' for _ in item)+')',list(item.values()))
        self.h.render();page=self.h.root/'docs/history/page-1.md';before=page.stat().st_mtime_ns
        with self.h.transaction():self.h.event('manager','fix','new entry','synthetic-209','v1')
        self.h.render()
        self.assertEqual(page.stat().st_mtime_ns,before)
        self.assertIn('new entry',(self.h.root/'docs/history/page-2.md').read_text())
        for item in self.h.db.execute('SELECT id,action,acceptance FROM requests'):
            text=''.join(p.read_text() for p in (self.h.root/'docs/history').glob('*.md'))
            self.assertIn(item['id'],text);self.assertIn(item['action'],text);self.assertIn(item['acceptance'],text)

    def test_f03_concurrent_change_during_render_remains_dirty(self):
        other=Hub(self.h.root)
        try:
            with self.h.transaction():self.h.event('manager','before','first',self.row()['request_id'],'v1')
            original=self.h._render_dirty
            def during(*args):
                original(*args)
                with other.transaction():other.event('manager','during','second',self.row()['request_id'],'v1')
            with patch.object(self.h,'_render_dirty',side_effect=during):self.h.render()
            self.assertTrue(self.h.db.execute('SELECT 1 FROM projection_pages').fetchone())
            self.h.render()
            self.assertIn('second',(self.h.root/'docs/history/page-1.md').read_text())
        finally:other.close()

    def test_f09_renderer_blocks_legacy_bad_role_and_symlink(self):
        marker=self.h.root/'README.md';marker.write_text('keep')
        with self.h.transaction():self.h.db.execute('INSERT INTO members VALUES(?,?,?,?,?)',('../../README','bad','bad','{}',now()))
        with self.assertRaises(ValueError):self.h.render()
        self.assertEqual(marker.read_text(),'keep')
        with self.h.transaction():self.h.db.execute("DELETE FROM members WHERE role='../../README'")
        page=self.h.root/'docs/roles/a.md';page.unlink();page.symlink_to(marker)
        with self.assertRaises(ValueError):self.h.render()
        self.assertEqual(marker.read_text(),'keep')

    def test_f11_closed_old_request_no_result_update_or_dispatch(self):
        row=self.row();req=row['request_id']
        self.h.version_review('manager',{'decision':'archived','summary':'end'})
        self.h.version_create('manager',dict(id='v2',title='next',goal='next',tasks={}))
        run=self.start('a')
        with self.assertRaises(ValueError):self.h.end('a',dict(run_id=run,state='idle',results=[dict(request_id=req,summary='done')]))
        with self.assertRaises(ValueError):self.h.update_request('manager',dict(request_id=req,expected_revision=1,note='change',requires_action=True))
        self.assertNotEqual(self.h.get_request(req)['status'],'done')
        with self.h.transaction():
            for kind in ('request_changed','budget_resume','dependency_cycle'):
                self.h.enqueue('b',kind,'closed-'+kind,{},version='v1')
        with patch('worker.DesktopIPC',side_effect=AssertionError('closed must not send')):
            for row in self.h.db.execute("SELECT * FROM outbox WHERE version='v1'").fetchall():
                self.assertFalse(Worker(self.h).dispatch(row))
        self.assertEqual(self.h.call('thread-manager','delivery_candidates',{})['deliveries'],[])

    def test_f12_late_negative_receipt_never_downgrades_begin(self):
        row=self.row();self.h.call('thread-manager','delivery_claim_native',dict(delivery_id=row['id']))
        self.start('a',[row['request_id']])
        result=self.h.call('thread-manager','delivery_receipt_native',dict(delivery_id=row['id'],confirmed=False))
        self.assertEqual(result['state'],'delivered')
        self.h.call('thread-manager','delivery_receipt_native',dict(delivery_id=row['id'],confirmed=True,turn_id='sent'))
        self.h.call('thread-manager','delivery_receipt_native',dict(delivery_id=row['id'],confirmed=False))
        self.assertEqual(self.row()['turn_id'],'sent')

    def test_f13_recover_does_not_take_native_claim(self):
        row=self.row();self.h.call('thread-manager','delivery_claim_native',dict(delivery_id=row['id']))
        Worker(self.h).recover();self.assertEqual(self.row()['state'],'sending')
        result=self.h.call('thread-manager','delivery_receipt_native',dict(delivery_id=row['id'],confirmed=True,turn_id='sent'))
        self.assertEqual(result['state'],'delivered')

    def add_candidates(self, busy=False):
        with self.h.transaction():
            self.h.db.execute("UPDATE outbox SET state='archived'")
            for i in range(10):
                role='role'+str(i);self.h.db.execute('INSERT INTO members VALUES(?,?,?,?,?)',(role,'thread-'+role,role,'{}',now()))
                self.h.enqueue(role,'probe',role,{},version='v1')
                if busy and i<8:
                    self.h.db.execute('INSERT INTO runs(id,role,version,state,started) VALUES(?,?,?,?,?)',(role,role,'v1','working',now()))

    def test_f14_ledger_busy_filtered_before_limit(self):
        self.add_candidates(True)
        result=self.h.call('thread-manager','delivery_candidates',{})['deliveries']
        self.assertEqual({r['recipient'] for r in result},{'role8','role9'})

    def test_f14_native_busy_candidates_rotate(self):
        self.add_candidates()
        seen=set()
        for _ in range(2):seen.update(r['recipient'] for r in self.h.call('thread-manager','delivery_candidates',{})['deliveries'])
        self.assertEqual(len(seen),10)

    def test_f17_failed_monitor_initialization_falls_back_visibly(self):
        w=Worker(self.h)
        with patch('worker.Hub',side_effect=sqlite3.OperationalError('locked')):
            w.start_monitor();w.monitor.join(1)
        self.assertFalse(w.monitor.is_alive())
        with patch.object(w.collector,'collect') as collect, patch.object(w,'dispatch'), patch.object(w,'refresh_active'):
            w.tick();collect.assert_called_once()
        self.assertEqual(self.h.meta('budget_monitor')['state'],'fallback')

    def test_f05_cancelled_tick_has_no_ipc_or_render(self):
        w=Worker(self.h);w.monitor_stop.set()
        with patch.object(w,'live',side_effect=AssertionError()),patch.object(self.h,'render',side_effect=AssertionError()):w.tick()

    def test_f06_missing_turn_link_requires_exact_message_and_fresh_ledger(self):
        row=self.row();claim=self.h.call('thread-manager','delivery_claim_native',dict(delivery_id=row['id']))
        self.h.call('thread-manager','delivery_receipt_native',dict(delivery_id=row['id'],confirmed=True))
        args=dict(delivery_id=row['id'],expected_thread_id='thread-a')
        ticket=self.h.call('thread-manager','delivery_link_native_prepare',args)
        proof=dict(thread_id='thread-a',turn_id='verified',user_message='unrelated')
        with self.assertRaises(ValueError):self.h.call('thread-manager','delivery_link_native_commit',dict(args,token=ticket['token'],proof=proof))
        proof['user_message']=claim['message']
        result=self.h.call('thread-manager','delivery_link_native_commit',dict(args,token=ticket['token'],proof=proof))
        self.assertEqual(result['turn_id'],'verified');self.assertEqual(self.row()['state'],'delivered')


class BudgetAuditTests(unittest.TestCase):
    setUp=budget_fixture.BudgetTests.setUp
    tearDown=budget_fixture.BudgetTests.tearDown
    thread=budget_fixture.BudgetTests.thread
    event=budget_fixture.BudgetTests.event
    tokens=budget_fixture.BudgetTests.tokens
    bind=budget_fixture.BudgetTests.bind
    used=budget_fixture.BudgetTests.used
    report=budget_fixture.BudgetTests.report
    decide=budget_fixture.BudgetTests.decide

    def reset_source(self):
        with self.h.transaction():self.h.db.execute("DELETE FROM budget_sources WHERE thread_id='t-a'")

    def test_f18_first_bound_usage_100_survives_rescan(self):
        self.reset_source();self.bind();self.tokens('t-a',100)
        for _ in range(2):self.c.collect();self.assertEqual(self.used(),100)
        self.assertEqual(self.h.budgets.view(self.b)['coverage'],'complete')

    def test_f18_first_confirmed_assignment_excludes_old_history(self):
        self.reset_source()
        self.event('t-a','task_started',turn_id='old');self.tokens('t-a',900)
        with self.h.transaction():
            self.h.db.execute('INSERT INTO budget_assignments VALUES(?,?,?,?,?,?)',('t-a',self.b,'v1','work','delivery','2000'))
        # An assignment alone is no longer evidence of a delivered native turn.
        p=self.root/'t-a.jsonl';p.write_text(p.read_text().replace(now(),'1999'))
        self.event('t-a','task_started',turn_id='new');self.tokens('t-a',1000,100)
        with self.h.transaction():
            self.h.budgets.receipt_turn({'recipient':'a','id':'delivery'}, 'new')
        self.c.collect();self.assertEqual(self.used(),100)

    def test_f19_first_missing_file_recovers_but_replacement_does_not(self):
        self.reset_source();path=self.root/'t-a.jsonl';path.unlink();self.c.collect()
        self.bind();self.tokens('t-a',100);self.c.collect();self.assertEqual(self.used(),100)
        self.assertEqual(self.h.budgets.view(self.b)['coverage'],'complete')
        new=path.with_suffix('.new');new.write_text(path.read_text());os.replace(new,path)
        self.c.collect();self.assertEqual(self.used(),100)
        self.assertEqual(self.h.budgets.view(self.b)['coverage'],'incomplete')

    def test_f19_first_missing_metadata_recovers(self):
        self.reset_source();self.catalog.execute("DELETE FROM threads WHERE id='t-a'");self.catalog.commit();self.c.collect()
        self.thread('t-a');self.bind();self.tokens('t-a',100);self.c.collect();self.assertEqual(self.used(),100)

    def test_f20_bad_shapes_isolated_and_cursor_retained(self):
        self.bind('t-b','healthy');self.tokens('t-b',50)
        for bad in ([],{'type':'event_msg','payload':[]},{'type':'event_msg','payload':{'type':'token_count','info':[]}}):
            (self.root/'t-a.jsonl').write_text(dump(bad)+'\n')
            self.c.collect();self.assertEqual(self.used(),50)
            row=self.h.db.execute("SELECT quality,offset FROM budget_sources WHERE thread_id='t-a'").fetchone()
            self.assertEqual(tuple(row),('gap',0))

    def test_f20_sql_failure_rolls_back_usage_and_offset_retry_exactly_once(self):
        self.bind();self.tokens('t-a',100);self.tokens('t-a',150,50)
        self.h.db.executescript("CREATE TRIGGER fail_usage BEFORE INSERT ON budget_usage WHEN NEW.tokens=50 BEGIN SELECT RAISE(ABORT,'test failure'); END;")
        self.c.collect();self.assertEqual(self.used(),0)
        self.assertEqual(self.h.db.execute("SELECT offset FROM budget_sources WHERE thread_id='t-a'").fetchone()[0],0)
        self.h.db.execute('DROP TRIGGER fail_usage')
        for _ in range(2):self.c.collect();self.assertEqual(self.used(),150)
        root=self.h.root;self.h.close();self.h=Hub(root);self.c=UsageCollector(self.h,self.root/'catalog.db');self.c.collect();self.assertEqual(self.used(),150)

    def test_f21_cross_version_and_closed_budget_begin_rejected(self):
        with self.h.transaction():self.h.db.execute("UPDATE budgets SET state='closed' WHERE id=?",(self.b,))
        with self.assertRaises(ValueError):self.h.begin('a',dict(budget_id=self.b))
        self.assertIsNone(self.h.active_run('a'))
        with self.h.transaction():self.h.db.execute("UPDATE budgets SET state='active' WHERE id=?",(self.b,))
        self.h.version_review('manager',dict(decision='archived',summary='next'))
        self.h.version_create('manager',dict(id='v2',title='next',goal='next',tasks={}))
        with self.assertRaises(ValueError):self.h.begin('a',dict(budget_id=self.b))

    def test_f22_material_report_revision_blocks_stale_decision_no_extra_notice(self):
        r=self.report()
        self.h.budgets.call('a','budget_report',dict(budget_id=self.b,review_id=r['review_id'],completed='saved',evidence='fixture',remaining='new scope',recommendation='9000',additional_tokens=9000))
        with self.assertRaises(ValueError):self.decide(r)
        self.assertEqual(self.h.budgets.view(self.b)['review']['revision'],2)
        self.assertEqual(self.h.db.execute("SELECT COUNT(*) FROM outbox WHERE kind='budget_review' AND state='pending'").fetchone()[0],1)

    def test_f23_close_invalidates_review_and_prevents_reactivation(self):
        r=self.report();self.h.budgets.call('manager','budget_close',dict(budget_id=self.b))
        with self.assertRaises(ValueError):self.decide(r)
        self.assertEqual(self.h.budgets.view(self.b)['state'],'closed')
        self.assertEqual(self.h.db.execute("SELECT state FROM outbox WHERE kind='budget_review'").fetchone()[0],'archived')
        # Existing bindings still collect late records after closure.
        self.bind();self.tokens('t-a',30);self.c.collect();self.assertEqual(self.used(),30)

    def test_f12_budget_ack_before_send_reply_remains_processed(self):
        self.bind();self.tokens('t-a',1100);self.c.collect()
        row=self.h.db.execute("SELECT * FROM outbox WHERE kind='budget_limit'").fetchone();h=self.h;b=self.b
        class IPC:
            def __init__(self,**kwargs):pass
            def owner(self,*args):return 'owner'
            def runtime(self,*args):return dict(status='active',provider='openai')
            def budget_notice(self,*args):
                h.budgets.call('a','budget_ack',dict(budget_id=b,delivery_id=row['id']))
                return {'turnId':'turn-a'}
            def close(self):pass
        with patch('worker.DesktopIPC',IPC):self.assertTrue(Worker(h).dispatch(row))
        self.assertEqual(h.db.execute('SELECT state FROM outbox WHERE id=?',(row['id'],)).fetchone()[0],'completed')
        self.assertEqual(h.db.execute('SELECT state FROM budget_notice_receipts').fetchone()[0],'processed')


class CycleAuditTests(unittest.TestCase):
    setUp=cycle_fixture.TeamCycleTests.setUp
    tearDown=cycle_fixture.TeamCycleTests.tearDown
    edge=cycle_fixture.TeamCycleTests.edge
    check=cycle_fixture.TeamCycleTests.check
    pending=cycle_fixture.TeamCycleTests.pending

    def test_f04_dense_dag_no_cycle_and_deep_chain_no_recursion(self):
        for i in range(40):
            for j in range(i+1,40):self.edge(f'e{i}-{j}',str(i),str(j))
        self.assertEqual(self.hub.team_cycle_snapshots(),[])
        for i in range(1200):self.edge('deep'+str(i),'d'+str(i),'d'+str(i+1))
        self.assertEqual(self.hub.team_cycle_snapshots(),[])

    def test_f08_unsent_a_b_a_cycle_revives_only_original(self):
        self.edge('ab','a','b');self.edge('ba','b','a');self.check();original=self.pending()[0]['id']
        for action in ('B','original action'):
            with self.hub.transaction():self.hub.db.execute('UPDATE requests SET action=? WHERE id=?',(action,'ab'))
            self.check();self.assertEqual(len(self.pending()),1)
        self.assertEqual(self.pending()[0]['id'],original);self.assertEqual(self.pending()[0]['attempts'],0)


class BootstrapAuditTests(unittest.TestCase):
    valid_members=bootstrap_fixture.BootstrapTests.valid_members
    write_config=bootstrap_fixture.BootstrapTests.write_config

    def test_f09_invalid_roles_structures_and_duplicate_threads(self):
        for role in ('../../README','/tmp/escape',' ','role/name','角色','..'):
            members=self.valid_members();members[1]['role']=role
            with self.subTest(role=role),self.assertRaises(ValueError):bootstrap.load_config(self.write_config(dict(members=members)))
        for members in ([None,{}],[1,2]):
            with self.assertRaises(ValueError):bootstrap.load_config(self.write_config(dict(members=members)))
        members=self.valid_members();members[1]['thread_id']=members[0]['thread_id']
        with self.assertRaises(ValueError):bootstrap.load_config(self.write_config(dict(members=members)))

    def test_f10_bad_onboarding_and_mid_transaction_failure_leave_no_members(self):
        for onboarding in (dict(version='bad/version',title='a',goal='b'),[],{},dict(version='v',title=2,goal='b')):
            with self.assertRaises(ValueError):bootstrap.load_config(self.write_config(dict(members=self.valid_members(),onboarding=onboarding)))
        config=self.write_config(dict(members=self.valid_members(),onboarding=dict(version='v1',title='a',goal='b')))
        with tempfile.TemporaryDirectory() as root:
            h=Hub(root)
            with patch.object(bootstrap,'Hub',return_value=h),patch.object(h,'version_create',side_effect=ValueError('injected')),patch.dict(os.environ,CODEX_THREAD_ID='thread-manager-example'),patch('sys.argv',['bootstrap.py','--config',str(config)]):
                with self.assertRaises(ValueError):bootstrap.main()
            h=Hub(root)
            self.assertEqual(h.db.execute('SELECT COUNT(*) FROM members').fetchone()[0],0)
            with patch.object(bootstrap,'Hub',return_value=h),patch.dict(os.environ,CODEX_THREAD_ID='thread-manager-example'),patch('sys.argv',['bootstrap.py','--config',str(config)]),patch('sys.stdout',io.StringIO()):bootstrap.main()
            h=Hub(root)
            try:self.assertEqual(h.db.execute('SELECT COUNT(*) FROM members').fetchone()[0],2)
            finally:h.close()


class LifecycleAuditTests(unittest.TestCase):
    def test_f16_stop_never_uses_stale_pid_or_command_substring(self):
        with tempfile.TemporaryDirectory() as root:
            h=Hub(root)
            with h.transaction():h.set_meta('worker',dict(pid=12345))
            with patch.object(control,'Hub',return_value=h),patch('sys.argv',['control.py','stop']),patch('control.os.kill',side_effect=AssertionError('must not kill')),patch('control.subprocess.check_output',return_value='/usr/bin/vim '+str(h.root/'worker.py')):
                with self.assertRaises(SystemExit):control.main()

    def test_f05_f16_control_endpoint_requests_actual_worker_stop(self):
        # Real same-user local socket with a synthetic callback, no real worker.
        with tempfile.TemporaryDirectory(dir='/tmp') as root:
            event=threading.Event();path=Path(root)/'control.sock';endpoint=ControlEndpoint(path,event.set);endpoint.start()
            try:
                self.assertTrue(request_stop(path)['stop_requested']);self.assertTrue(event.wait(1))
            finally:endpoint.close()
            self.assertFalse(path.exists())

    def test_f05_real_isolated_worker_start_stop_and_metadata(self):
        import shutil
        import subprocess
        import sys
        with tempfile.TemporaryDirectory(dir='/tmp') as root:
            root=Path(root)
            source=Path(__file__).resolve().parents[1]
            for name in ('worker.py','hub.py','budget.py','usage.py','transport.py','dispatch_extensions.py','projections.py','lifecycle.py'):
                shutil.copy2(source/name,root/name)
            process=subprocess.Popen([sys.executable,str(root/'worker.py')],cwd=root,
                stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'))
            try:
                deadline=time.monotonic()+5
                path=root/'.state/worker-control.sock'
                while time.monotonic()<deadline:
                    if path.exists() and (root/'.state/hub.sqlite3').exists():
                        with sqlite3.connect(root/'.state/hub.sqlite3') as db:
                            row=db.execute("SELECT value FROM meta WHERE key='worker'").fetchone()
                        if row:
                            state=json.loads(row[0])
                            if state.get('pid')==process.pid:break
                    time.sleep(.02)
                else:self.fail('worker failed to register')
                start=time.monotonic();reply=request_stop(path);out,err=process.communicate(timeout=3)
                elapsed=time.monotonic()-start
                self.assertEqual(reply['pid'],process.pid);self.assertEqual(process.returncode,0,err.decode())
                self.assertLess(elapsed,2)
                self.assertNotIn('error',out.decode())
                with sqlite3.connect(root/'.state/hub.sqlite3') as db:
                    self.assertEqual(json.loads(db.execute("SELECT value FROM meta WHERE key='worker'").fetchone()[0])['state'],'stopped')
            finally:
                if process.poll() is None:
                    process.terminate();process.communicate(timeout=5)
