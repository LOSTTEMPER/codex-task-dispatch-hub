import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from hub import Hub, dump, now
from usage import UsageCollector
from worker import Worker


class BudgetTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.root=Path(self.temp.name)
        self.h=Hub(self.root/'hub')
        with self.h.transaction():
            for role in ('manager','a','b'):
                self.h.db.execute('INSERT INTO members VALUES(?,?,?,?,?)',(role,'t-'+role,role,'{}',now()))
        self.h.version_create('manager',dict(id='v1',title='test',goal='test',tasks={}))
        self.h.begin('manager',{})
        self.b=self.h.budgets.call('manager','budget_create',dict(owner='a',token_limit=1000,warning_tokens=800))['id']
        self.catalog=sqlite3.connect(self.root/'catalog.db')
        self.catalog.executescript('CREATE TABLE threads(id TEXT,rollout_path TEXT,created_at_ms INTEGER,model_provider TEXT); CREATE TABLE thread_spawn_edges(parent_thread_id TEXT,child_thread_id TEXT);')
        for role in ('manager','a','b'):self.thread('t-'+role)
        self.c=UsageCollector(self.h,self.root/'catalog.db')
        self.c.collect()

    def tearDown(self):
        self.catalog.close();self.h.close();self.temp.cleanup()

    def thread(self,tid,parent=None):
        path=self.root/(tid+'.jsonl');path.touch()
        self.catalog.execute('INSERT INTO threads VALUES(?,?,?,?)',(tid,str(path),1,'openai'))
        if parent:self.catalog.execute('INSERT INTO thread_spawn_edges VALUES(?,?)',(parent,tid))
        self.catalog.commit()

    def event(self,tid,kind,**kwargs):
        with (self.root/(tid+'.jsonl')).open('a') as f:
            f.write(dump(dict(timestamp=now(),type='event_msg',payload=dict(type=kind,**kwargs)))+'\n')

    def tokens(self,tid,total,last=None):
        self.event(tid,'token_count',info={'total_token_usage':{'total_tokens':total,'input_tokens':max(0,total-1),'output_tokens':1},'last_token_usage':{'total_tokens':last if last is not None else total}})

    def bind(self,tid='t-a',turn='turn-a'):
        with self.h.transaction():self.h.budgets.bind_turn(tid,turn,self.b,'v1')
        self.event(tid,'task_started',turn_id=turn)

    def used(self):return self.h.budgets.used(self.b)

    def report(self,**extra):
        return self.h.budgets.call('a','budget_report',dict(budget_id=self.b,completed='saved',evidence='fixture',remaining='one step',recommendation='finish',additional_tokens=500,**extra))

    def decide(self,r,action='increase',**extra):
        return self.h.budgets.call('manager','budget_decide',dict(review_id=r['review_id'],expected_revision=extra.pop('revision',1),decision=action,reason='valuable',next_action='finish',acceptance='check',remaining_scope='one step',additional_tokens=500 if action=='increase' else 0,**extra))

    def test_recursive_children_first_fork_seed_retry_replay_tail(self):
        self.bind();self.tokens('t-a',100,100)
        self.thread('child','t-a');self.event('child','task_started',turn_id='ct',root_turn_id='turn-a');self.tokens('child',10000,200)
        self.thread('grand','child');self.event('grand','task_started',turn_id='gt',root_turn_id='ct');self.tokens('grand',90,90)
        self.c.collect();self.assertEqual(self.used(),390)
        self.c.collect();self.tokens('child',10000,200);self.c.collect();self.assertEqual(self.used(),390)
        self.tokens('child',10040,40);self.event('t-a','task_complete',turn_id='turn-a');self.tokens('t-a',125,25)
        self.tokens('grand',100,10);self.c.collect();self.assertEqual(self.used(),465)
        self.assertEqual(self.h.budgets.view(self.b)['coverage'],'complete')

    def test_historical_root_baseline_and_historical_child_not_charged(self):
        self.tokens('t-a',50000);self.c.collect();self.bind();self.tokens('t-a',50100,100)
        self.thread('oldchild','t-a');self.event('oldchild','task_started',turn_id='old',root_turn_id='old-root');self.tokens('oldchild',2000)
        self.c.collect();self.assertEqual(self.used(),100)

    def test_late_native_binding_backfills_child_once(self):
        self.event('t-a','task_started',turn_id='turn-a');self.tokens('t-a',20)
        self.thread('child','t-a');self.event('child','task_started',turn_id='ct',root_turn_id='turn-a');self.tokens('child',30)
        self.c.collect();self.assertEqual(self.used(),0)
        with self.h.transaction():self.h.budgets.bind_turn('t-a','turn-a',self.b,'v1')
        self.c.collect();self.assertEqual(self.used(),50)
        self.c.collect();self.assertEqual(self.used(),50)

    def test_missing_source_and_counter_reset_are_incomplete(self):
        self.bind();self.tokens('t-a',20);self.c.collect();self.tokens('t-a',10,10);self.c.collect()
        self.assertEqual(self.used(),30);self.assertEqual(self.h.budgets.view(self.b)['coverage'],'incomplete')
        (self.root/'t-a.jsonl').unlink();self.c.collect();self.assertEqual(self.used(),30)
        self.assertEqual(self.h.budgets.view(self.b)['coverage'],'incomplete')

    def test_partial_event_retried_and_collector_restart(self):
        self.bind();p=self.root/'t-a.jsonl'
        line=dump(dict(type='event_msg',timestamp=now(),payload=dict(type='token_count',info={'total_token_usage':{'total_tokens':42},'last_token_usage':{'total_tokens':42}})))+'\n'
        with p.open('a') as f:f.write(line[:20])
        self.c.collect();self.assertEqual(self.used(),0)
        with p.open('a') as f:f.write(line[20:])
        UsageCollector(self.h,self.root/'catalog.db').collect();self.assertEqual(self.used(),42)

    def test_limit_dedup_review_clarify_and_addition_preserve_usage(self):
        self.bind();self.tokens('t-a',850);self.c.collect();self.c.collect()
        self.assertEqual(self.h.db.execute("SELECT COUNT(*) FROM outbox WHERE kind='budget_warning'").fetchone()[0],1)
        self.tokens('t-a',1100,250);self.c.collect()
        self.assertEqual(self.h.db.execute("SELECT state FROM outbox WHERE kind='budget_warning'").fetchone()[0],'archived')
        report=self.report();self.report();self.c.collect()
        self.assertEqual(self.h.db.execute('SELECT COUNT(*) FROM budget_reviews').fetchone()[0],1)
        self.assertEqual(self.h.db.execute("SELECT COUNT(*) FROM outbox WHERE kind='budget_review'").fetchone()[0],1)
        self.decide(report,'clarify');report=self.report(review_id=report['review_id']);self.decide(report,revision=2)
        self.assertEqual(self.used(),1100);self.assertEqual(self.h.budgets.view(self.b)['token_limit'],1500)
        with self.assertRaises(ValueError):self.decide(report,revision=2)

    def test_late_usage_prevents_insufficient_resume(self):
        self.bind();self.tokens('t-a',1000);self.c.collect();r=self.report()
        self.tokens('t-a',1600,600);self.c.collect()
        with self.assertRaises(ValueError):self.decide(r)
        self.assertEqual(self.h.budgets.view(self.b)['token_limit'],1000)
        self.assertEqual(self.h.budgets.view(self.b)['review']['state'],'open')

    def test_review_wait_is_independent_of_business_wait(self):
        req=self.h.request('manager',dict(to='a',kind='wait',idempotency_key='one',urgent=False,important=True,reason='test',title='test',action='test',acceptance='test',budget_id=self.b))['request_id']
        run=self.h.active_run('manager')['id'];self.h.end('manager',dict(run_id=run,state='waiting',wait_for=[req]))
        run=self.h.begin('a',dict(request_ids=[req]))['run_id'];r=self.report()
        self.h.end('a',dict(run_id=run,state='waiting',budget_review_id=r['review_id']))
        self.assertEqual(self.h.get_request(req)['status'],'in_progress')
        self.assertEqual(self.h.db.execute('SELECT COUNT(*) FROM barriers WHERE fired=0').fetchone()[0],1)
        self.assertEqual(self.h.db.execute("SELECT COUNT(*) FROM outbox WHERE kind='budget_review' AND state='pending'").fetchone()[0],1)

    def test_manager_cap_self_grant_and_mixed_turn_rejected(self):
        self.h.budgets.call('manager','budget_configure',dict(version_token_limit=1100))
        r=self.report()
        with self.assertRaises(ValueError):self.decide(r)
        with self.assertRaises(PermissionError):self.h.budgets.call('a','budget_decide',{})
        self.bind()
        with self.h.transaction():
            with self.assertRaises(ValueError):self.h.budgets.bind_turn('t-a','turn-a','another','v1')

    def test_enabled_new_request_requires_explicit_budget(self):
        self.h.budgets.call('manager','budget_configure',dict(enabled=True))
        args=dict(to='a',kind='notify',idempotency_key='one',urgent=False,important=False,reason='test',title='test',action='test',acceptance='test')
        with self.assertRaises(ValueError):self.h.request('manager',args)
        self.h.request('manager',dict(args,unmanaged_reason='administrative coordination'))

    def test_active_notice_bypasses_busy_but_regular_delivery_does_not(self):
        self.bind();self.tokens('t-a',1100);self.c.collect();self.h.begin('a',dict(budget_id=self.b))
        row=self.h.db.execute("SELECT * FROM outbox WHERE kind='budget_limit'").fetchone();sent=[]
        class IPC:
            def owner(self,*a):return 'owner'
            def runtime(self,*a):return dict(status='active',provider='openai')
            def budget_notice(self,*a):sent.append(a);return {'turn':{'id':'turn-a'}}
            def close(self):pass
        w=Worker(self.h)
        with patch('worker.DesktopIPC',lambda **kwargs:IPC()):self.assertTrue(w.dispatch(row))
        self.assertEqual(len(sent),1);self.assertNotIn('a',w.active)
        self.h.budgets.call('a','budget_ack',dict(budget_id=self.b,delivery_id=row['id']))
        self.assertEqual(self.h.db.execute('SELECT state FROM budget_notice_receipts').fetchone()[0],'processed')
        with patch('worker.DesktopIPC',lambda **kwargs:IPC()):self.assertFalse(w.dispatch(row))

    def test_stop_retains_late_descendant_usage(self):
        self.bind();self.tokens('t-a',10);self.c.collect();r=self.report();self.decide(r,'stop')
        self.tokens('t-a',30,20);self.c.collect();self.assertEqual(self.used(),30)
        self.assertEqual(self.h.budgets.view(self.b)['state'],'stopped')

    def test_active_old_budget_notice_never_steals_current_turn(self):
        self.bind();self.tokens('t-a',1100);self.c.collect()
        row=self.h.db.execute("SELECT * FROM outbox WHERE kind='budget_limit'").fetchone()
        with self.h.transaction():
            self.h.budgets.prepare_delivery(row,active_turn=True)
            self.h.budgets.receipt_turn(row,'unrelated-new-turn')
        self.assertIsNone(self.h.db.execute("SELECT * FROM budget_turns WHERE turn_id='unrelated-new-turn'").fetchone())

    def test_real_desktop_nested_result_turn_id(self):
        from worker import turn_id
        self.assertEqual(turn_id({'result':{'turn':{'id':'actual'}}}),'actual')

    def test_resume_ack_persists_receipt_and_unknown_ack_rejected(self):
        report=self.report();self.decide(report)
        row=self.h.db.execute("SELECT * FROM outbox WHERE kind='budget_resume'").fetchone()
        with self.h.transaction():self.h.db.execute("UPDATE outbox SET state='delivered' WHERE id=?",(row['id'],))
        self.h.budgets.call('a','budget_ack',dict(budget_id=self.b,delivery_id=row['id']))
        self.assertEqual(self.h.db.execute('SELECT state FROM budget_notice_receipts WHERE delivery_id=?',(row['id'],)).fetchone()[0],'processed')
        with self.assertRaises(ValueError):self.h.budgets.call('a','budget_ack',dict(budget_id=self.b,delivery_id='missing'))

    def test_review_usage_rolls_up_separately_from_executor(self):
        report=self.report()
        row=self.h.db.execute("SELECT * FROM outbox WHERE kind='budget_review'").fetchone()
        with self.h.transaction():
            self.h.budgets.prepare_delivery(row)
            self.h.budgets.receipt_turn(row,'review-turn')
        self.event('t-manager','task_started',turn_id='review-turn');self.tokens('t-manager',42);self.c.collect()
        view=self.h.budgets.call('manager','budget_version_view',{})
        self.assertEqual(view['coordination_tokens'],42);self.assertEqual(view['work_tokens'],0)
        self.assertEqual(self.used(),0)

    def test_confirmed_owner_retry_not_reclaimed_by_native_bridge(self):
        with self.h.transaction():
            self.h.enqueue('a','probe','owner-retry',{},version='v1')
            self.h.db.execute("UPDATE outbox SET state='uncertain'")
        row=self.h.db.execute('SELECT * FROM outbox').fetchone()
        with self.assertRaises(ValueError):self.h.call('t-manager','delivery_retry',dict(delivery_id=row['id'],prefer_owner=True))
        self.h.call('t-manager','delivery_retry',dict(delivery_id=row['id'],prefer_owner=True,verified_not_received='Original turn completed, no later turn, app owner idle.'))
        self.assertEqual(self.h.call('t-manager','delivery_candidates',{})['deliveries'],[])
        self.assertEqual(self.h.db.execute('SELECT state FROM outbox').fetchone()[0],'pending')

    def test_uncertain_closed_version_result_retired_without_retry(self):
        with self.h.transaction():
            self.h.enqueue('manager','dependency_ready','old',{},version='v1')
            self.h.db.execute("UPDATE outbox SET state='uncertain'")
            self.h.db.execute("UPDATE versions SET state='archived' WHERE id='v1'")
            self.h.reconcile_wakeups()
        row=self.h.db.execute('SELECT * FROM outbox').fetchone()
        self.assertEqual(row['state'],'archived');self.assertEqual(row['attempts'],0)

if __name__=='__main__':unittest.main()
