import json
import queue
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hub import Hub, dump, now
from worker import Worker

class HubTests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory(); self.h=Hub(self.temp.name)
  with self.h.transaction():
   for r in ['manager','a','b','c']:
    self.h.db.execute('INSERT INTO members VALUES(?,?,?,?,?)',(r,'thread-'+r,r,dump({'secret_card':r}),now()))
  self.h.version_create('manager',{'id':'v1','title':'test','goal':'test','tasks':{r:{'title':'initial','action':'do','acceptance':'done'} for r in ['a','b','c']}})
 def tearDown(self): self.h.close(); self.temp.cleanup()
 def start(self,r,ids=None): return self.h.begin(r,{'request_ids':ids or []})['run_id']
 def req(self,r,to='b',kind='wait',key='x',**kwargs):
  a=dict(idempotency_key=key,to=to,kind=kind,urgent=False,important=True,reason='test',title='test',action='do',acceptance='done',required_for_review=False);a.update(kwargs)
  return self.h.request(r,a)['request_id']
 def done(self,r,ids,state='idle'):
  run=self.start(r,ids); return self.h.end(r,{'run_id':run,'state':state,'results':[{'request_id':x,'summary':'done'} for x in ids]})
 def count(self,kind): return self.h.db.execute('SELECT COUNT(*) FROM outbox WHERE kind=?',(kind,)).fetchone()[0]
 def ready_with_manager_wait(self):
  run=self.start('manager');x=self.req('manager',required_for_review=True)
  self.h.end('manager',{'run_id':run,'state':'waiting','wait_for':[x]})
  for role in ['a','b','c']:
   ids=[r[0] for r in self.h.db.execute('SELECT id FROM requests WHERE recipient=?',(role,))]
   self.done(role,ids,'submitted')
  return self.h.db.execute("SELECT * FROM outbox WHERE recipient='manager' AND kind='dependency_ready'").fetchone()
 def test_identity_explicit_and_unregistered(self):
  self.assertEqual(self.h.call('thread-a','identity',{}),{'secret_card':'a'})
  self.assertNotIn('secret_card',dump(self.h.begin('a',{})))
  with self.assertRaises(PermissionError):self.h.call('forged','identity',{})
  row=self.h.db.execute('SELECT * FROM outbox LIMIT 1').fetchone();self.assertNotIn('secret_card',self.h.message(row))
 def test_request_idempotent_and_priority(self):
  self.start('a');x=self.req('a');self.assertEqual(x,self.req('a'))
  for i,(u,m,p) in enumerate([(True,True,0),(False,True,1),(True,False,2),(False,False,3)]):
   x=self.req('a',key=str(i),urgent=u,important=m);self.assertEqual(self.h.get_request(x)['priority'],p)
 def test_wait_group_once(self):
  run=self.start('a');x=self.req('a');y=self.req('a','c',key='y')
  self.h.end('a',{'run_id':run,'state':'waiting','wait_for':[x,y]})
  self.done('b',[x]);self.assertEqual(self.count('dependency_ready'),0)
  self.done('c',[y]);self.assertEqual(self.count('dependency_ready'),1)
  with self.h.transaction(): self.h.evaluate()
  self.assertEqual(self.count('dependency_ready'),1)
 def test_notify_no_return(self):
  run=self.start('a');x=self.req('a',kind='notify');self.h.end('a',{'run_id':run,'state':'idle'})
  self.done('b',[x]);self.assertEqual(self.count('dependency_ready'),0)
 def test_duplicate_wait_sets_across_turns_wake_once(self):
  run=self.start('a');x=self.req('a');y=self.req('a','c',key='y')
  self.h.end('a',{'run_id':run,'state':'waiting','wait_for':[x,y]})
  run=self.start('a');self.h.end('a',{'run_id':run,'state':'waiting','wait_for':[y,x,x]})
  self.assertEqual(self.h.db.execute("SELECT COUNT(*) FROM barriers WHERE role='a'").fetchone()[0],1)
  self.done('b',[x]);self.done('c',[y]);self.assertEqual(self.count('dependency_ready'),1)
  run=self.start('a');self.h.end('a',{'run_id':run,'state':'waiting','wait_for':[x,y]})
  self.assertEqual(self.count('dependency_ready'),1,'an already announced result cannot create another turn')
 def test_changed_dependency_outcome_still_wakes(self):
  run=self.start('a');x=self.req('a');self.h.end('a',{'run_id':run,'state':'waiting','wait_for':[x]})
  self.h.update_request('b',{'request_id':x,'expected_revision':1,'status':'blocked','note':'blocked'})
  self.assertEqual(self.count('dependency_ready'),1)
  self.start('b',[x])
  run=self.start('a');self.h.end('a',{'run_id':run,'state':'waiting','wait_for':[x]})
  self.done('b',[x]);self.assertEqual(self.count('dependency_ready'),2,'blocked and later resolved are different actions')
 def test_dependency_and_review_share_one_wake(self):
  row=self.ready_with_manager_wait()
  pending=self.h.db.execute("SELECT kind FROM outbox WHERE recipient='manager' AND state='pending'").fetchall()
  self.assertEqual([r[0] for r in pending],['dependency_ready'])
  self.assertTrue(json.loads(row['payload'])['review_required'])
  review=self.h.db.execute("SELECT * FROM outbox WHERE kind='review_ready'").fetchone()
  self.assertEqual(review['state'],'archived');self.assertEqual(review['attempts'],0)
  self.assertIn('一并决定',self.h.message(row))
 def test_late_wait_coalesces_preexisting_review(self):
  run=self.start('manager');x=self.req('manager',required_for_review=True)
  for role in ['a','b','c']:
   ids=[r[0] for r in self.h.db.execute('SELECT id FROM requests WHERE recipient=?',(role,))]
   self.done(role,ids,'submitted')
  self.assertEqual(self.h.db.execute("SELECT state FROM outbox WHERE kind='review_ready'").fetchone()[0],'pending')
  self.h.end('manager',{'run_id':run,'state':'waiting','wait_for':[x]})
  rows=self.h.db.execute("SELECT kind FROM outbox WHERE recipient='manager' AND state='pending'").fetchall()
  self.assertEqual([r[0] for r in rows],['dependency_ready'])
 def test_explicit_hold_does_not_leave_a_review_wake(self):
  for role in ['a','b','c']:
   ids=[r[0] for r in self.h.db.execute('SELECT id FROM requests WHERE recipient=?',(role,))]
   self.done(role,ids,'submitted')
  self.h.version_review('manager',{'decision':'hold','summary':'reviewed; no further action now'})
  self.assertEqual(self.h.version()['state'],'awaiting_review')
  row=self.h.db.execute("SELECT * FROM outbox WHERE kind='review_ready'").fetchone()
  self.assertEqual(row['state'],'archived');self.assertEqual(row['attempts'],0)
 def test_review_does_not_wake_before_waited_dependency(self):
  run=self.start('manager');x=self.req('manager')
  self.h.end('manager',{'run_id':run,'state':'waiting','wait_for':[x]})
  for role in ['a','b','c']:
   ids=[r[0] for r in self.h.db.execute("SELECT id FROM requests WHERE recipient=? AND title='initial'",(role,))]
   self.done(role,ids,'submitted')
  self.assertEqual(self.h.version()['state'],'awaiting_review')
  self.assertEqual(self.h.db.execute("SELECT COUNT(*) FROM outbox WHERE recipient='manager' AND state='pending'").fetchone()[0],0)
  self.done('b',[x])
  row=self.h.db.execute("SELECT * FROM outbox WHERE recipient='manager' AND state='pending'").fetchone()
  self.assertEqual(row['kind'],'dependency_ready');self.assertTrue(json.loads(row['payload'])['review_required'])
 def test_closed_result_is_archived_before_either_transport(self):
  row=self.ready_with_manager_wait()
  self.assertIn(row['id'],[r['delivery_id'] for r in self.h.call('thread-c','delivery_candidates',{})['deliveries']])
  self.h.version_review('manager',{'decision':'accepted','summary':'already reviewed'})
  self.assertNotIn(row['id'],[r['delivery_id'] for r in self.h.call('thread-c','delivery_candidates',{})['deliveries']])
  result=self.h.call('thread-c','delivery_claim_native',{'delivery_id':row['id']})
  self.assertEqual(result,{'delivery_id':row['id'],'state':'archived','skipped':True})
  with patch('worker.DesktopIPC',side_effect=AssertionError('must not contact application')):
   self.assertFalse(Worker(self.h).dispatch(row))
  archived=self.h.db.execute('SELECT * FROM outbox WHERE id=?',(row['id'],)).fetchone()
  self.assertEqual(archived['attempts'],0);self.assertIn('request_ids',json.loads(archived['payload']))
  run=self.start('manager')
  with self.assertRaises(ValueError):
   self.h.end('manager',{'run_id':run,'state':'waiting','wait_for':json.loads(row['payload'])['request_ids']})
 def test_worker_rechecks_after_live_lookup(self):
  row=self.ready_with_manager_wait()
  def runtime(*_):
   self.h.version_review('manager',{'decision':'accepted','summary':'reviewed while inspecting owner'})
   return {'status':'idle','provider':'openai'}
  ipc=type('IPC',(),{'owner':lambda s,t:'owner','runtime':runtime,
                     'start':lambda *a: self.fail('stale result must not start a turn'),'close':lambda s:None})
  with patch('worker.DesktopIPC',lambda **kwargs:ipc()):self.assertFalse(Worker(self.h).dispatch(row))
  self.assertEqual(self.h.db.execute('SELECT attempts FROM outbox WHERE id=?',(row['id'],)).fetchone()[0],0)
 def test_worker_sends_review_merged_during_live_lookup(self):
  run=self.start('manager');x=self.req('manager',required_for_review=True)
  self.h.end('manager',{'run_id':run,'state':'waiting','wait_for':[x]})
  for role in ['a','b']:
   ids=[r[0] for r in self.h.db.execute('SELECT id FROM requests WHERE recipient=?',(role,))]
   self.done(role,ids,'submitted')
  row=self.h.db.execute("SELECT * FROM outbox WHERE recipient='manager' AND kind='dependency_ready'").fetchone()
  self.assertFalse(json.loads(row['payload'])['review_required'])
  messages=[]
  def runtime(*_):
   ids=[r[0] for r in self.h.db.execute("SELECT id FROM requests WHERE recipient='c'")]
   self.done('c',ids,'submitted')
   return {'status':'idle','provider':'openai'}
  ipc=type('IPC',(),{'owner':lambda s,t:'owner','runtime':runtime,
                     'start':lambda s,t,o,message,key:messages.append(message),'close':lambda s:None})
  with patch('worker.DesktopIPC',lambda **kwargs:ipc()):self.assertTrue(Worker(self.h).dispatch(row))
  self.assertEqual(len(messages),1);self.assertIn('一并决定',messages[0])
  self.assertEqual(self.h.db.execute("SELECT state FROM outbox WHERE kind='review_ready'").fetchone()[0],'archived')
 def test_policy_and_information_records_are_silent(self):
  before=self.h.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0]
  run=self.h.begin('a',{})
  self.assertIn('纯知会',run['notification_policy']);self.assertIn('同一组依赖只登记一次',run['notification_policy'])
  self.h.document_put('a',dict(key='fyi',title='Reference',body='For reference',expected_revision=0,change_note='record only'))
  self.h.end('a',{'run_id':run['run_id'],'state':'idle','product_updates':[{'idempotency_key':'fyi-update','summary':'record only','validation':'checked'}]})
  self.assertEqual(self.h.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0],before)
 def test_no_implicit_completion(self):
  x=self.h.db.execute("SELECT id FROM requests WHERE recipient='a'").fetchone()[0]
  run=self.start('a',[x]);self.h.end('a',{'run_id':run,'state':'idle'})
  self.assertEqual(self.h.get_request(x)['status'],'in_progress')
 def test_required_submission_guard(self):
  run=self.start('a')
  with self.assertRaises(ValueError):self.h.end('a',{'run_id':run,'state':'submitted'})
  self.assertIsNotNone(self.h.active_run('a'))
 def test_wait_must_be_declared(self):
  run=self.start('a');self.req('a')
  with self.assertRaises(ValueError):self.h.end('a',{'run_id':run,'state':'idle'})
 def test_cancel_dependency(self):
  run=self.start('a');x=self.req('a');self.h.end('a',{'run_id':run,'state':'waiting','wait_for':[x]})
  self.h.update_request('a',{'request_id':x,'expected_revision':1,'status':'cancelled','note':'stop'})
  self.assertEqual(self.count('dependency_ready'),1)
 def test_cycle_once(self):
  a=self.start('a');x=self.req('a');self.h.end('a',{'run_id':a,'state':'waiting','wait_for':[x]})
  b=self.start('b');y=self.req('b','a');self.h.end('b',{'run_id':b,'state':'waiting','wait_for':[y]})
  with self.h.transaction():self.h._check_cycles()
  self.assertEqual(self.count('dependency_cycle'),1)
 def test_review_once(self):
  for r in ['a','b','c']:
   x=self.h.db.execute('SELECT id FROM requests WHERE recipient=?',(r,)).fetchone()[0];self.done(r,[x],'submitted')
  self.assertEqual(self.h.version()['state'],'awaiting_review');self.assertEqual(self.count('review_ready'),1)
  with self.h.transaction():self.h.evaluate()
  self.assertEqual(self.count('review_ready'),1)
  with self.assertRaises(PermissionError):self.h.version_review('a',{'decision':'accepted','summary':'ok'})
  self.h.version_review('manager',{'decision':'accepted','summary':'ok'})
 def test_document_acl_revision_and_latest_only(self):
  a=dict(key='doc',title='Doc',body='old-body-unique',expected_revision=0,change_note='created')
  self.h.document_put('a',a)
  with self.assertRaises(PermissionError):self.h.document_put('b',dict(a,expected_revision=1))
  with self.assertRaises(ValueError):self.h.document_put('a',a)
  self.h.document_put('a',dict(a,body='new-body',expected_revision=1,change_note='updated'))
  text=(self.h.root/'docs/documents/doc.md').read_text();self.assertNotIn('old-body-unique',text);self.assertIn('created',text)
 def test_product_update_even_idle_and_dedup(self):
  for _ in range(2):
   run=self.start('a');self.h.end('a',{'run_id':run,'state':'idle','product_updates':[{'idempotency_key':'button','summary':'button changed','validation':'checked'}]})
  self.assertEqual(self.h.db.execute('SELECT COUNT(*) FROM product_updates').fetchone()[0],1)
 def test_end_and_begin_retries(self):
  run=self.start('a');self.assertEqual(run,self.start('a'))
  self.h.end('a',{'run_id':run,'state':'idle'});self.assertTrue(self.h.end('a',{'run_id':run,'state':'idle'})['duplicate'])
 def test_restart_uncertain_and_busy_no_dispatch(self):
  rpc=type('RPC',(),{'notifications':queue.Queue(),'latest_turn':lambda s,t:{'status':'inProgress'}})()
  w=Worker(self.h,rpc)
  with self.h.transaction():self.h.db.execute("UPDATE outbox SET state='sending'")
  w.recover();self.assertEqual(self.h.db.execute("SELECT COUNT(*) FROM outbox WHERE state='uncertain'").fetchone()[0],3)
  self.start('a');self.assertTrue(w.busy('a'))
 def test_closed_version_prevents_new_request(self):
  self.h.version_review('manager',{'decision':'archived','summary':'stop'});self.start('a')
  with self.assertRaises(ValueError):self.req('a')

 def test_desktop_live_active_beats_stale_history(self):
  rpc=type('RPC',(),{'notifications':queue.Queue(),'latest_turn':lambda s,t:{'status':'completed'}})()
  ipc=type('IPC',(),{'owner':lambda s,t:'owner','runtime':lambda s,t,o:{'status':'active','provider':'openai'},'close':lambda s:None})
  row=self.h.db.execute('SELECT * FROM outbox LIMIT 1').fetchone()
  with patch('worker.DesktopIPC',lambda **kwargs:ipc()):self.assertFalse(Worker(self.h,rpc).dispatch(row))
  self.assertEqual(self.h.db.execute('SELECT attempts FROM outbox WHERE id=?',(row['id'],)).fetchone()[0],0)
 def test_empty_goal_no_wake_or_premature_review(self):
  self.h.version_review('manager',{'decision':'archived','summary':'done'})
  old=self.h.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0]
  self.h.version_create('manager',{'id':'operations','title':'Daily','goal':'wait for real tasks','tasks':{}})
  with self.h.transaction():self.h.evaluate()
  self.assertEqual(self.h.version()['state'],'active')
  self.assertEqual(self.h.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0],old)

 def test_no_owner_never_executes_hidden_engine(self):
  row=self.h.db.execute('SELECT * FROM outbox LIMIT 1').fetchone()
  ipc=type('IPC',(),{'owner':lambda s,t:None,'close':lambda s:None})
  with patch('worker.DesktopIPC',lambda **kwargs:ipc()):self.assertFalse(Worker(self.h).dispatch(row))
  result=self.h.db.execute('SELECT * FROM outbox WHERE id=?',(row['id'],)).fetchone()
  self.assertEqual(result['state'],'pending');self.assertEqual(result['attempts'],0)
  self.assertIn('禁止后台',result['last_error'])
 def test_native_claim_exclusive_and_receipt(self):
  row=self.h.db.execute('SELECT * FROM outbox LIMIT 1').fetchone()
  args={'delivery_id':row['id']}
  claimed=self.h.call('thread-c','delivery_claim_native',args)
  self.assertEqual(claimed['thread_id'],'thread-'+row['recipient'])
  with self.assertRaises(ValueError):self.h.call('thread-manager','delivery_claim_native',args)
  with self.assertRaises(PermissionError):self.h.call('thread-b','delivery_receipt_native',dict(args,confirmed=True))
  self.h.call('thread-c','delivery_receipt_native',dict(args,confirmed=True,turn_id='visible-turn'))
  self.assertEqual(self.h.db.execute('SELECT state FROM outbox WHERE id=?',(row['id'],)).fetchone()[0],'delivered')

 def test_busy_backlog_does_not_hide_cold_recipient(self):
  with self.h.transaction():
   self.h.db.execute('DELETE FROM outbox')
   for i in range(12):self.h.enqueue('a','probe','busy-'+str(i),{},priority=0)
   self.h.enqueue('b','probe','cold',{},priority=1)
  candidates=self.h.call('thread-manager','delivery_candidates',{})['deliveries']
  self.assertEqual([r['recipient'] for r in candidates],['a','b'])
  own=self.h.call('thread-a','delivery_candidates',{})['deliveries']
  self.assertEqual([r['recipient'] for r in own],['b'])

 def test_worker_cannot_race_another_native_delivery(self):
  self.start('manager');self.req('manager',to='a',kind='notify')
  rows=self.h.db.execute("SELECT * FROM outbox WHERE recipient='a' ORDER BY created,id").fetchall()
  ipc=type('IPC',(),{'owner':lambda s,t:'owner','runtime':lambda s,t,o:{'status':'idle','provider':'openai'},'close':lambda s:None})
  worker=Worker(self.h)
  with patch('worker.DesktopIPC',lambda **kwargs:ipc()):
   # Simulate another path winning while the worker checks the live owner.
   self.h.call('thread-b','delivery_claim_native',{'delivery_id':rows[0]['id']})
   self.assertFalse(worker.dispatch(rows[1]))
  state=self.h.db.execute('SELECT state,attempts FROM outbox WHERE id=?',(rows[1]['id'],)).fetchone()
  self.assertEqual(tuple(state),('pending',0))

 def test_protocol_upgrade_preserves_identity_and_scope(self):
  card={'role':'a','thread_id':'thread-a','card_version':'1.0','identity_block':'<team_identity>\ncard_version: 1.0\n职责：unchanged-scope\n授权边界：unchanged-permissions\n协作入口：old-cli\n每轮正式工作前 old-rule\n</team_identity>'}
  with self.h.transaction():self.h.db.execute("UPDATE members SET card=? WHERE role='a'",(dump(card),))
  with self.assertRaises(PermissionError):self.h.call('thread-a','identity_protocol_update',{})
  changed=self.h.call('thread-manager','identity_protocol_update',{})
  self.assertEqual(changed['updated_roles'],['a'])
  current=self.h.identity('a')
  self.assertEqual((current['role'],current['thread_id'],current['card_version']),('a','thread-a','1.2'))
  self.assertIn('职责：unchanged-scope',current['identity_block']);self.assertIn('授权边界：unchanged-permissions',current['identity_block'])
  self.assertIn('native_call.js',current['identity_block']);self.assertNotIn('old-cli',current['identity_block'])
  self.assertEqual(self.h.call('thread-manager','identity_protocol_update',{})['updated_roles'],[])

 def test_wake_uses_current_native_protocol_without_identity_card(self):
  row=self.h.db.execute('SELECT * FROM outbox LIMIT 1').fetchone()
  message=self.h.message(row)
  self.assertIn('native_call.js',message);self.assertIn('早于 v1.2',message)
  self.assertNotIn('<team_identity>',message)

if __name__=='__main__':unittest.main(verbosity=2)
