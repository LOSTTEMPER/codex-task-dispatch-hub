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
  self.assertTrue(w.busy('a'))
 def test_closed_version_prevents_new_request(self):
  self.h.version_review('manager',{'decision':'archived','summary':'stop'});self.start('a')
  with self.assertRaises(ValueError):self.req('a')

 def test_desktop_live_active_beats_stale_history(self):
  rpc=type('RPC',(),{'notifications':queue.Queue(),'latest_turn':lambda s,t:{'status':'completed'}})()
  ipc=type('IPC',(),{'owner':lambda s,t:'owner','runtime':lambda s,t,o:{'status':'active','provider':'openai'},'close':lambda s:None})
  row=self.h.db.execute('SELECT * FROM outbox LIMIT 1').fetchone()
  with patch('worker.DesktopIPC',ipc):self.assertFalse(Worker(self.h,rpc).dispatch(row))
  self.assertEqual(self.h.db.execute('SELECT attempts FROM outbox WHERE id=?',(row['id'],)).fetchone()[0],0)
 def test_empty_goal_no_wake_or_premature_review(self):
  self.h.version_review('manager',{'decision':'archived','summary':'done'})
  old=self.h.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0]
  self.h.version_create('manager',{'id':'operations','title':'Daily','goal':'wait for real tasks','tasks':{}})
  with self.h.transaction():self.h.evaluate()
  self.assertEqual(self.h.version()['state'],'active')
  self.assertEqual(self.h.db.execute('SELECT COUNT(*) FROM outbox').fetchone()[0],old)

if __name__=='__main__':unittest.main(verbosity=2)
