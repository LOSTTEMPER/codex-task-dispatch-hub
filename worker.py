#!/usr/bin/env python3
"""Model-free outbox routing to an existing desktop owner. No execution engine."""
from __future__ import annotations
import fcntl
import os
import signal
import time
import threading
from hub import Hub, TERMINAL, now, dump
from transport import DesktopIPC
from usage import UsageCollector
from budget import ACTIVE_NOTICES, stamp

def turn_id(result):
    if isinstance(result, dict):
        return result.get('turnId') or (result.get('turn') or {}).get('id') or turn_id(result.get('result'))

class Worker:
    def __init__(self, hub, unused=None):
        self.hub=hub
        self.active={}
        self.stopping=False
        self.collector=UsageCollector(hub)
        self.monitor=None
        self.monitor_stop=threading.Event()

    def start_monitor(self):
        """Separate SQLite connection: desktop discovery cannot stall metering."""
        def monitor():
            h=Hub(self.hub.root)
            collector=UsageCollector(h)
            try:
                while not self.monitor_stop.is_set():
                    try:collector.collect()
                    except Exception as e:
                        with h.transaction():h.set_meta('budget_collector',{'state':'incomplete','checked_at':stamp(),'error':str(e)[:200]})
                    self.monitor_stop.wait(3)
            finally:h.close()
        self.monitor=threading.Thread(target=monitor,name='budget-metadata-monitor',daemon=True)
        self.monitor.start()

    def update_delivery(self, identifier, **fields):
        fields['updated']=now()
        with self.hub.transaction():
            self.hub.db.execute('UPDATE outbox SET '+','.join(k+'=?' for k in fields)+' WHERE id=?',list(fields.values())+[identifier])

    def recover(self):
        with self.hub.transaction():
            self.hub.db.execute("UPDATE outbox SET state='uncertain',last_error='发送确认丢失，核对原对话后再重试',updated=? WHERE state='sending'",(now(),))
        self.active={r['recipient']:r['id'] for r in self.hub.db.execute("SELECT * FROM outbox WHERE state='delivered' AND kind NOT IN ('budget_warning','budget_limit')")}

    def busy(self, role):
        return role in self.active or self.hub.active_run(role) is not None

    def live(self, role):
        ipc=DesktopIPC()
        try:
            tid=self.hub.member(role)['thread_id'];owner=ipc.owner(tid)
            return ipc,owner,ipc.runtime(tid,owner) if owner else None
        except BaseException:
            ipc.close();raise

    def refresh_active(self):
        # Native adapter receipts can arrive after the worker started.
        for row in self.hub.db.execute("SELECT * FROM outbox WHERE state='delivered' AND kind NOT IN ('budget_warning','budget_limit')"):
            self.active[row['recipient']]=row['id']
        for role,identifier in list(self.active.items()):
            row=self.hub.db.execute('SELECT * FROM outbox WHERE id=?',(identifier,)).fetchone()
            if row['state']!='delivered':self.active.pop(role,None);continue
            try:
                ipc,owner,live=self.live(role)
                try:
                    if not owner or live['status']!='idle':continue
                    if self.hub.active_run(role):continue
                    self.update_delivery(identifier,state='completed',last_error=None)
                    self.active.pop(role,None)
                finally:ipc.close()
            except Exception as error:
                self.update_delivery(identifier,last_error='无法确认应用内执行状态：'+str(error)[:250])

    def dispatch(self,row):
        with self.hub.transaction():
            self.hub.reconcile_wakeups()
            row=self.hub.db.execute('SELECT * FROM outbox WHERE id=?',(row['id'],)).fetchone()
            if not row or row['state']!='pending':return False
        role=row['recipient']
        immediate=row['kind'] in ACTIVE_NOTICES
        if not immediate and self.busy(role):return False
        if row['request_id'] and row['kind']=='request' and self.hub.get_request(row['request_id'])['status'] in TERMINAL:
            self.update_delivery(row['id'],state='cancelled');return True
        ipc=None
        try:
            ipc,owner,live=self.live(role)
            if not owner:
                self.update_delivery(row['id'],available_at=time.time()+30,last_error='应用尚未持有此原对话；需要原生应用投递桥载入。禁止后台代执行。')
                return False
            if not immediate and live['status']!='idle':return False
            if immediate and live['status'] not in ('idle','active'):return False
            if live['provider']!='openai':raise ValueError('非授权 Codex provider，不投递')
            with self.hub.transaction():
                self.hub.reconcile_wakeups()
                row=self.hub.db.execute('SELECT * FROM outbox WHERE id=?',(row['id'],)).fetchone()
                if not row or row['state']!='pending':return False
                # The native bridge may have claimed another item for this
                # recipient since the initial busy check. Recheck under lock.
                if not immediate and self.hub.active_run(role):return False
                if not immediate and self.hub.db.execute("SELECT 1 FROM outbox WHERE recipient=? AND state IN ('sending','delivered','uncertain') AND kind NOT IN ('budget_warning','budget_limit')",(role,)).fetchone():return False
                self.hub.db.execute("UPDATE outbox SET state='sending',attempts=attempts+1,route='desktop-owner',updated=? WHERE id=?",(now(),row['id']))
                self.hub.budgets.prepare_delivery(row,active_turn=live['status']=='active')
            tid=self.hub.member(role)['thread_id']
            if immediate:
                result=ipc.budget_notice(tid,owner,self.hub.budgets.notice(row),row['id'])
            else:
                self.active[role]=row['id']
                result=ipc.start(tid,owner,self.hub.message(row),row['id'])
            actual=turn_id(result)
            with self.hub.transaction():
                self.hub.db.execute("UPDATE outbox SET state='delivered',turn_id=?,last_error=NULL,updated=? WHERE id=?",(actual,now(),row['id']))
                self.hub.budgets.receipt_turn(row,actual)
                if immediate:
                    self.hub.db.execute('INSERT OR REPLACE INTO budget_notice_receipts VALUES(?,?,?,?,?,?)',
                        (row['id'],self.hub.budgets.notice(row)['budget']['id'],tid,actual,'accepted_by_app',stamp()))
            return True
        except Exception as error:
            current=self.hub.db.execute('SELECT state FROM outbox WHERE id=?',(row['id'],)).fetchone()[0]
            self.update_delivery(row['id'],state='uncertain' if current=='sending' else current,
                available_at=time.time()+30,last_error=str(error)[:400])
            self.active.pop(role,None);return False
        finally:
            if ipc:ipc.close()

    def tick(self):
        if self.monitor is None:self.collector.collect()
        self.refresh_active()
        with self.hub.transaction():self.hub.reconcile_wakeups()
        checked=set()
        for row in self.hub.db.execute("SELECT * FROM outbox WHERE state='pending' AND available_at<=? ORDER BY priority,created",(time.time(),)).fetchall():
            key=(row['recipient'],row['kind'] in ACTIVE_NOTICES)
            if key in checked:continue
            checked.add(key);self.dispatch(row)
        with self.hub.transaction():
            self.hub.evaluate()
            self.hub.set_meta('worker',{'pid':os.getpid(),'state':'running','mode':'desktop-visible-only',
                'heartbeat':now(),'active_recipients':sorted(self.active)})
        self.hub.render()

def main():
    hub=Hub();lock=open(hub.state/'worker.lock','a')
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:raise SystemExit('中枢已有后台进程')
    worker=Worker(hub)
    def stop(*_):worker.stopping=True
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    worker.recover()
    worker.start_monitor()
    try:
        while not worker.stopping:
            try:worker.tick()
            except Exception as e:print(dump({'at':now(),'error':str(e)[:500]}),flush=True)
            time.sleep(3)
    finally:
        worker.monitor_stop.set()
        if worker.monitor:worker.monitor.join(timeout=5)
        with hub.transaction():hub.set_meta('worker',{'state':'stopped','mode':'desktop-visible-only','heartbeat':now()})
        hub.close();lock.close()

if __name__=='__main__':main()
