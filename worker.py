#!/usr/bin/env python3
"""Durable no-model dispatcher. Run under the installed user LaunchAgent."""
from __future__ import annotations

import fcntl
import json
import os
import queue
import signal
import sys
import time
from pathlib import Path

from hub import Hub, ROOT, TERMINAL, now, dump
from transport import AppServer, DesktopIPC, RPCError


class Worker:
    def __init__(self, hub, rpc):
        self.hub = hub
        self.rpc = rpc
        self.owned = set()
        self.active = {}
        self.stopping = False

    def update_delivery(self, identifier, **fields):
        fields["updated"] = now()
        with self.hub.transaction():
            self.hub.db.execute("UPDATE outbox SET " + ",".join(k + "=?" for k in fields) + " WHERE id=?", list(fields.values()) + [identifier])

    def recover(self):
        # A lost response may already have started a turn: never blindly resend it.
        with self.hub.transaction():
            self.hub.db.execute("UPDATE outbox SET state='uncertain',last_error='中枢重启：发送结果待确认，先核对接收方记录，禁止盲目重发',updated=? WHERE state='sending'", (now(),))
        for row in self.hub.db.execute("SELECT * FROM outbox WHERE state='delivered'"):
            self.active[row["recipient"]] = row["id"]

    def process_notifications(self):
        while True:
            try: message = self.rpc.notifications.get_nowait()
            except queue.Empty: break
            params = message.get("params", {})
            thread_id = params.get("threadId")
            try: role = self.hub.actor(thread_id) if thread_id else None
            except PermissionError: role = None
            if not role: continue
            identifier = self.active.get(role)
            if not identifier: continue
            known = self.hub.db.execute("SELECT turn_id FROM outbox WHERE id=?", (identifier,)).fetchone()
            incoming_turn = params.get("turn", {}).get("id")
            if known and known[0] and incoming_turn and known[0] != incoming_turn: continue
            if message.get("method") == "turn/started":
                self.update_delivery(identifier, state="delivered", turn_id=params.get("turn", {}).get("id"), last_error=None)
            elif message.get("method") == "turn/completed":
                turn = params.get("turn", {})
                state = "completed" if turn.get("status") == "completed" else "failed"
                self.update_delivery(identifier, state=state, turn_id=turn.get("id"), last_error=None if state == "completed" else "Codex 执行未正常结束，请核对原对话")
                self.active.pop(role, None)
            elif "id" in message and "method" in message:
                self.update_delivery(identifier, last_error="执行需要用户处理：" + message["method"])

    def refresh_active(self):
        for role, identifier in list(self.active.items()):
            row = self.hub.db.execute("SELECT * FROM outbox WHERE id=?", (identifier,)).fetchone()
            thread_id = self.hub.member(role)["thread_id"]
            if row["route"] == "desktop-owner":
                ipc = DesktopIPC()
                try:
                    owner = ipc.owner(thread_id)
                    if owner and ipc.runtime(thread_id, owner)["status"] != "idle": continue
                finally: ipc.close()
            latest = self.rpc.latest_turn(thread_id)
            if not latest or latest.get("status") == "inProgress": continue
            if row["turn_id"] and latest.get("id") != row["turn_id"]: continue
            # Matching by time is only used after a confirmed desktop submission.
            if not row["turn_id"] and row["route"] != "desktop-owner": continue
            self.update_delivery(identifier, state="completed" if latest.get("status") == "completed" else "failed",
                                 turn_id=latest.get("id"))
            self.active.pop(role, None)

    def busy(self, role):
        if role in self.active: return True
        latest = self.rpc.latest_turn(self.hub.member(role)["thread_id"])
        if latest and latest.get("status") == "inProgress": return True
        run = self.hub.active_run(role)
        if run:
            # Grace period prevents racing begin immediately before start is persisted.
            from datetime import datetime, timezone
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(run["started"])).total_seconds()
            if age < 30: return True
            with self.hub.transaction():
                self.hub.db.execute("UPDATE runs SET state='interrupted',ended=?,summary='上一轮未提交收尾登记；运行已结束，结果待确认' WHERE id=? AND ended IS NULL", (now(), run["id"]))
                self.hub.db.execute("UPDATE version_roles SET state='interrupted' WHERE version=? AND role=? AND state!='submitted'", (run["version"], role))
        return False

    def dispatch(self, row):
        role = row["recipient"]
        if role in self.active: return False
        if row["request_id"] and row["kind"] == "request" and self.hub.get_request(row["request_id"])["status"] in TERMINAL:
            self.update_delivery(row["id"], state="cancelled")
            return True
        thread_id = self.hub.member(role)["thread_id"]
        ipc = None
        try:
            # Existing desktop ownership is respected; do not resume a second engine.
            ipc = DesktopIPC()
            owner = ipc.owner(thread_id)
            if owner:
                live = ipc.runtime(thread_id, owner)
                if live["status"] != "idle":
                    ipc.close()
                    return False
                if live["provider"] != "openai":
                    raise RPCError("非 OpenAI provider，不投递")
        except (FileNotFoundError, ConnectionRefusedError):
            owner = None
        except Exception as error:
            if ipc: ipc.close()
            self.update_delivery(row["id"], available_at=time.time() + 20, last_error="无法确认桌面所有权，暂缓投递：" + str(error)[:250])
            return False
        message = self.hub.message(row)
        try:
            if self.busy(role): return False
            if not owner:
                # Resuming auto-starts persisted queue entries. Respect unrelated user input.
                if self.rpc.queue_list(thread_id):
                    self.update_delivery(row["id"], available_at=time.time() + 20, last_error="该任务已有 Codex 待发送消息；等待用户队列处理，避免抢先执行")
                    return False
                if thread_id not in self.owned:
                    self.rpc.resume(thread_id, self.hub.root / "config" / "compact-prompt.md")
                    self.owned.add(thread_id)
            with self.hub.transaction():
                fresh = self.hub.db.execute("SELECT state FROM outbox WHERE id=?", (row["id"],)).fetchone()
                if fresh[0] != "pending": return False
                self.hub.db.execute("UPDATE outbox SET state='sending',attempts=attempts+1,route=?,updated=? WHERE id=?", ("desktop-owner" if owner else "app-server", now(), row["id"]))
            self.active[role] = row["id"]
            if owner:
                ipc.start(thread_id, owner, message, row["id"])
                self.update_delivery(row["id"], state="delivered", last_error=None)
            else:
                result = self.rpc.start(thread_id, message, row["id"])
                turn_id = result.get("turn", {}).get("id")
                if not turn_id: raise RPCError("Codex 未返回执行轮次编号")
                self.update_delivery(row["id"], state="delivered", turn_id=turn_id, last_error=None)
            return True
        except Exception as error:
            fresh = self.hub.db.execute("SELECT state FROM outbox WHERE id=?", (row["id"],)).fetchone()
            if fresh[0] in {"sending", "delivered"}:
                # The receiver may have begun even if this call timed out.
                self.update_delivery(row["id"], state="uncertain", last_error=str(error)[:450])
            else:
                attempts = row["attempts"] + 1
                self.update_delivery(row["id"], state="failed" if attempts >= 3 else "pending",
                                     attempts=attempts, available_at=time.time() + min(300, 15 * 2 ** attempts), last_error=str(error)[:450])
            self.active.pop(role, None)
            return False
        finally:
            if ipc: ipc.close()

    def tick(self):
        self.process_notifications()
        self.refresh_active()
        # One pending item per recipient per tick; strict priority then FIFO.
        rows = self.hub.db.execute("SELECT * FROM outbox WHERE state='pending' AND available_at<=? ORDER BY priority,created", (time.time(),)).fetchall()
        checked = set()
        for row in rows:
            if row["recipient"] in checked: continue
            checked.add(row["recipient"])
            self.dispatch(row)
        with self.hub.transaction():
            self.hub.evaluate()
            self.hub.set_meta("worker", {"pid": os.getpid(), "state": "running", "heartbeat": now(), "active_recipients": sorted(self.active)})
        self.hub.render()


def main():
    hub = Hub()
    lock = open(hub.state / "worker.lock", "a")
    try: fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError: raise SystemExit("中枢已有后台进程")
    workspace = Path(os.environ.get("CODEX_DISPATCH_WORKSPACE", str(ROOT)))
    rpc = AppServer(workspace)
    worker = Worker(hub, rpc)
    def stop(*_): worker.stopping = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    worker.recover()
    try:
        while not worker.stopping:
            try: worker.tick()
            except Exception as error:
                with hub.transaction(): hub.set_meta("worker", {"pid": os.getpid(), "state": "error", "heartbeat": now(), "error": str(error)[:500]})
                print(dump({"at": now(), "error": str(error)[:500]}), flush=True)
                if rpc.closed: break
            time.sleep(3)
    finally:
        rpc.close()
        with hub.transaction(): hub.set_meta("worker", {"state": "stopped", "heartbeat": now()})
        hub.close()
        lock.close()


if __name__ == "__main__": main()
