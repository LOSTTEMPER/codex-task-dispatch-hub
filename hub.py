#!/usr/bin/env python3
"""任务调度中枢: local, model-free collaboration ledger and CLI.

Only this module writes managed collaboration documents. Model-private execution
notes and product source files remain outside the hub.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import uuid

ROOT = Path(__file__).resolve().parent
MANAGER = "manager"
STATES = {"idle", "working", "waiting", "submitted", "needs_user", "interrupted"}
TERMINAL = {"done", "cancelled", "superseded"}
PRIORITIES = {"highest": 0, "high": 1, "medium": 2, "low": 3}
PRIORITY_NAMES = ["最高", "高", "中", "低"]


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def uid(prefix):
    return prefix + "-" + uuid.uuid4().hex


def dump(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def slug(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}", value):
        raise ValueError("标识须为 1–96 位字母、数字、点、下划线或连字符")
    return value


def short(value, name, maximum=1500, required=True):
    if not isinstance(value, str) or len(value) > maximum or (required and not value.strip()):
        raise ValueError(f"{name}须为{'非空' if required else ''}文本，最多 {maximum} 字符；详情请用文档引用")
    return value.strip()


def refs(value):
    if not isinstance(value, list) or len(value) > 12:
        raise ValueError("文档引用须为列表，最多 12 项")
    out = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError("每项引用须包含 path 和 revision")
        path = short(item.get("path"), "引用路径", 1000)
        revision = short(item.get("revision"), "引用版本", 80)
        out.append({"path": path, "revision": revision})
    return out


class Hub:
    def __init__(self, root=ROOT):
        self.root = Path(root).resolve()
        self.state = self.root / ".state"
        self.state.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.state / "hub.sqlite3", timeout=20)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=20000")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS members(
          role TEXT PRIMARY KEY,thread_id TEXT UNIQUE NOT NULL,label TEXT NOT NULL,
          card TEXT NOT NULL,updated TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS versions(
          id TEXT PRIMARY KEY,title TEXT NOT NULL,goal TEXT NOT NULL,state TEXT NOT NULL,
          epoch INTEGER NOT NULL DEFAULT 1,created TEXT NOT NULL,updated TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS version_roles(
          version TEXT REFERENCES versions(id),role TEXT REFERENCES members(role),
          state TEXT NOT NULL DEFAULT 'assigned',summary TEXT NOT NULL DEFAULT '',
          PRIMARY KEY(version,role));
        CREATE TABLE IF NOT EXISTS runs(
          id TEXT PRIMARY KEY,role TEXT REFERENCES members(role),version TEXT,
          state TEXT NOT NULL,started TEXT NOT NULL,ended TEXT,summary TEXT NOT NULL DEFAULT '',
          request_ids TEXT NOT NULL DEFAULT '[]');
        CREATE UNIQUE INDEX IF NOT EXISTS one_active_run ON runs(role) WHERE ended IS NULL;
        CREATE TABLE IF NOT EXISTS requests(
          id TEXT PRIMARY KEY,version TEXT REFERENCES versions(id),sender TEXT REFERENCES members(role),
          recipient TEXT REFERENCES members(role),run_id TEXT REFERENCES runs(id),kind TEXT NOT NULL,
          priority INTEGER NOT NULL,urgent INTEGER NOT NULL,important INTEGER NOT NULL,
          blocking TEXT NOT NULL,reason TEXT NOT NULL,title TEXT NOT NULL,action TEXT NOT NULL,
          acceptance TEXT NOT NULL,refs TEXT NOT NULL,required INTEGER NOT NULL,status TEXT NOT NULL,
          revision INTEGER NOT NULL DEFAULT 1,result TEXT NOT NULL DEFAULT '',result_refs TEXT NOT NULL DEFAULT '[]',
          created TEXT NOT NULL,updated TEXT NOT NULL,idempotency_key TEXT NOT NULL,
          UNIQUE(sender,idempotency_key));
        CREATE TABLE IF NOT EXISTS outbox(
          id TEXT PRIMARY KEY,recipient TEXT REFERENCES members(role),kind TEXT NOT NULL,
          request_id TEXT REFERENCES requests(id),version TEXT,event_key TEXT UNIQUE NOT NULL,
          payload TEXT NOT NULL,priority INTEGER NOT NULL,state TEXT NOT NULL DEFAULT 'pending',
          attempts INTEGER NOT NULL DEFAULT 0,available_at REAL NOT NULL DEFAULT 0,
          turn_id TEXT,route TEXT,last_error TEXT,created TEXT NOT NULL,updated TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS barriers(
          id TEXT PRIMARY KEY,role TEXT REFERENCES members(role),version TEXT,
          dependencies TEXT NOT NULL,fired INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS events(
          seq INTEGER PRIMARY KEY AUTOINCREMENT,request_id TEXT,version TEXT,
          actor TEXT NOT NULL,kind TEXT NOT NULL,summary TEXT NOT NULL,created TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS documents(
          key TEXT PRIMARY KEY,owner TEXT REFERENCES members(role),version TEXT REFERENCES versions(id),
          title TEXT NOT NULL,body TEXT NOT NULL,revision INTEGER NOT NULL,updated TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS revisions(
          document TEXT REFERENCES documents(key),revision INTEGER,note TEXT NOT NULL,
          request_id TEXT,actor TEXT NOT NULL,created TEXT NOT NULL,PRIMARY KEY(document,revision));
        CREATE TABLE IF NOT EXISTS product_updates(
          id TEXT PRIMARY KEY,role TEXT REFERENCES members(role),version TEXT REFERENCES versions(id),
          summary TEXT NOT NULL,validation TEXT NOT NULL,refs TEXT NOT NULL,created TEXT NOT NULL,
          UNIQUE(role,id));
        """)

    def close(self):
        self.db.close()

    @contextlib.contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def meta(self, key, default=None):
        row = self.db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_meta(self, key, value):
        self.db.execute("INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, dump(value)))

    def member(self, role):
        row = self.db.execute("SELECT * FROM members WHERE role=?", (role,)).fetchone()
        if not row:
            raise ValueError("未登记的角色: " + str(role))
        return row

    def actor(self, thread_id):
        row = self.db.execute("SELECT role FROM members WHERE thread_id=?", (thread_id,)).fetchone()
        if not row:
            raise PermissionError("当前任务尚未加入团队；不能自行声明角色。请由总管理登记。")
        return row[0]

    def require_manager(self, role):
        if role != MANAGER:
            raise PermissionError("仅总管理可修改团队绑定、创建/切换版本和裁定验收")

    def current(self):
        return self.meta("current_version")

    def version(self, value=None, writable=True):
        value = value or self.current()
        row = self.db.execute("SELECT * FROM versions WHERE id=?", (value,)).fetchone()
        if not row:
            raise ValueError("版本不存在")
        if writable and row["state"] in {"accepted", "archived"}:
            raise ValueError("已关闭版本只读；需要新工作请创建新版本")
        return row

    def event(self, actor, kind, summary, request_id=None, version=None):
        self.db.execute("INSERT INTO events(request_id,version,actor,kind,summary,created) VALUES(?,?,?,?,?,?)", (request_id, version, actor, kind, summary, now()))

    def enqueue(self, recipient, kind, key, payload, priority=2, request_id=None, version=None):
        self.member(recipient)
        identifier = uid("delivery")
        self.db.execute("""INSERT OR IGNORE INTO outbox(id,recipient,kind,request_id,version,event_key,payload,priority,created,updated)
          VALUES(?,?,?,?,?,?,?,?,?,?)""", (identifier, recipient, kind, request_id, version, key, dump(payload), priority, now(), now()))

    def identity(self, role):
        return json.loads(self.member(role)["card"])

    def active_run(self, role):
        return self.db.execute("SELECT * FROM runs WHERE role=? AND ended IS NULL", (role,)).fetchone()

    def begin(self, role, args):
        request_ids = args.get("request_ids", [])
        if not isinstance(request_ids, list) or len(request_ids) > 30:
            raise ValueError("request_ids 须为至多 30 项列表")
        with self.transaction():
            run = self.active_run(role)
            if run:
                # Retried begin is harmless. A different pending turn must not steal the run.
                if request_ids and not set(request_ids).issubset(json.loads(run["request_ids"])):
                    raise ValueError("此角色已有执行轮次；不能覆盖。先核对旧轮次是否已结束。")
                return {"run_id": run["id"], "state": "working", "duplicate": True}
            version = args.get("version") or self.current()
            if version:
                self.version(version, writable=False)
            for identifier in request_ids:
                req = self.get_request(identifier)
                if req["recipient"] != role:
                    raise PermissionError("不能接手其他角色的请求")
                if req["status"] in TERMINAL:
                    raise ValueError("请求已经结束；勿重复执行: " + identifier)
                if req["version"] != version:
                    raise ValueError("请求所属版本与本轮版本不一致")
            identifier = uid("run")
            self.db.execute("INSERT INTO runs(id,role,version,state,started,request_ids) VALUES(?,?,?,?,?,?)", (identifier, role, version, "working", now(), dump(request_ids)))
            for request_id in request_ids:
                self.db.execute("UPDATE requests SET status='in_progress',updated=? WHERE id=?", (now(), request_id))
                # A recipient's begin is definitive receipt, even if dispatch lost its response.
                self.db.execute("UPDATE outbox SET state='delivered',last_error=NULL,updated=? WHERE request_id=? AND kind='request' AND state IN ('sending','uncertain','pending')", (now(), request_id))
            if request_ids:
                self.db.execute("UPDATE version_roles SET state='working' WHERE version=? AND role=?", (version, role))
        self.render()
        return {"run_id": identifier, "state": "working"}

    def get_request(self, identifier):
        row = self.db.execute("SELECT * FROM requests WHERE id=?", (identifier,)).fetchone()
        if not row:
            raise ValueError("请求不存在: " + str(identifier))
        return row

    def request(self, role, args):
        with self.transaction():
            result = self._request(role, args)
            self.evaluate()
        self.render()
        return result

    def _request(self, role, args, allow_without_run=False):
        key = short(args.get("idempotency_key"), "幂等键", 160)
        old = self.db.execute("SELECT id,status FROM requests WHERE sender=? AND idempotency_key=?", (role, key)).fetchone()
        if old:
            return {"request_id": old["id"], "state": old["status"], "duplicate": True}
        version = self.version(args.get("version"))
        recipient = slug(args.get("to"))
        self.member(recipient)
        if recipient == role:
            raise ValueError("不能通过协作请求唤醒自己；自己的工作写入状态或产品更新")
        kind = args.get("kind")
        if kind not in {"wait", "notify"}:
            raise ValueError("kind 必须为 wait（等待结果）或 notify（无需回复的处理通知）")
        for key_name in ["urgent", "important"]:
            if not isinstance(args.get(key_name), bool):
                raise ValueError(key_name + " 必须由发起模型明确填写布尔值")
        urgent, important = args["urgent"], args["important"]
        priority = 0 if urgent and important else 1 if important else 2 if urgent else 3
        blocking = args.get("blocking", "none")
        if blocking not in {"none", "partial", "all"}:
            raise ValueError("blocking 必须为 none/partial/all")
        reason = short(args.get("reason"), "优先级理由", 300)
        title = short(args.get("title"), "事项", 120)
        action = short(args.get("action"), "需要完成的动作", 2000)
        acceptance = short(args.get("acceptance"), "完成条件", 1200)
        references = refs(args.get("refs", []))
        required = args.get("required_for_review", True)
        if not isinstance(required, bool):
            raise ValueError("required_for_review 须为布尔值")
        run = self.active_run(role)
        if not run and not allow_without_run:
            raise ValueError("先调用 begin 登记本轮工作")
        if run and run["version"] != version["id"]:
            raise ValueError("协作请求必须属于本轮版本")
        identifier = uid("request")
        self.db.execute("""INSERT INTO requests(id,version,sender,recipient,run_id,kind,priority,urgent,important,blocking,reason,title,action,acceptance,refs,required,status,created,updated,idempotency_key)
          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (identifier, version["id"], role, recipient, run["id"] if run else None, kind, priority, int(urgent), int(important), blocking, reason, title, action, acceptance, dump(references), int(required), "queued", now(), now(), args["idempotency_key"]))
        if required and version["state"] == "awaiting_review":
            self.db.execute("UPDATE versions SET state='active',epoch=epoch+1,updated=? WHERE id=?", (now(), version["id"]))
            self.db.execute("UPDATE outbox SET state='cancelled',updated=? WHERE version=? AND kind='review_ready' AND state='pending'", (now(), version["id"]))
        if required:
            self.db.execute("INSERT INTO version_roles(version,role,state) VALUES(?,?,'assigned') ON CONFLICT(version,role) DO UPDATE SET state='assigned'", (version["id"], recipient))
        self.event(role, "request_created", title, identifier, version["id"])
        self.enqueue(recipient, "request", f"{identifier}:1", {"request_id": identifier}, priority, identifier, version["id"])
        return {"request_id": identifier, "state": "queued", "priority": PRIORITY_NAMES[priority]}

    def _resolve(self, role, identifier, summary, references):
        req = self.get_request(identifier)
        if req["recipient"] != role:
            raise PermissionError("只能提交自己收到的协作任务的处理结果")
        summary = short(summary, "处理结果", 1500)
        references = refs(references)
        if req["status"] == "done":
            if req["result"] == summary and json.loads(req["result_refs"]) == references:
                return
            raise ValueError("已提交结果不同；请通过新请求处理返工")
        if req["status"] in TERMINAL:
            raise ValueError("请求已撤回或被替代，不得继续提交完成")
        self.db.execute("UPDATE requests SET status='done',result=?,result_refs=?,updated=? WHERE id=?", (summary, dump(references), now(), identifier))
        self.event(role, "request_done", summary, identifier, req["version"])

    def end(self, role, args):
        identifier = short(args.get("run_id"), "run_id", 80)
        state = args.get("state")
        if state not in STATES - {"working"}:
            raise ValueError("结束状态须为 idle/waiting/submitted/needs_user/interrupted")
        summary = short(args.get("summary", ""), "共享结果摘要", 1000, False)
        outcomes = args.get("results", [])
        if not isinstance(outcomes, list) or len(outcomes) > 30:
            raise ValueError("results 须为至多 30 项列表")
        with self.transaction():
            run = self.db.execute("SELECT * FROM runs WHERE id=? AND role=?", (identifier, role)).fetchone()
            if not run:
                raise PermissionError("本轮登记不存在或不属于当前角色")
            if run["ended"]:
                return {"run_id": identifier, "state": run["state"], "duplicate": True}
            for result in outcomes:
                self._resolve(role, result["request_id"], result["summary"], result.get("refs", []))
            dependencies = args.get("wait_for", [])
            if not isinstance(dependencies, list) or len(dependencies) > 30:
                raise ValueError("wait_for 须为至多 30 项列表")
            if state == "waiting" and not dependencies:
                raise ValueError("等待协作必须列出 wait_for 请求编号；等待用户请用 needs_user")
            if dependencies and state != "waiting":
                raise ValueError("wait_for 只能与 waiting 一起使用")
            for dep in dependencies:
                req = self.get_request(dep)
                if req["sender"] != role or req["kind"] != "wait" or req["version"] != run["version"]:
                    raise PermissionError("只能等待本版本自己发起的 wait 请求")
            outstanding_wait = self.db.execute("SELECT id FROM requests WHERE run_id=? AND kind='wait' AND status NOT IN ('done','cancelled','superseded')", (identifier,)).fetchall()
            if any(row[0] not in dependencies for row in outstanding_wait):
                raise ValueError("本轮仍有等待结果的请求，须在 wait_for 中登记后休眠")
            if state == "submitted":
                remaining = self.db.execute("SELECT id FROM requests WHERE version=? AND recipient=? AND required=1 AND status NOT IN ('done','cancelled','superseded') LIMIT 1", (run["version"], role)).fetchone()
                if remaining:
                    raise ValueError("仍有必需协作任务未完成，不能将本端标记已提交")
            self.db.execute("UPDATE runs SET state=?,ended=?,summary=? WHERE id=?", (state, now(), summary, identifier))
            if dependencies:
                self.db.execute("INSERT INTO barriers VALUES(?,?,?,?,0)", (identifier, role, run["version"], dump(dependencies)))
                self._check_cycles()
            if state in {"submitted", "waiting", "needs_user", "interrupted"}:
                self.db.execute("UPDATE version_roles SET state=?,summary=? WHERE version=? AND role=?", (state, summary, run["version"], role))
            for change in args.get("product_updates", []):
                self._product_update(role, run["version"], change)
            self.evaluate()
        self.render()
        return {"run_id": identifier, "state": state, "saved": True}

    def _check_cycles(self):
        graph = {}
        for b in self.db.execute("SELECT * FROM barriers WHERE fired=0"):
            graph.setdefault(b["role"], set())
            for identifier in json.loads(b["dependencies"]):
                r = self.get_request(identifier)
                if r["status"] not in TERMINAL:
                    graph[b["role"]].add(r["recipient"])
        def visit(node, path):
            if node in path:
                return True
            return any(visit(other, path | {node}) for other in graph.get(node, set()))
        if any(visit(node, set()) for node in graph):
            # Preserve the waiting facts and ask the manager to make a decision once.
            fingerprint = hashlib.sha256(dump({k: sorted(v) for k, v in sorted(graph.items())}).encode()).hexdigest()[:20]
            self.enqueue(MANAGER, "dependency_cycle", "cycle:" + fingerprint, {"summary": "发现相互等待，请读取公共请求并裁定依赖拆分。", "roles": sorted(graph)}, 0, version=self.current())

    def evaluate(self):
        for barrier in self.db.execute("SELECT * FROM barriers WHERE fired=0").fetchall():
            dependencies = [self.get_request(x) for x in json.loads(barrier["dependencies"])]
            # A withdrawn prerequisite requires attention even if another is still pending.
            failed = any(r["status"] in {"cancelled", "superseded", "blocked"} for r in dependencies)
            ready = all(r["status"] == "done" for r in dependencies)
            if failed or ready:
                self.enqueue(barrier["role"], "dependency_ready", "barrier:" + barrier["id"], {
                    "summary": "依赖发生撤回/阻塞，请调整安排。" if failed else "等待的协作结果已齐，可以继续工作。",
                    "request_ids": [r["id"] for r in dependencies]}, min(r["priority"] for r in dependencies), version=barrier["version"])
                self.db.execute("UPDATE barriers SET fired=1 WHERE id=?", (barrier["id"],))
        for version in self.db.execute("SELECT * FROM versions WHERE state='active'").fetchall():
            roles = self.db.execute("SELECT state FROM version_roles WHERE version=?", (version["id"],)).fetchall()
            pending = self.db.execute("SELECT 1 FROM requests WHERE version=? AND required=1 AND status NOT IN ('done','cancelled','superseded') LIMIT 1", (version["id"],)).fetchone()
            if roles and all(r[0] == "submitted" for r in roles) and not pending:
                self.db.execute("UPDATE versions SET state='awaiting_review',updated=? WHERE id=?", (now(), version["id"]))
                self.enqueue(MANAGER, "review_ready", f"review:{version['id']}:{version['epoch']}", {"summary": "本版本所有必需参与方均已提交，协作请求已收束。请按需读取文档，自行决定核验或后续安排。"}, 1, version=version["id"])

    def update_request(self, role, args):
        with self.transaction():
            req = self.get_request(args["request_id"])
            if role not in {req["sender"], req["recipient"], MANAGER}:
                raise PermissionError("只能修改与自己相关的协作请求")
            if int(args.get("expected_revision", 0)) != req["revision"]:
                raise ValueError("请求已被更新，请先读取最新修订")
            if req["status"] in TERMINAL:
                raise ValueError("已结束请求保留历史；返工请创建新的关联请求")
            status = args.get("status", req["status"])
            if status not in {req["status"], "blocked", "cancelled", "superseded"}:
                raise ValueError("此入口支持说明/优先级修订、阻塞、撤回或替代；完成请在 end 提交结果")
            if status in {"cancelled", "superseded"} and role not in {req["sender"], MANAGER}:
                raise PermissionError("撤回或替代由发起方或总管理决定")
            note = short(args.get("note"), "修订说明", 800)
            urgent, important = args.get("urgent", bool(req["urgent"])), args.get("important", bool(req["important"]))
            if not isinstance(urgent, bool) or not isinstance(important, bool):
                raise ValueError("紧急和重要须为布尔值")
            priority = 0 if urgent and important else 1 if important else 2 if urgent else 3
            action = short(args.get("action", req["action"]), "行动要求", 2000)
            references = refs(args.get("refs", json.loads(req["refs"])))
            revision = req["revision"] + 1
            self.db.execute("UPDATE requests SET status=?,priority=?,urgent=?,important=?,action=?,refs=?,revision=?,updated=? WHERE id=?", (status, priority, int(urgent), int(important), action, dump(references), revision, now(), req["id"]))
            self.event(role, "request_updated", note, req["id"], req["version"])
            self.db.execute("UPDATE outbox SET priority=?,updated=? WHERE request_id=? AND state='pending'", (priority, now(), req["id"]))
            if status in {"cancelled", "superseded"}:
                self.db.execute("UPDATE outbox SET state='cancelled',updated=? WHERE request_id=? AND state='pending'", (now(), req["id"]))
            if args.get("requires_action", False) or (status in {"cancelled", "superseded"} and req["status"] == "in_progress"):
                target = req["recipient"] if role != req["recipient"] else req["sender"]
                self.enqueue(target, "request_changed", f"change:{req['id']}:{revision}", {"summary": note, "request_id": req["id"]}, priority, req["id"], req["version"])
            self.evaluate()
        self.render()
        return {"request_id": req["id"], "revision": revision, "state": status}

    def _product_update(self, role, version, args):
        identifier = short(args.get("idempotency_key"), "产品更新幂等键", 160)
        self.db.execute("INSERT OR IGNORE INTO product_updates VALUES(?,?,?,?,?,?,?)", (
            role + ":" + identifier, role, version, short(args.get("summary"), "产品变化", 1000),
            short(args.get("validation", "未验证"), "验证结果", 800), dump(refs(args.get("refs", []))), now()))

    def document_put(self, role, args):
        key = slug(args.get("key"))
        with self.transaction():
            previous = self.db.execute("SELECT * FROM documents WHERE key=?", (key,)).fetchone()
            owner = previous["owner"] if previous else args.get("owner", role)
            self.member(owner)
            if role != owner and role != MANAGER:
                raise PermissionError("此文档由其他角色维护；请提交协作请求")
            version = self.version(args.get("version"))["id"]
            if previous and previous["version"] != version:
                raise ValueError("文档不能通过覆盖迁移到其他项目版本；请使用新的文档标识")
            if int(args.get("expected_revision", 0)) != (previous["revision"] if previous else 0):
                raise ValueError("文档修订冲突，请读取当前版本后重试")
            title = short(args.get("title"), "标题", 160)
            body = short(args.get("body"), "正文", 120000)
            note = short(args.get("change_note"), "修订说明", 1200)
            revision = (previous["revision"] if previous else 0) + 1
            self.db.execute("INSERT INTO documents VALUES(?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET body=excluded.body,title=excluded.title,revision=excluded.revision,updated=excluded.updated", (key, owner, version, title, body, revision, now()))
            self.db.execute("INSERT INTO revisions VALUES(?,?,?,?,?,?)", (key, revision, note, args.get("request_id"), role, now()))
            self.event(role, "document_updated", f"{title} v1.{revision}: {note}", args.get("request_id"), version)
        self.render()
        return {"key": key, "revision": revision, "path": str(self.root / "docs" / "documents" / (key + ".md")), "notified": False}

    def version_create(self, role, args):
        self.require_manager(role)
        identifier = slug(args.get("id"))
        tasks = args.get("tasks")
        if not isinstance(tasks, dict) or MANAGER in tasks:
            raise ValueError("tasks 须为参与执行角色到任务定义的映射；空映射只建立目标，不唤醒成员")
        title, goal = short(args.get("title"), "版本名称", 160), short(args.get("goal"), "版本目标", 2000)
        with self.transaction():
            if self.current():
                current = self.version(writable=False)
                if current["state"] not in {"accepted", "archived"}:
                    raise ValueError("当前版本未关闭；先完成或明确归档，避免跨版本混派")
            self.db.execute("INSERT INTO versions(id,title,goal,state,created,updated) VALUES(?,?,?,'active',?,?)", (identifier, title, goal, now(), now()))
            self.set_meta("current_version", identifier)
            issued = []
            for target, task in tasks.items():
                self.member(target)
                self.db.execute("INSERT INTO version_roles(version,role) VALUES(?,?)", (identifier, target))
                item = dict(task, to=target, version=identifier, kind="notify", required_for_review=True, idempotency_key=f"start:{identifier}:{target}")
                item.setdefault("urgent", False); item.setdefault("important", True)
                item.setdefault("reason", "当前版本分工启动")
                issued.append(self._request(role, item, allow_without_run=True))
            self.event(role, "version_started", title, version=identifier)
        self.render()
        return {"version": identifier, "requests": issued}

    def version_review(self, role, args):
        self.require_manager(role)
        with self.transaction():
            v = self.version(args.get("version"), writable=False)
            decision = args.get("decision")
            if decision not in {"accepted", "archived", "hold"}:
                raise ValueError("decision 须为 accepted/archived/hold；返工请创建定向请求")
            note = short(args.get("summary"), "总管理决定", 1500)
            if decision == "accepted" and v["state"] != "awaiting_review":
                raise ValueError("只有待验收版本可以由总管理标记接受")
            if decision != "hold":
                self.db.execute("UPDATE versions SET state=?,updated=? WHERE id=?", (decision, now(), v["id"]))
                # A manager already reviewing the facts needs no stale wake afterwards.
                self.db.execute("UPDATE outbox SET state='completed',updated=? WHERE version=? AND kind='review_ready' AND recipient=? AND state='pending'", (now(), v["id"], role))
            if decision == "archived":
                self.db.execute("UPDATE outbox SET state='cancelled',updated=? WHERE version=? AND state='pending'", (now(), v["id"]))
            self.event(role, "version_" + decision, note, version=v["id"])
        self.render()
        return {"version": v["id"], "decision": decision}

    def history(self, args):
        limit = min(max(int(args.get("limit", 12)), 1), 50)
        where, values = [], []
        if args.get("version"):
            where.append("version=?"); values.append(args["version"])
        if args.get("role"):
            where.append("(sender=? OR recipient=?)"); values += [args["role"], args["role"]]
        if args.get("status"):
            where.append("status=?"); values.append(args["status"])
        if args.get("query"):
            where.append("(title LIKE ? OR action LIKE ?)"); values += ["%" + args["query"] + "%"] * 2
        sql = "SELECT id,version,sender,recipient,title,kind,priority,status,revision,updated FROM requests"
        if where: sql += " WHERE " + " AND ".join(where)
        return [dict(row) for row in self.db.execute(sql + " ORDER BY created DESC LIMIT ?", values + [limit])]

    def status(self):
        return {"current_version": self.current(), "versions": [dict(x) for x in self.db.execute("SELECT id,title,state FROM versions ORDER BY created DESC")],
                "members": [{"role": row["role"], "label": row["label"], "state": (self.active_run(row["role"]) or {"state": "idle"})["state"]} for row in self.db.execute("SELECT role,label FROM members")],
                "delivery_counts": {row[0]: row[1] for row in self.db.execute("SELECT state,COUNT(*) FROM outbox GROUP BY state")},
                "worker": self.meta("worker", {}),
                "delivery_issues": [dict(x) for x in self.db.execute("SELECT id,recipient,state,last_error FROM outbox WHERE state IN ('uncertain','failed') ORDER BY created DESC LIMIT 10")]}

    def message(self, delivery):
        payload = json.loads(delivery["payload"])
        lines = ["[任务调度中枢]", f"投递编号：{delivery['id']}", f"版本：{delivery['version']}"]
        if delivery["kind"] == "request":
            r = self.get_request(delivery["request_id"])
            lines += [f"请求编号：{r['id']}（修订 {r['revision']}）", f"发起方：{r['sender']} → 接收方：{r['recipient']}",
                      f"协作方式：{'等待结果' if r['kind']=='wait' else '需要处理，无需回复发起方'}；优先级：{PRIORITY_NAMES[r['priority']]}；阻塞：{r['blocking']}",
                      f"优先级理由：{r['reason']}", f"事项：{r['title']}", f"行动：{r['action']}", f"完成条件：{r['acceptance']}"]
            for ref in json.loads(r["refs"]): lines.append(f"按需参考：{ref['path']}（{ref['revision']}）")
            lines.append(f"正式工作前调用中枢 begin，request_ids 填 [\"{r['id']}\"]。收尾时调用 end 提交此请求结果；没有额外协作需要就结束本轮。")
        else:
            lines.append(payload.get("summary", "协作状态发生需要处理的变化。"))
            identifiers = payload.get("request_ids", []) + ([payload["request_id"]] if payload.get("request_id") else [])
            for identifier in identifiers:
                r = self.get_request(identifier)
                lines.append(f"关联请求 {identifier}：{r['title']}；状态 {r['status']}；结果 {r['result']}")
                for ref in json.loads(r["result_refs"]): lines.append(f"结果资料：{ref['path']}（{ref['revision']}）")
            lines.append("正式工作前调用 begin；此消息是结果/状态事件，不要把已完成的关联请求当作新的待执行请求接手。")
        lines += [f"中枢入口：python3 '{self.root / 'hub.py'}' call <操作>（JSON 参数从标准输入传入）",
                  f"协作使用说明：{self.root / 'README.md'}；团队入口：{self.root / 'docs' / 'index.md'}，均按需读取。",
                  "身份或权限不清楚时主动调用 identity。禁止使用任务间直接发消息工具进行协作；通过中枢 request/end。不要为确认收到而创建新请求。"]
        return "\n".join(lines)

    def render(self):
        """Atomic projections. Original bodies are not archived; revision notes are."""
        with open(self.state / "render.lock", "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            docs = self.root / "docs"
            docs.mkdir(exist_ok=True)
            def write(relative, body):
                path = docs / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                if path.exists() and path.read_text() == body: return
                temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
                temporary.write_text(body, encoding="utf-8")
                os.replace(temporary, path)
            current = self.current()
            index = ["# 团队协作入口", "", "文档版本：v1.0", "", "仅用于团队共享与协作，不替代项目记忆或线程私有执行记录。", "", f"当前版本：{current or '尚未启动'}", "", "## 当前版本与历史索引", ""]
            for v in self.db.execute("SELECT * FROM versions ORDER BY created DESC"):
                index.append(f"- [{v['title']}](versions/{v['id']}/overview.md)：{v['state']}；{v['goal']}")
                overview = [f"# {v['title']}", "", f"项目版本：{v['id']}；文档版本：v1.0；状态：{v['state']}", "", "## 总目标", "", v["goal"], "", "## 交付分工", ""]
                for row in self.db.execute("SELECT * FROM version_roles WHERE version=? ORDER BY role", (v["id"],)):
                    overview.append(f"- [{row['role']}](../../roles/{row['role']}.md)：{row['state']}。{row['summary']}")
                overview += ["", "## 共享资料索引", ""]
                for d in self.db.execute("SELECT key,title,revision,owner FROM documents WHERE version=?", (v["id"],)):
                    overview.append(f"- [{d['title']}](../../documents/{d['key']}.md)：v1.{d['revision']}，维护者 {d['owner']}")
                overview += ["", "## 版本记录", ""]
                for e in self.db.execute("SELECT kind,summary,created FROM events WHERE version=? AND kind LIKE 'version_%' ORDER BY seq", (v["id"],)):
                    overview.append(f"- {e['created']} · {e['kind']}：{e['summary']}")
                write(f"versions/{v['id']}/overview.md", "\n".join(overview) + "\n")
            index += ["", "[公共协作历史](public-history.md) · [操作说明](../README.md)", "", "## 修订记录", "", "- v1.0：建立按需阅读的团队入口；状态由中枢登记生成。"]
            write("index.md", "\n".join(index) + "\n")
            history = ["# 公共协作历史", "", "文档结构版本：v1.0。这里只保存协作摘要、状态和资料引用；详细材料按需打开。", ""]
            for r in self.db.execute("SELECT * FROM requests ORDER BY created DESC"):
                history += [f"## {r['title']}", "", f"请求：{r['id']} · 修订：{r['revision']} · 项目版本：{r['version']}",
                            f"{r['sender']} → {r['recipient']} · {r['kind']} · {PRIORITY_NAMES[r['priority']]} · {r['status']}", "", r["action"], "", "完成条件：" + r["acceptance"]]
                for ref in json.loads(r["refs"]): history.append(f"- 资料：{ref['path']}（{ref['revision']}）")
                if r["result"]: history += ["", "处理结果：" + r["result"]]
                for ref in json.loads(r["result_refs"]): history.append(f"- 结果：{ref['path']}（{ref['revision']}）")
                history += ["", "修订与处理记录："]
                for e in self.db.execute("SELECT * FROM events WHERE request_id=? ORDER BY seq", (r["id"],)):
                    history.append(f"- {e['created']} · {e['actor']} · {e['kind']}：{e['summary']}")
                history.append("")
            write("public-history.md", "\n".join(history) + "\n")
            for member in self.db.execute("SELECT * FROM members"):
                role = member["role"]
                body = [f"# {member['label']}：交付与产品更新", "", "文档结构版本：v1.0。只保留团队需要的交付信息；执行过程留在本线程。", "", "## 当前协作事项", ""]
                for r in self.db.execute("SELECT id,title,status FROM requests WHERE version=? AND recipient=? ORDER BY created", (current, role)):
                    body.append(f"- {r['id']} · {r['title']} · {r['status']}")
                updates = self.db.execute("SELECT * FROM product_updates WHERE role=? ORDER BY created DESC", (role,)).fetchall()
                body += ["", f"## 产品更新记录（v1.{len(updates)}）", ""]
                if not updates: body.append("本中枢接入后尚无产品变更记录；不据此推断此前没有变更。")
                for u in updates:
                    body += [f"- {u['created']} · 关联版本 {u['version']}：{u['summary']}", f"  验证：{u['validation']}"]
                    for ref in json.loads(u["refs"]): body.append(f"  资料：{ref['path']}（{ref['revision']}）")
                body += ["", "## 修订记录", "", "- v1.0：建立协作交付与产品更新入口；每次新增产品更新追加记录。"]
                write(f"roles/{role}.md", "\n".join(body) + "\n")
            for d in self.db.execute("SELECT * FROM documents"):
                body = [f"# {d['title']}", "", f"文档版本：v1.{d['revision']} · 维护者：{d['owner']} · 项目版本：{d['version']}", "", d["body"], "", "## 修订记录", ""]
                for r in self.db.execute("SELECT * FROM revisions WHERE document=? ORDER BY revision DESC", (d["key"],)):
                    body.append(f"- v1.{r['revision']} · {r['created']} · {r['actor']}：{r['note']}" + (f"（关联 {r['request_id']}）" if r["request_id"] else ""))
                write(f"documents/{d['key']}.md", "\n".join(body) + "\n")

    def call(self, thread_id, operation, args):
        role = self.actor(thread_id)
        if operation == "identity": return self.identity(role)
        if operation == "begin": return self.begin(role, args)
        if operation == "end": return self.end(role, args)
        if operation == "request": return self.request(role, args)
        if operation == "request_update": return self.update_request(role, args)
        if operation == "request_get":
            result = dict(self.get_request(args["request_id"]))
            for key in ["refs", "result_refs"]: result[key] = json.loads(result[key])
            return result
        if operation == "history": return self.history(args)
        if operation == "status": return self.status()
        if operation == "document_put": return self.document_put(role, args)
        if operation == "document_get":
            row = self.db.execute("SELECT * FROM documents WHERE key=?", (args["key"],)).fetchone()
            if not row: raise ValueError("文档不存在")
            return dict(row)
        if operation == "version_create": return self.version_create(role, args)
        if operation == "version_review": return self.version_review(role, args)
        if operation == "delivery_retry":
            self.require_manager(role)
            with self.transaction():
                row = self.db.execute("SELECT * FROM outbox WHERE id=?", (args["delivery_id"],)).fetchone()
                if not row or row["state"] not in {"failed", "uncertain"}: raise ValueError("仅重试失败或结果待确认的投递")
                self.db.execute("UPDATE outbox SET state='pending',available_at=0,updated=? WHERE id=?", (now(), row["id"]))
            return {"delivery_id": row["id"], "state": "pending"}
        raise ValueError("未知操作: " + operation)


def main():
    parser = argparse.ArgumentParser(description="任务调度中枢：参数使用标准输入 JSON，身份来自 CODEX_THREAD_ID。")
    parser.add_argument("command", choices=["call"])
    parser.add_argument("operation")
    parser.add_argument("--json", dest="json_text", help="短 JSON 参数；复杂内容建议从标准输入传入")
    args = parser.parse_args()
    try:
        text = args.json_text if args.json_text is not None else (sys.stdin.read() if not sys.stdin.isatty() else "{}")
        payload = json.loads(text or "{}")
        if not isinstance(payload, dict): raise ValueError("参数必须为 JSON 对象")
        thread_id = os.environ.get("CODEX_THREAD_ID")
        if not thread_id: raise ValueError("缺少 Codex 任务身份；请在已登记任务中调用，不能手填其他角色冒领")
        hub = Hub()
        try: result = hub.call(thread_id, args.operation, payload)
        finally: hub.close()
        print(dump({"ok": True, "result": result}))
    except (ValueError, PermissionError, KeyError, sqlite3.Error) as error:
        print(dump({"ok": False, "error": str(error)}))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
