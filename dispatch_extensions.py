"""Deployed dependency-cycle and explicit native-completion repairs.

No private team bindings or incident-specific uncertain-delivery overrides.
This is a same-user coordination protocol, not an authentication boundary.
"""
import datetime as dt
import hashlib
import json
import secrets
import time

TERMINAL = {"done", "cancelled", "superseded"}
def dump(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
def now():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

class DispatchExtensions:
    def team_cycle_snapshots(self):
        """Actual registered wait edges only; unrelated branches are not cycle identity."""
        version = self.current()
        edges = {}
        for barrier in self.db.execute("SELECT * FROM barriers WHERE fired=0 AND version=?", (version,)):
            for identifier in json.loads(barrier["dependencies"]):
                req = self.get_request(identifier)
                if (req["version"] == version and req["sender"] == barrier["role"]
                        and req["kind"] == "wait" and req["status"] not in TERMINAL):
                    edges[req["id"]] = req
        graph = {}
        for req in edges.values():
            graph.setdefault(req["sender"], set()).add(req["recipient"])

        def reaches(start, target, seen):
            if start == target:
                return True
            if start in seen:
                return False
            return any(reaches(other, target, seen | {start}) for other in graph.get(start, ()))

        cyclic = [req for req in edges.values() if reaches(req["recipient"], req["sender"], set())]
        groups = []
        while cyclic:
            group = [cyclic.pop()]
            roles = {group[0]["sender"], group[0]["recipient"]}
            changed = True
            while changed:
                changed = False
                for req in cyclic[:]:
                    if roles & {req["sender"], req["recipient"]}:
                        group.append(req)
                        roles.update((req["sender"], req["recipient"]))
                        cyclic.remove(req)
                        changed = True
            group.sort(key=lambda req: req["id"])
            semantic = [{key: req[key] for key in ("id", "sender", "recipient", "status", "blocking")}
                        | {"action_sha256": hashlib.sha256(req["action"].encode()).hexdigest()} for req in group]
            fingerprint = hashlib.sha256(dump([version, semantic]).encode()).hexdigest()
            groups.append({"key": "team-cycle-v1:" + fingerprint, "version": version,
                           "roles": sorted(roles), "requests": [dict(item, revision=req["revision"])
                           for item, req in zip(semantic, group)]})
        return groups

    def enqueue_team_cycles(self, snapshots):
        for group in snapshots:
            if self.db.execute("SELECT 1 FROM outbox WHERE event_key=?", (group["key"],)).fetchone():
                continue
            payload = {"summary": "已登记的wait依赖成环，请读取以下请求并裁定依赖拆分。",
                       "roles": group["roles"], "requests": group["requests"], "cycle_schema": "team-cycle-v1"}
            self.enqueue("manager", "dependency_cycle", group["key"], payload, 0, version=group["version"])
            self.event("system", "team_dependency_cycle_detected", dump(payload), version=group["version"])

    def _check_cycles(self):
        self.enqueue_team_cycles(self.team_cycle_snapshots())

    def reconcile_wakeups(self):
        # Shared behavior (readiness, budget notices) stays intact. Called under the
        # existing transaction before both native claims and worker dispatches.
        super().reconcile_wakeups()
        snapshots = self.team_cycle_snapshots()
        valid = {group["key"] for group in snapshots}
        for notice in self.db.execute("SELECT * FROM outbox WHERE kind='dependency_cycle' AND state='pending'").fetchall():
            if notice["event_key"] in valid:
                continue
            reason = ("legacy_cycle_has_no_request_snapshot" if not notice["event_key"].startswith("team-cycle-v1:")
                      else "wait_cycle_resolved_or_materially_changed")
            # Keep original payload/key/error/history verbatim; only retire an
            # unsent derived notice. Never alter requests, barriers or sent notices.
            self.db.execute("UPDATE outbox SET state='archived',updated=? WHERE id=? AND state='pending'", (now(), notice["id"]))
            self.event("system", "team_dependency_cycle_filtered", dump({"delivery_id": notice["id"],
                       "event_key": notice["event_key"], "reason": reason}), version=notice["version"])
        self.enqueue_team_cycles(snapshots)

    def native_reconcile_target(self, args):
        """Only confirmed native deliveries in this instance/current version."""
        row = self.db.execute("SELECT * FROM outbox WHERE id=?", (args.get("delivery_id"),)).fetchone()
        if not row or row["state"] != "delivered" or row["route"] != "desktop-native":
            raise ValueError("Only delivered desktop-native records can be reconciled")
        if row["version"] != self.current():
            raise ValueError("Delivery is not in the current version")
        target = self.member(row["recipient"])["thread_id"]
        if (args.get("expected_thread_id") != target or not row["turn_id"]
                or args.get("expected_turn_id") != row["turn_id"]):
            raise ValueError("Delivery target or sent turn does not match")
        if self.active_run(row["recipient"]):
            raise ValueError("Recipient still has an active run")
        return row, target

    @staticmethod
    def native_ledger_signature(row):
        fields = ("id", "recipient", "version", "state", "route", "turn_id", "attempts", "payload")
        return hashlib.sha256(dump({key: row[key] for key in fields}).encode()).hexdigest()

    @staticmethod
    def validate_native_completion(proof, target, old_turn):
        """Only structured native status/history; never infer from age or UUID order."""
        if not isinstance(proof, dict):
            raise ValueError("缺少真实原生证据")
        snapshots = [proof.get("before", {}), proof.get("after", {})]
        for snap in snapshots:
            if not isinstance(snap, dict) or snap.get("thread_id") != target or snap.get("status") != "idle":
                raise ValueError("原生目标不匹配、仍活动或状态未知")
            latest = snap.get("latest", {})
            if not isinstance(latest, dict) or not latest.get("id") or latest.get("status") != "completed" or latest.get("error"):
                raise ValueError("最新轮次未明确完成")
        if snapshots[0]["latest"] != snapshots[1]["latest"]:
            raise ValueError("核对期间最新轮次变化，保留原投递")
        history = proof.get("history", {})
        if not isinstance(history, dict):
            raise ValueError("缺少同线程有界历史")
        turns = history.get("turns", [])
        if history.get("thread_id") != target or history.get("status") != "idle" or not isinstance(turns, list) or not turns or len(turns) > 15:
            raise ValueError("缺少同线程有界历史")
        if turns[0] != snapshots[1]["latest"]:
            raise ValueError("历史与当前最新轮次不一致")
        seen, newer_start = set(), None
        for turn in turns:
            if not isinstance(turn, dict):
                raise ValueError("历史轮次格式未知")
            start, end = turn.get("startedAt"), turn.get("completedAt")
            if (not turn.get("id") or turn["id"] in seen or turn.get("status") != "completed"
                    or turn.get("error") or type(start) not in (int, float) or type(end) not in (int, float)
                    or not 0 < start <= end or (newer_start is not None and end > newer_start)):
                raise ValueError("历史轮次未完成、未知或顺序证据不成立")
            seen.add(turn["id"])
            newer_start = start
            if turn["id"] == old_turn:
                return
        raise ValueError("有界历史未找到已发送轮次；禁止猜测完成")

    def call(self, thread_id, operation, args):
        if operation not in {"delivery_reconcile_native_prepare", "delivery_reconcile_native_commit"}:
            return super().call(thread_id, operation, args)
        role = self.actor(thread_id)
        self.require_manager(role)
        with self.transaction():
            row, target = self.native_reconcile_target(args)
            key = "team_native_reconcile:" + row["id"]
            if operation.endswith("_prepare"):
                token = secrets.token_hex(24)
                mode = "native-turn"
                self.set_meta(key, {"token": token, "expires": time.time() + 60,
                                   "thread_id": target, "turn_id": args["expected_turn_id"], "actor": thread_id,
                                   "ledger_signature": self.native_ledger_signature(row), "mode": mode})
                return {"delivery_id": row["id"], "thread_id": target, "turn_id": args["expected_turn_id"],
                        "token": token, "mode": mode}
            ticket = self.meta(key, {})
            if (not ticket or ticket.get("token") != args.get("token") or ticket.get("expires", 0) < time.time()
                    or ticket.get("actor") != thread_id or ticket.get("thread_id") != target
                    or ticket.get("turn_id") != args.get("expected_turn_id")
                    or ticket.get("ledger_signature") != self.native_ledger_signature(row)):
                raise ValueError("缺少本次新鲜核对票据；须经native_call重新取得实际证据")
            completed_turn = ticket["turn_id"]
            self.validate_native_completion(args.get("proof"), target, completed_turn)
            # The transaction rechecks delivery, binding and active_run after native reads.
            # Preserve request status, old turn, payload, attempts and the original error.
            self.db.execute("UPDATE outbox SET state='completed',turn_id=?,updated=? WHERE id=? AND state=? AND turn_id IS ?",
                            (completed_turn, now(), row["id"], row["state"], row["turn_id"]))
            self.event(role, "team_native_delivery_completed", dump({"delivery_id": row["id"],
                       "thread_id": target, "sent_turn_id": row["turn_id"], "completed_turn_id": completed_turn,
                       "previous_receipt": {field: row[field] for field in ("state", "route", "turn_id", "last_error", "attempts")},
                       "locked_payload_sha256": hashlib.sha256(row["payload"].encode()).hexdigest(),
                       "proof": args["proof"], "previous_error": row["last_error"]}), row["request_id"], row["version"])
            self.db.execute("DELETE FROM meta WHERE key=?", (key,))
            return {"delivery_id": row["id"], "thread_id": target, "state": "completed", "request_unchanged": True}

