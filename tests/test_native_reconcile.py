"""Narrow adapter tests; in-memory ledger only, no live TeamHub initialization."""
import copy
import json
from pathlib import Path
import sqlite3
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hub import Hub as TeamHub


class ReconcileTests(unittest.TestCase):
    def setUp(self):
        self.hub = TeamHub.__new__(TeamHub)
        self.hub.db = sqlite3.connect(":memory:")
        self.hub.db.row_factory = sqlite3.Row
        self.hub.db.executescript("""
          CREATE TABLE members(role TEXT, thread_id TEXT);
          INSERT INTO members VALUES('manager','fixture-manager'),('runtime','fixture-runtime'),('art','fixture-art');
          CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT);
          INSERT INTO meta VALUES('current_version','"fixture-version"');
          CREATE TABLE runs(role TEXT,ended TEXT);
          CREATE TABLE requests(id TEXT,status TEXT);
          INSERT INTO requests VALUES('request-old','in_progress');
          CREATE TABLE outbox(id TEXT,recipient TEXT,version TEXT,state TEXT,route TEXT,turn_id TEXT,
            request_id TEXT,last_error TEXT,updated TEXT,payload TEXT,attempts INTEGER);
          INSERT INTO outbox VALUES('delivery-old','art','fixture-version','delivered','desktop-native',
            'turn-old','request-old','timed out','before','original message',1);
          INSERT INTO outbox VALUES('delivery-next','art','fixture-version','pending',NULL,
            NULL,NULL,NULL,'before','next message',0);
          CREATE TABLE events(request_id TEXT,version TEXT,actor TEXT,kind TEXT,summary TEXT,created TEXT);
        """)
        self.args = dict(delivery_id="delivery-old", expected_thread_id="fixture-art", expected_turn_id="turn-old")
        latest = dict(id="turn-new", status="completed", error=None, startedAt=20, completedAt=30)
        snap = dict(thread_id="fixture-art", status="idle", latest=latest)
        self.proof = dict(before=copy.deepcopy(snap), after=copy.deepcopy(snap), history=dict(
            thread_id="fixture-art", status="idle", turns=[latest,
            dict(id="turn-old", status="completed", error=None, startedAt=10, completedAt=15)]))

    def tearDown(self):
        self.hub.close()

    def prepare(self):
        return self.hub.call("fixture-manager", "delivery_reconcile_native_prepare", self.args)

    def commit(self, ticket):
        return self.hub.call("fixture-manager", "delivery_reconcile_native_commit",
                             dict(self.args, token=ticket["token"], proof=self.proof))

    def state(self):
        return self.hub.db.execute("SELECT state FROM outbox WHERE id='delivery-old'").fetchone()[0]

    def test_completed_preserves_business_and_original_receipt(self):
        before = dict(self.hub.db.execute("SELECT * FROM outbox WHERE id='delivery-old'").fetchone())
        ticket = self.prepare()
        self.assertEqual(self.commit(ticket)["state"], "completed")
        after = dict(self.hub.db.execute("SELECT * FROM outbox WHERE id='delivery-old'").fetchone())
        for key in before.keys() - {"state", "updated"}:
            self.assertEqual(before[key], after[key])
        self.assertEqual(self.hub.db.execute("SELECT status FROM requests").fetchone()[0], "in_progress")
        self.assertEqual(self.hub.db.execute("SELECT state,attempts FROM outbox WHERE id='delivery-next'").fetchone()[:], ("pending", 0))
        self.assertEqual(self.hub.db.execute("SELECT count(*) FROM events").fetchone()[0], 1)
        with self.assertRaises(ValueError):
            self.commit(ticket)

    def test_active_or_unknown_native_keeps_delivery(self):
        for state in ("active", "unknown", None):
            with self.subTest(state=state):
                ticket = self.prepare()
                self.proof["after"]["status"] = state
                with self.assertRaises(ValueError):
                    self.commit(ticket)
                self.assertEqual(self.state(), "delivered")

    def test_missing_old_turn_or_new_turn_change_rejected(self):
        ticket = self.prepare()
        original = copy.deepcopy(self.proof)
        for change in ("missing", "active_old", "changed_latest", "wrong_target"):
            with self.subTest(change=change):
                self.proof = copy.deepcopy(original)
                if change == "missing": self.proof["history"]["turns"].pop()
                if change == "active_old": self.proof["history"]["turns"][1]["status"] = "inProgress"
                if change == "changed_latest": self.proof["after"]["latest"]["id"] = "different"
                if change == "wrong_target": self.proof["history"]["thread_id"] = "other-team"
                with self.assertRaises(ValueError): self.commit(ticket)
                self.assertEqual(self.state(), "delivered")

    def test_commit_rechecks_active_run_and_turn(self):
        ticket = self.prepare()
        with self.hub.db:
            self.hub.db.execute("INSERT INTO runs VALUES('art',NULL)")
        with self.assertRaises(ValueError): self.commit(ticket)
        with self.hub.db:
            self.hub.db.execute("DELETE FROM runs")
            self.hub.db.execute("UPDATE outbox SET turn_id='changed' WHERE id='delivery-old'")
        with self.assertRaises(ValueError): self.commit(ticket)
        self.assertEqual(self.state(), "delivered")

    def test_manager_only_and_target_scope(self):
        for op in ("delivery_reconcile_native_prepare", "delivery_reconcile_native_commit"):
            with self.assertRaises(PermissionError): self.hub.call("fixture-runtime", op, self.args)
        for field, value in (("state", "uncertain"), ("version", "other-team"), ("route", "desktop-owner")):
            with self.subTest(field=field):
                with self.hub.db:
                    self.hub.db.execute("UPDATE outbox SET state='delivered',recipient='art',version='fixture-version',route='desktop-native' WHERE id='delivery-old'")
                    self.hub.db.execute(f"UPDATE outbox SET {field}=? WHERE id='delivery-old'", (value,))
                with self.assertRaises(ValueError): self.prepare()

    def test_expired_or_missing_ticket_rejected(self):
        ticket = self.prepare()
        with self.hub.db:
            self.hub.db.execute("DELETE FROM meta WHERE key LIKE 'team_native_reconcile:%'")
        with self.assertRaises(ValueError): self.commit(ticket)
        self.assertEqual(self.state(), "delivered")



if __name__ == "__main__":
    unittest.main()
