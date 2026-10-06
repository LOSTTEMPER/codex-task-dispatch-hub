"""Only local cycle detection/filtering, with an isolated in-memory ledger."""
import json
from pathlib import Path
import sqlite3
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from hub import Hub as TeamHub


class TeamCycleTests(unittest.TestCase):
    def setUp(self):
        self.hub = TeamHub.__new__(TeamHub)
        self.hub.db = sqlite3.connect(":memory:")
        self.hub.db.row_factory = sqlite3.Row
        self.hub.db.executescript("""
          CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT);
          INSERT INTO meta VALUES('current_version','"test-version"');
          CREATE TABLE versions(id TEXT,state TEXT);
          INSERT INTO versions VALUES('test-version','active');
          CREATE TABLE members(role TEXT);
          INSERT INTO members VALUES('manager');
          CREATE TABLE requests(id TEXT PRIMARY KEY,version TEXT,sender TEXT,recipient TEXT,kind TEXT,status TEXT,
            blocking TEXT,revision INTEGER,action TEXT);
          CREATE TABLE barriers(id TEXT PRIMARY KEY,role TEXT,version TEXT,dependencies TEXT,fired INTEGER);
          CREATE TABLE outbox(id TEXT PRIMARY KEY,recipient TEXT,kind TEXT,request_id TEXT,version TEXT,event_key TEXT UNIQUE,
            payload TEXT,priority INTEGER,state TEXT DEFAULT 'pending',attempts INTEGER DEFAULT 0,
            turn_id TEXT,route TEXT,last_error TEXT,created TEXT,updated TEXT);
          CREATE TABLE events(request_id TEXT,version TEXT,actor TEXT,kind TEXT,summary TEXT,created TEXT);
        """)
        self.hub.budgets = type("FixtureBudget", (), {"reconcile": lambda _: None})()

    def tearDown(self):
        self.hub.close()

    def edge(self, identifier, sender, recipient, kind="wait", status="queued", version="test-version"):
        with self.hub.db:
            self.hub.db.execute("INSERT INTO requests VALUES(?,?,?,?,?,?,'partial',1,'original action')",
                                (identifier, version, sender, recipient, kind, status))
            self.hub.db.execute("INSERT INTO barriers VALUES(?,?,?,?,0)",
                                ("barrier:" + identifier, sender, version, json.dumps([identifier])))

    def check(self):
        with self.hub.transaction():
            self.hub._check_cycles()
            self.hub.reconcile_wakeups()

    def pending(self):
        return self.hub.db.execute("SELECT * FROM outbox WHERE state='pending'").fetchall()

    def test_same_registered_cycle_has_one_notice_and_no_business_mutation(self):
        self.edge("mi", "manager", "integration")
        self.edge("im", "integration", "manager")
        before = [tuple(r) for r in self.hub.db.execute("SELECT * FROM requests ORDER BY id")]
        barriers = [tuple(r) for r in self.hub.db.execute("SELECT * FROM barriers ORDER BY id")]
        for _ in range(5): self.check()
        self.assertEqual(len(self.pending()), 1)
        self.assertEqual([tuple(r) for r in self.hub.db.execute("SELECT * FROM requests ORDER BY id")], before)
        self.assertEqual([tuple(r) for r in self.hub.db.execute("SELECT * FROM barriers ORDER BY id")], barriers)
        self.assertEqual(self.hub.db.execute("SELECT count(*) FROM events WHERE kind='team_dependency_cycle_detected'").fetchone()[0], 1)

    def test_acyclic_branch_duplicate_barrier_and_note_revision_do_not_repeat(self):
        self.edge("ma", "manager", "art")
        self.edge("am", "art", "manager")
        self.check()
        original = self.pending()[0]["event_key"]
        self.edge("iu", "integration", "ui")
        with self.hub.db:
            self.hub.db.execute("INSERT INTO barriers VALUES('duplicate','manager','test-version','[\"ma\"]',0)")
            self.hub.db.execute("UPDATE requests SET revision=revision+1 WHERE id='ma'")
        self.check()
        self.assertEqual([r["event_key"] for r in self.pending()], [original])
        with self.hub.db:
            self.hub.db.execute("UPDATE requests SET status='done' WHERE id='iu'")
        self.check()
        self.assertEqual([r["event_key"] for r in self.pending()], [original])

    def test_notify_and_resolved_dependencies_are_not_reverse_waits(self):
        self.edge("mi", "manager", "integration")
        self.edge("ia", "integration", "art")
        self.edge("ai", "art", "integration", kind="notify", status="in_progress")
        self.edge("am-done", "art", "manager", status="done")
        self.edge("ma-old", "manager", "art", status="superseded")
        self.check()
        self.assertEqual(self.pending(), [])

    def test_new_dependency_and_material_change_report_once(self):
        self.edge("mi", "manager", "integration")
        self.edge("im", "integration", "manager")
        self.check()
        first = self.pending()[0]["event_key"]
        with self.hub.db:
            self.hub.db.execute("UPDATE requests SET status='blocked' WHERE id='im'")
        self.check()
        second = self.pending()[0]["event_key"]
        self.assertNotEqual(first, second)
        self.edge("im-new", "integration", "manager")
        self.check()
        third = self.pending()[0]["event_key"]
        self.assertNotEqual(second, third)
        self.check()
        self.assertEqual([r["event_key"] for r in self.pending()], [third])
        self.assertEqual(self.hub.get_request("im")["status"], "blocked")

    def test_resolved_or_legacy_pending_notice_archived_with_original_payload(self):
        self.edge("mi", "manager", "integration")
        self.edge("im", "integration", "manager")
        self.check()
        original = dict(self.pending()[0])
        with self.hub.transaction():
            self.hub.enqueue("manager", "dependency_cycle", "cycle:legacy", {"summary": "original legacy"}, 0, version="test-version")
            self.hub.db.execute("UPDATE requests SET status='done' WHERE id='im'")
            self.hub.reconcile_wakeups()
        self.assertEqual(self.pending(), [])
        after = dict(self.hub.db.execute("SELECT * FROM outbox WHERE id=?", (original["id"],)).fetchone())
        for key in original.keys() - {"state", "updated"}: self.assertEqual(after[key], original[key])
        self.assertEqual(after["state"], "archived")
        self.assertEqual(self.hub.get_request("mi")["status"], "queued")
        legacy = self.hub.db.execute("SELECT * FROM outbox WHERE event_key='cycle:legacy'").fetchone()
        self.assertEqual(json.loads(legacy["payload"]), {"summary": "original legacy"})

    def test_sent_history_and_other_versions_preserved(self):
        self.edge("mi", "manager", "integration", version="old-version")
        self.edge("im", "integration", "manager", version="old-version")
        with self.hub.transaction():
            for key, state in (("cycle:old1", "completed"), ("cycle:old2", "delivered")):
                self.hub.enqueue("manager", "dependency_cycle", key, {"summary": "sent history"}, 0, version="test-version")
                self.hub.db.execute("UPDATE outbox SET state=? WHERE event_key=?", (state, key))
        before = [tuple(r) for r in self.hub.db.execute("SELECT * FROM outbox ORDER BY id")]
        self.check()
        self.assertEqual([tuple(r) for r in self.hub.db.execute("SELECT * FROM outbox ORDER BY id")], before)

    def test_independent_new_cycle_reports_without_repeating_existing_group(self):
        self.edge("mi", "manager", "integration")
        self.edge("im", "integration", "manager")
        self.check()
        old = self.pending()[0]["event_key"]
        self.edge("au", "art", "ui")
        self.edge("ua", "ui", "art")
        self.check()
        self.assertEqual(len(self.pending()), 2)
        self.assertIn(old, [r["event_key"] for r in self.pending()])
        self.assertEqual(self.hub.db.execute("SELECT count(*) FROM events WHERE kind='team_dependency_cycle_detected'").fetchone()[0], 2)


if __name__ == "__main__":
    unittest.main()
