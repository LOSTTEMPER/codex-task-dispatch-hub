import json
import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import bootstrap
from bootstrap import identity_block, load_config
from hub import Hub


class BootstrapTests(unittest.TestCase):
    def write_config(self, payload):
        temp = tempfile.TemporaryDirectory()
        path = Path(temp.name) / "team.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        self.addCleanup(temp.cleanup)
        return path

    def valid_members(self):
        return [
            {
                "role": "manager",
                "thread_id": "thread-manager-example",
                "label": "Coordinator",
                "scope": "Coordinate delivery and review.",
            },
            {
                "role": "backend",
                "thread_id": "thread-backend-example",
                "label": "Backend",
                "scope": "Own service contracts and implementation.",
            },
        ]

    def test_loads_valid_team(self):
        payload = {"members": self.valid_members()}
        data, members = load_config(self.write_config(payload))
        self.assertEqual(data, payload)
        self.assertEqual([member["role"] for member in members], ["manager", "backend"])

    def test_rejects_example_placeholder_thread_ids(self):
        members = self.valid_members()
        members[1]["thread_id"] = "replace-with-backend-thread-id"
        with self.assertRaisesRegex(ValueError, "replace every example thread_id"):
            load_config(self.write_config({"members": members}))

    def test_requires_exactly_one_manager_and_unique_roles(self):
        members = self.valid_members()
        members[1]["role"] = "manager"
        with self.assertRaisesRegex(ValueError, "exactly one manager"):
            load_config(self.write_config({"members": members}))

    def test_requires_non_empty_member_fields(self):
        members = self.valid_members()
        members[1]["scope"] = ""
        with self.assertRaisesRegex(ValueError, "field scope"):
            load_config(self.write_config({"members": members}))

    def test_identity_card_uses_only_supplied_member_and_hub_path(self):
        member = self.valid_members()[1]
        block = identity_block(member, Path("/opt/dispatch-hub"))
        self.assertIn("thread_id: thread-backend-example", block)
        self.assertIn("role: backend", block)
        self.assertIn("/opt/dispatch-hub/hub.py", block)
        self.assertNotIn("thread-manager-example", block)

    def test_main_initializes_private_bindings_and_onboarding(self):
        payload = {
            "members": self.valid_members(),
            "onboarding": {
                "version": "team-onboarding-v1",
                "title": "Onboarding",
                "goal": "Verify the hub protocol.",
            },
        }
        config_path = self.write_config(payload)
        with tempfile.TemporaryDirectory() as state_root:
            private_hub = Hub(state_root)
            stdout = io.StringIO()
            with (
                patch.object(bootstrap, "Hub", return_value=private_hub),
                patch.object(os, "environ", dict(os.environ, CODEX_THREAD_ID="thread-manager-example")),
                patch("sys.argv", ["bootstrap.py", "--config", str(config_path)]),
                redirect_stdout(stdout),
            ):
                bootstrap.main()

            result = json.loads(stdout.getvalue())
            self.assertEqual(result["members"], ["manager", "backend"])
            reopened = Hub(state_root)
            try:
                self.assertEqual(reopened.actor("thread-backend-example"), "backend")
                request = reopened.db.execute(
                    "SELECT recipient, status FROM requests WHERE recipient='backend'"
                ).fetchone()
                self.assertEqual((request["recipient"], request["status"]), ("backend", "queued"))
            finally:
                reopened.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
