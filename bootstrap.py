#!/usr/bin/env python3
"""Initialize a dispatch hub from a local, git-ignored team file."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from hub import Hub, MANAGER, dump, now, slug, short


def load_config(path: Path):
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError('team config must be an object')
    members = data.get("members")
    if not isinstance(members, list) or len(members) < 2:
        raise ValueError("team config requires at least a manager and one member")
    if any(not isinstance(item, dict) for item in members):
        raise ValueError('each member must be an object')
    roles = [slug(item.get("role")) for item in members]
    if roles.count(MANAGER) != 1 or len(set(roles)) != len(roles):
        raise ValueError("roles must be unique and include exactly one manager")
    for item in members:
        for key in ("role", "thread_id", "label", "scope"):
            if not isinstance(item.get(key), str) or not item[key].strip():
                raise ValueError(f"member field {key} must be non-empty text")
        if "replace-with" in item["thread_id"]:
            raise ValueError("replace every example thread_id before bootstrap")
    if len({item['thread_id'] for item in members}) != len(members):
        raise ValueError('thread IDs must be unique')
    onboarding = data.get('onboarding')
    if onboarding is not None:
        if not isinstance(onboarding, dict):
            raise ValueError('onboarding must be an object')
        slug(onboarding.get('version'))
        short(onboarding.get('title'), 'onboarding title', 160)
        short(onboarding.get('goal'), 'onboarding goal', 2000)
    return data, members


def identity_block(item, root):
    return "\n".join([
        "<team_identity>",
        "card_version: 1.2",
        "thread_id: " + item["thread_id"],
        "role: " + item["role"],
        "name: " + item["label"],
        "scope: " + item["scope"],
        "collaboration_authority: Maintain this role's delivery records and "
        "product updates, create hub requests, and read shared collaboration documents.",
        "authority_boundary: This card does not grant installation, deployment, "
        "data migration, billing, or external model access.",
        f"hub_entry: execute {root / 'native_call.js'} in the current tools context; pass {root / 'hub.py'} as hubPath.",
        "identity_check: call identity when role or authority is unclear; the actual "
        "CODEX_THREAD_ID is authoritative.",
        "turn_protocol: call begin before formal work and end before the final response; "
        "coordinate with other conversations only through the dispatch hub.",
        "compaction: preserve this complete block through every context compaction; "
        "never adopt another conversation's quoted identity card.",
        "</team_identity>",
    ])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/team.json")
    args = parser.parse_args()
    config_path = Path(args.config).expanduser().resolve()
    data, members = load_config(config_path)
    manager = next(item for item in members if item["role"] == MANAGER)
    if os.environ.get("CODEX_THREAD_ID") != manager["thread_id"]:
        raise SystemExit("run bootstrap from the configured manager conversation")

    hub = Hub()
    try:
        with hub.transaction():
            if hub.db.execute("SELECT 1 FROM members LIMIT 1").fetchone():
                raise SystemExit("hub is already initialized; bindings were not changed")
            for item in members:
                block = identity_block(item, hub.root)
                card = {
                    "role": item["role"],
                    "thread_id": item["thread_id"],
                    "card_version": "1.2",
                    "identity_block": block,
                }
                hub.db.execute(
                    "INSERT INTO members VALUES(?,?,?,?,?)",
                    (item["role"], item["thread_id"], item["label"], dump(card), now()),
                )

            onboarding = data.get("onboarding")
            result = {"members": [item["role"] for item in members]}
            if onboarding:
                tasks = {}
                for item in members:
                    if item["role"] == MANAGER:
                        continue
                    tasks[item["role"]] = {
                        "title": "Join the task dispatch hub",
                        "action": identity_block(item, hub.root) + "\n"
                        "Read README.md and the shared collaboration rules. Query identity, "
                        "then use begin/end to complete this onboarding request. Do not perform "
                        "unrelated product work during onboarding.",
                        "acceptance": "Identity matches the current thread and the request is "
                        "completed through begin/end without creating a receipt request.",
                    }
                result["onboarding"] = hub.version_create(MANAGER, {
                    "id": onboarding["version"],
                    "title": onboarding["title"],
                    "goal": onboarding["goal"],
                    "tasks": tasks,
                })
        hub.render()
    finally:
        hub.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
