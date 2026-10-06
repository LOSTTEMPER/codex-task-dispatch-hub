"""Desktop-owned dispatch only. Never launches a Codex execution engine."""
from __future__ import annotations
import json
from pathlib import Path
import socket
import struct
import time
import uuid

class RPCError(RuntimeError):
    pass


class DesktopIPC:
    """Versioned same-user follower interface observed in the installed desktop.

    Uses ordinary request discovery and owner validation. Never connects to the
    code-signing-restricted native app-tools pipe or modifies its authorizer.
    Unknown owner/protocol is an error; it is not permission to interrupt a task.
    """
    def __init__(self, path=None):
        self.path = str(path or Path.home() / ".codex" / "ipc" / "ipc.sock")
        self.client = "initializing-client"
        self.socket = socket.socket(socket.AF_UNIX)
        self.socket.settimeout(12)
        self.socket.connect(self.path)
        result = self.request("initialize", {"clientType": "task-dispatch-hub"}, 0, timeout=5)
        self.client = result["result"]["clientId"]

    def _write(self, message):
        data = json.dumps(message, ensure_ascii=False).encode()
        self.socket.sendall(struct.pack("<I", len(data)) + data)

    def _read_exact(self, size):
        data = b""
        while len(data) < size:
            chunk = self.socket.recv(size - len(data))
            if not chunk: raise ConnectionError("desktop IPC closed")
            data += chunk
        return data

    def request(self, method, params, version, target=None, timeout=12):
        identifier = str(uuid.uuid4())
        message = {"type": "request", "requestId": identifier, "sourceClientId": self.client,
                   "version": version, "method": method, "params": params, "timeoutMs": int(timeout * 1000)}
        if target: message["targetClientId"] = target
        self._write(message)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.socket.settimeout(max(.1, deadline - time.monotonic()))
            size = struct.unpack("<I", self._read_exact(4))[0]
            if size > 256 * 1024 * 1024: raise RPCError("invalid desktop IPC frame")
            response = json.loads(self._read_exact(size))
            if response.get("type") == "client-discovery-request":
                self._write({"type": "client-discovery-response", "requestId": response["requestId"],
                             "result": {"canHandle": False}})
            if response.get("requestId") == identifier and response.get("type") == "response":
                return response
        raise TimeoutError("desktop IPC outcome unknown: " + method)

    def owner(self, thread_id):
        result = self.request("thread-owner-discovery", {"hostId": "local", "conversationId": thread_id}, 1)
        if result.get("resultType") == "success": return result.get("handledByClientId")
        if result.get("error") == "no-client-found": return None
        raise RPCError("desktop owner discovery failed: " + str(result.get("error")))

    def start(self, thread_id, owner, text, message_id):
        result = self.request("thread-follower-start-turn", {"conversationId": thread_id,
            "turnStart": {"request": {"threadId": thread_id, "clientUserMessageId": message_id,
                                      "input": [{"type": "text", "text": text, "text_elements": []}]},
                          "context": {"inheritThreadSettings": True}}}, 2, target=owner, timeout=35)
        if result.get("resultType") != "success":
            raise RPCError("desktop start outcome not confirmed: " + str(result.get("error")))
        return result.get("result", {})

    def runtime(self, thread_id, owner):
        self._write({"type": "broadcast", "sourceClientId": self.client,
            "method": "thread-stream-following-changed", "version": 1,
            "params": {"conversationId": thread_id, "hostId": "local", "following": True},
            "targetClientIds": [owner]})
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            self.socket.settimeout(max(.1, deadline - time.monotonic()))
            size = struct.unpack("<I", self._read_exact(4))[0]
            if size > 256 * 1024 * 1024: raise RPCError("invalid desktop IPC frame")
            message = json.loads(self._read_exact(size))
            params = message.get("params", {})
            change = params.get("change", {})
            if params.get("conversationId") == thread_id and change.get("type") == "snapshot":
                state = change.get("conversationState", {})
                # Never persist or return conversation contents to the ledger.
                return {"status": state.get("threadRuntimeStatus", {}).get("type"),
                        "provider": state.get("modelProvider")}
        raise TimeoutError("desktop live state unavailable")

    def budget_notice(self, thread_id, owner, notice, message_id):
        result = self.request('thread-follower-start-turn', {'conversationId': thread_id,
            'turnStart': {'request': {'threadId': thread_id, 'clientUserMessageId': message_id,
                'input': [], 'toolOutput': {'name': 'budget_notice', 'namespace': 'task_dispatch_hub',
                                          'output': json.dumps(notice, ensure_ascii=False)}},
                'context': {'inheritThreadSettings': True}}}, 2, target=owner, timeout=35)
        if result.get('resultType') != 'success':
            raise RPCError('budget tool output outcome unknown: ' + str(result.get('error')))
        return result.get('result', {})

    def close(self):
        self.socket.close()
