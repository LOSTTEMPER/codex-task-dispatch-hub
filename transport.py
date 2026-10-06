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
    def __init__(self, path=None, cancel=None):
        self.cancel = cancel
        self.path = str(path or Path.home() / ".codex" / "ipc" / "ipc.sock")
        self.client = "initializing-client"
        self.socket = socket.socket(socket.AF_UNIX)
        self.socket.settimeout(.25)
        try:
            self.socket.connect(self.path)
            result = self.request("initialize", {"clientType": "task-dispatch-hub"}, 0, timeout=5)
            self.client = result["result"]["clientId"]
        except BaseException:
            self.socket.close()
            raise

    MAX_FRAME = 32 * 1024 * 1024

    def _check(self, deadline):
        if getattr(self, 'cancel', None) is not None and self.cancel.is_set():
            raise InterruptedError("desktop IPC cancelled; delivery outcome may be unknown")
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError("desktop IPC absolute deadline exceeded")

    def _write(self, message, deadline):
        self._check(deadline)
        data = json.dumps(message, ensure_ascii=False).encode()
        if len(data) > self.MAX_FRAME:
            raise RPCError("desktop IPC frame exceeds limit")
        self._check(deadline)
        frame = memoryview(struct.pack("<I", len(data)) + data)
        while frame:
            self._check(deadline)
            self.socket.settimeout(min(.25, max(.001, deadline - time.monotonic())))
            try:
                sent = self.socket.send(frame[:65536])
            except socket.timeout:
                continue
            if sent <= 0:
                raise ConnectionError('desktop IPC closed while writing')
            frame = frame[sent:]
        self._check(deadline)

    def _read_exact(self, size, deadline=None):
        if size < 0 or size > self.MAX_FRAME:
            raise RPCError("invalid desktop IPC frame")
        data = bytearray()
        while len(data) < size:
            self._check(deadline)
            if deadline is not None:
                self.socket.settimeout(min(.25, max(.001, deadline - time.monotonic())))
            try:
                chunk = self.socket.recv(min(size - len(data), 65536))
            except socket.timeout:
                if deadline is None:
                    raise
                continue
            if not chunk:
                raise ConnectionError("desktop IPC closed")
            data.extend(chunk)
        self._check(deadline)
        return data

    def _frame(self, deadline):
        size = struct.unpack("<I", self._read_exact(4, deadline))[0]
        data = self._read_exact(size, deadline)
        self._check(deadline)
        response = json.loads(data)
        self._check(deadline)
        if not isinstance(response, dict):
            raise RPCError("invalid desktop IPC object")
        return response

    def request(self, method, params, version, target=None, timeout=12):
        identifier = str(uuid.uuid4())
        message = {"type": "request", "requestId": identifier, "sourceClientId": self.client,
                   "version": version, "method": method, "params": params, "timeoutMs": int(timeout * 1000)}
        if target: message["targetClientId"] = target
        deadline = time.monotonic() + timeout
        self._write(message, deadline)
        while time.monotonic() < deadline:
            response = self._frame(deadline)
            if response.get("type") == "client-discovery-request":
                self._write({"type": "client-discovery-response", "requestId": response["requestId"],
                             "result": {"canHandle": False}}, deadline)
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
        deadline = time.monotonic() + 12
        self._write({"type": "broadcast", "sourceClientId": self.client,
            "method": "thread-stream-following-changed", "version": 1,
            "params": {"conversationId": thread_id, "hostId": "local", "following": True},
            "targetClientIds": [owner]}, deadline)
        while time.monotonic() < deadline:
            message = self._frame(deadline)
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
