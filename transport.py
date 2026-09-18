"""Codex transport adapters. No model/API credentials and no shell interpolation.

App Server is the public transport for dormant conversations. For a conversation
already owned by a desktop window, the local desktop follower protocol delegates
to that owner instead of creating a competing runtime. Neither adapter changes
approval policy, model provider, sandbox permissions or application binaries.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import queue
import shutil
import socket
import struct
import subprocess
import threading
import time
import uuid

def resolve_codex_binary():
    override = os.environ.get("CODEX_DISPATCH_CODEX_BIN")
    if override:
        return override
    discovered = shutil.which("codex")
    if discovered:
        return discovered
    desktop_binary = "/Applications/ChatGPT.app/Contents/Resources/codex"
    if Path(desktop_binary).is_file():
        return desktop_binary
    raise FileNotFoundError(
        "Codex executable not found. Set CODEX_DISPATCH_CODEX_BIN to its path."
    )


class RPCError(RuntimeError):
    pass


class AppServer:
    def __init__(self, cwd, stderr=None):
        self.lock = threading.Lock()
        self.pending = {}
        self.notifications = queue.Queue()
        self.next_id = 0
        self.closed = False
        self.process = subprocess.Popen([resolve_codex_binary(), "app-server", "--stdio"], cwd=str(cwd),
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=stderr or subprocess.DEVNULL, text=True, bufsize=1)
        threading.Thread(target=self._reader, daemon=True).start()
        self.call("initialize", {"clientInfo": {"name": "task_dispatch_hub", "version": "1.0.0"},
                                 "capabilities": {"experimentalApi": True}}, timeout=20)
        self.send({"method": "initialized"})

    def send(self, value):
        with self.lock:
            self.process.stdin.write(json.dumps(value, ensure_ascii=False) + "\n")
            self.process.stdin.flush()

    def _reader(self):
        try:
            for line in self.process.stdout:
                try: message = json.loads(line)
                except ValueError: continue
                if "id" in message and "method" not in message:
                    waiter = self.pending.get(message["id"])
                    if waiter: waiter.put(message)
                else:
                    self.notifications.put(message)
                    if "id" in message and "method" in message:
                        method = message["method"]
                        # No blanket approvals or user-input fabrication by the dispatcher.
                        if method in {"item/commandExecution/requestApproval", "item/fileChange/requestApproval"}:
                            self.send({"id": message["id"], "result": {"decision": "decline"}})
                        else:
                            self.send({"id": message["id"], "error": {"code": -32601,
                                "message": "Task dispatch hub cannot answer user approvals or questions. Record needs_user and ask the user in the conversation."}})
        finally:
            self.closed = True
            for waiter in list(self.pending.values()):
                waiter.put({"error": {"message": "app-server connection closed"}})

    def call(self, method, params, timeout=15):
        if self.closed: raise RPCError("app-server connection closed")
        self.next_id += 1
        identifier = self.next_id
        waiter = queue.Queue()
        self.pending[identifier] = waiter
        try:
            self.send({"id": identifier, "method": method, "params": params})
            try: reply = waiter.get(timeout=timeout)
            except queue.Empty: raise TimeoutError(method + " response unknown")
            if "error" in reply: raise RPCError(str(reply["error"].get("message", reply["error"])))
            return reply.get("result", {})
        finally:
            self.pending.pop(identifier, None)

    def latest_turn(self, thread_id):
        result = self.call("thread/turns/list", {"threadId": thread_id, "limit": 1,
                                               "sortDirection": "desc", "itemsView": "summary"})
        data = result.get("data", [])
        return data[0] if data else None

    def queue_list(self, thread_id):
        return self.call("thread/queue/list", {"threadId": thread_id, "limit": 10}).get("data", [])

    def resume(self, thread_id, compact_path):
        result = self.call("thread/resume", {"threadId": thread_id, "excludeTurns": True,
            "config": {"experimental_compact_prompt_file": str(compact_path)}}, timeout=40)
        if result.get("modelProvider") != "openai":
            raise RPCError("当前任务未使用授权的 Codex OpenAI provider，中枢拒绝产生外部模型调用")
        if result.get("thread", {}).get("id") != thread_id:
            raise RPCError("恢复的任务身份与目标不一致")
        return result

    def start(self, thread_id, text, message_id):
        return self.call("turn/start", {"threadId": thread_id,
            "clientUserMessageId": message_id,
            "input": [{"type": "text", "text": text, "text_elements": []}]}, timeout=30)

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try: self.process.wait(timeout=5)
            except subprocess.TimeoutExpired: self.process.kill()


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

    def close(self):
        self.socket.close()
