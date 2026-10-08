"""Same-user worker control. Never signals a persisted/reused PID."""
import json
import os
from pathlib import Path
import socket
import threading
import time


def read_line(connection, limit, deadline):
    """Bound a whole newline-terminated frame, including fragmented streams."""
    data = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('control frame deadline exceeded')
        connection.settimeout(remaining)
        chunk = connection.recv(min(256, limit + 1 - len(data)))
        if not chunk:
            raise ConnectionError('incomplete control frame')
        data.extend(chunk)
        if len(data) > limit:
            raise ValueError('control frame too large')
        if b'\n' in data:
            if not data.endswith(b'\n') or data.count(b'\n') != 1:
                raise ValueError('unexpected trailing control data')
            return bytes(data[:-1])


class ControlEndpoint:
    def __init__(self, path, stop):
        self.path = Path(path)
        self.stop = stop
        self.closed = threading.Event()
        self.socket = socket.socket(socket.AF_UNIX)

    def start(self):
        # Caller holds worker.lock, so any old socket has no live hub owner.
        self.path.unlink(missing_ok=True)
        self.socket.bind(str(self.path))
        os.chmod(self.path, 0o600)
        self.socket.listen(1)
        self.socket.settimeout(.25)
        self.thread = threading.Thread(target=self.serve, daemon=True, name='hub-control')
        self.thread.start()

    def serve(self):
        while not self.closed.is_set():
            try:
                connection, _ = self.socket.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with connection:
                try:
                    deadline = time.monotonic() + .5
                    if read_line(connection, 16, deadline) == b'stop':
                        self.stop()
                        connection.settimeout(max(.001, deadline-time.monotonic()))
                        connection.sendall(json.dumps({'stop_requested': True, 'pid': os.getpid()}).encode()+b'\n')
                except (OSError, ValueError):
                    pass

    def close(self):
        self.closed.set()
        self.socket.close()
        self.thread.join(timeout=1)
        self.path.unlink(missing_ok=True)


def request_stop(path):
    with socket.socket(socket.AF_UNIX) as connection:
        deadline = time.monotonic() + 2
        connection.settimeout(2)
        connection.connect(str(path))
        connection.sendall(b'stop\n')
        connection.settimeout(max(.001, deadline-time.monotonic()))
        response = json.loads(read_line(connection, 1024, deadline))
        if not isinstance(response, dict) or response.get('stop_requested') is not True:
            raise RuntimeError('worker did not confirm stop request')
        return response
