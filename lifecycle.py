"""Same-user worker control. Never signals a persisted/reused PID."""
import json
import os
from pathlib import Path
import socket
import threading


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
                connection.settimeout(.25)
                try:
                    if connection.recv(16) == b'stop\n':
                        self.stop()
                        connection.sendall(json.dumps({'stop_requested': True, 'pid': os.getpid()}).encode())
                except OSError:
                    pass

    def close(self):
        self.closed.set()
        self.socket.close()
        self.thread.join(timeout=1)
        self.path.unlink(missing_ok=True)


def request_stop(path):
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(2)
        connection.connect(str(path))
        connection.sendall(b'stop\n')
        response = json.loads(connection.recv(1024))
        if response.get('stop_requested') is not True:
            raise RuntimeError('worker did not confirm stop request')
        return response
