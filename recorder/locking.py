"""A single collector owns each capture database; read-only reports remain free."""
import fcntl
import json
import os
from pathlib import Path


class CaptureAlreadyRunning(RuntimeError):
    pass


class CaptureLock:
    def __init__(self, database):
        path = Path(str(database)+".capture.lock")
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.ftruncate(self.fd, 0)
            os.write(self.fd, json.dumps({"pid": os.getpid(), "database": str(Path(database).resolve())}).encode())
        except BlockingIOError:
            os.close(self.fd)
            self.fd = None
            raise CaptureAlreadyRunning("A collector already owns this database") from None

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
