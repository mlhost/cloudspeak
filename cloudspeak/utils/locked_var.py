from threading import Lock


class LockedVar:
    def __init__(self, default_v=None):
        self._var = default_v
        self._lock = Lock()

    def set_value(self, v):
        with self._lock:
            self._var = v

    def get_value(self):
        with self._lock:
            return self._var
