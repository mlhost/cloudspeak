import io

import joblib


class JoblibSerializer:
    def __init__(self, algorithm="lz4", level=1, protocol=4):
        self._algorithm = algorithm
        self._level = level
        self._protocol = protocol

    def serialize(self, data):
        with io.BytesIO() as b:
            joblib.dump(data, b, compress=(self._algorithm, self._level), protocol=self._protocol)
            b.seek(0)
            data_bytes = b.read()
        return data_bytes

    def deserialize(self, data_bytes):
        if len(data_bytes) == 0:
            return None

        with io.BytesIO(data_bytes) as b:
            data = joblib.load(b)

        return data

    def __str__(self):
        return f"[Joblib serializer ({self._algorithm} with level {self._level} and protocol {self._protocol})"

    def __repr__(self):
        return str(self)

    @property
    def fqdn(self):
        c = self.__class__
        return f"{c.__module__}.{c.__name__}-protocol_{self._protocol}-algorithm_{self._algorithm}-level_{self._level}"
