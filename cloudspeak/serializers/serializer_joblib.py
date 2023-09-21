import io

import joblib


class JoblibSerializer:
    """
    JoblibSerializer provides serialization and deserialization of data using the Joblib library.

    Args:
        algorithm (str, optional): The compression algorithm to use (default is "lz4").
        level (int, optional): The compression level (1 to 9) to use with lz4 compression (default is 1).
        protocol (int, optional): The pickling protocol to use (default is 4).

    Methods:
    """
    def __init__(self, algorithm="lz4", level=1, protocol=4):
        """
        Initializes an instance of JoblibSerializer with the provided configuration.

        Args:
            algorithm (str, optional): The compression algorithm to use (default is "lz4").
            level (int, optional): The compression level (1 to 9) to use with lz4 compression (default is 1).
            protocol (int, optional): The pickling protocol to use (default is 4).

        Returns:
            None
        """
        self._algorithm = algorithm
        self._level = level
        self._protocol = protocol

    def serialize(self, data):
        """
        Serialize data using Joblib.

        Args:
            data: The data to be serialized.

        Returns:
            bytes: The serialized data as bytes.

        Raises:
            None
        """
        with io.BytesIO() as b:
            joblib.dump(data, b, compress=(self._algorithm, self._level), protocol=self._protocol)
            b.seek(0)
            data_bytes = b.read()
        return data_bytes

    def deserialize(self, data_bytes):
        """
        Deserialize data from bytes using Joblib.

        Args:
            data_bytes (bytes): The serialized data as bytes.

        Returns:
            Any: The deserialized data.

        Raises:
            None
        """
        if len(data_bytes) == 0:
            return None

        with io.BytesIO(data_bytes) as b:
            data = joblib.load(b)

        return data

    def __str__(self):
        """
        Returns a string representation of the JoblibSerializer.

        Returns:
            str: A string describing the JoblibSerializer configuration.

        Raises:
            None
        """
        return f"[Joblib serializer ({self._algorithm} with level {self._level} and protocol {self._protocol})"

    def __repr__(self):
        """
        Returns a string representation of the JoblibSerializer.

        Returns:
            str: A string describing the JoblibSerializer configuration.

        Raises:
            None
        """
        return str(self)

    @property
    def fqdn(self):
        """
        Property to get the fully qualified class name of the JoblibSerializer instance.

        Returns:
            str: The fully qualified class name.

        Raises:
            None
        """
        c = self.__class__
        return f"{c.__module__}.{c.__name__}-protocol_{self._protocol}-algorithm_{self._algorithm}-level_{self._level}"
