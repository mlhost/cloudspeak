
class AzureCredentials:
    """
    AzureCredentials class provides a convenient way to manage Azure credentials, specifically for storage services.

    Attributes:
        _connection_string_storage (str): The connection string for Azure Blob Storage.

    Properties:
        connection_string_storage (str): Property to get or set the connection string for Azure Blob Storage.

    Raises:
        KeyError: Raised when attempting to access `connection_string_storage` property without a valid connection string.

    Usage:
        credentials = AzureCredentials()
        credentials.connection_string_storage = "YourConnectionString"
        storage_connection_string = credentials.connection_string_storage
    """

    def __int__(self, connection_string_storage=None):
        """
        Initializes an instance of AzureCredentials.
        """
        self._connection_string_storage = connection_string_storage

    @property
    def connection_string_storage(self):
        """
        Property to get or set the connection string for Azure Blob Storage.

        Raises:
            KeyError: Raised when attempting to access this property without a valid connection string.

        Returns:
            str: The connection string for Azure Blob Storage.
        """
        if self._connection_string_storage is None:
            raise KeyError("A connection string to the storage is required by this service! Configure a blob storage service, take note of the connection string and assign it to an instance of this class.")

        return self._connection_string_storage

    @connection_string_storage.setter
    def connection_string_storage(self, value):
        """
        Setter method for the connection_string_storage property.

        Args:
            value (str): The connection string for Azure Blob Storage.

        Returns:
            None
        """
        self._connection_string_storage = value
