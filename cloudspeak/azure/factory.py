from azure.core.exceptions import ResourceExistsError

from cloudspeak.azure.remote_dict import AzureDictionary
from cloudspeak.serializers import JoblibSerializer
from cloudspeak.storage.azure import AzureService
from cloudspeak.storage.azure.queue.queue import AzureQueue


class AzureFactory:
    """
    AzureFactory provides a flexible factory for creating various Azure-related data structures and services.

    Args:
        credentials (AzureCredentials): An instance of AzureCredentials containing necessary connection information.
        singleton (bool, optional): If True, a single instance of AzureService is shared among all instances of
            AzureFactory. If False, a new AzureService instance is created for each factory instance.
        operations_pool (int, optional): The maximum number of concurrent operations for AzureService.
        serializer (Serializer, optional): The serializer to use for data serialization and deserialization.
        context (Any, optional): Additional context data to be passed to AzureService.

    Methods:
        dictionary(container, folder_name, indexed=True, create_container=True, context=None):
            Generates a remote dictionary based on the specified Azure Blob Storage container and folder.

        queue(queue_name, create_queue=True):
            Retrieves or creates an Azure Queue Storage queue with the specified name.

        service_storage:
            Property to get the AzureService instance for blob storage operations.

        custom_service_storage(operations_pool=None, serializer=None, context=None, singleton=None):
            Creates a custom AzureService instance with optional configuration overrides.

    Attributes:
        _credentials (AzureCredentials): The AzureCredentials instance containing connection information.
        _service_storage (AzureService, optional): The singleton AzureService instance for blob storage operations.
        _singleton (bool): Indicates whether a single AzureService instance is shared among all factory instances.
        _serializer (Serializer): The serializer used for data serialization and deserialization.
        _context (Any): Additional context data passed to AzureService.
        _operations_pool (int): The maximum number of concurrent operations for AzureService.

    Raises:
        None.

    Usage:
        credentials = AzureCredentials(connection_string_storage="YourConnectionString")
        factory = AzureFactory(credentials, operations_pool=16)

        # Create a remote dictionary
        remote_dict = factory.dictionary(container="mycontainer", folder_name="myfolder")

        # Get or create an Azure Queue Storage queue
        azure_queue = factory.queue(queue_name="myqueue")

        # Access the Azure Blob Storage service
        blob_service = factory.service_storage

        # Create a custom AzureService instance
        custom_service = factory.custom_service_storage(operations_pool=32, serializer=MyCustomSerializer())
    """
    def __init__(self, credentials, singleton=True, operations_pool=16, serializer=JoblibSerializer(), context=None):
        """
        Initializes an instance of AzureFactory with the provided configuration.

        Args:
            credentials (AzureCredentials): An instance of AzureCredentials containing necessary connection information.
            singleton (bool, optional): If True, a single instance of AzureService is shared among all instances of
                AzureFactory. If False, a new AzureService instance is created for each factory instance.
            operations_pool (int, optional): The maximum number of concurrent operations for AzureService.
            serializer (Serializer, optional): The serializer to use for data serialization and deserialization.
            context (Any, optional): Additional context data to be passed to AzureService.

        Returns:
            None
        """
        self._credentials = credentials
        self._service_storage = None
        self._singleton = singleton
        self._serializer = serializer
        self._context = context
        self._operations_pool = operations_pool

    def dictionary(self, container_name, folder_name, indexed=True, create_container=True, context=None) -> AzureDictionary:
        """
        Generates a remote dictionary based on the specified Azure Blob Storage container and folder.

        Args:
            container_name (str or ContainerClient): The name of the Azure Blob Storage container or a Container instance retrieved from the ServiceStorage.
            folder_name (str): The name of the folder within the container where data will be stored.
            indexed (bool, optional): If True, an index is automatically maintained for faster access times (default).
                If False, indexing is disabled (faster writes/reads but no list of keys).
            create_container (bool, optional): If True, the container is created if it doesn't exist.
            context (Any, optional): Additional context data to be passed to AzureDictionary.

        Returns:
            AzureDictionary: An AzureDictionary instance representing the remote dictionary.

        Raises:
            None
        """
        context = context or self._context

        service_storage = self.service_storage

        if type(container_name) is str:
            container_name = service_storage.containers[container_name]

        if create_container:
            container_name.create(exists_ok=True)

        return AzureDictionary(container=container_name, folder_name=folder_name, indexed=indexed, context=context)

    def queue(self, queue_name, create=True) -> AzureQueue:
        """
        Retrieves or creates an Azure Queue Storage queue with the specified name.

        Args:
            queue_name (str): The name of the Azure Queue Storage queue.
            create (bool, optional): If True, the queue is created if it doesn't exist.

        Returns:
            AzureQueue: An AzureQueue instance representing the Azure Queue Storage queue.

        Raises:
            None
        """
        service_storage = self.service_storage

        queue = service_storage.queues[queue_name]

        if create:
            try:
                queue.create()
            except ResourceExistsError:
                pass

        return queue

    @property
    def service_storage(self) -> AzureService:
        """
        Property to get the AzureService instance for blob storage operations.

        Returns:
            AzureService: The AzureService instance.

        Raises:
            None
        """
        return self.custom_service_storage()

    def custom_service_storage(self, operations_pool=None, serializer=None, context=None, singleton=None):
        """
        Creates a custom AzureService instance with optional configuration overrides.

        Args:
            operations_pool (int, optional): The maximum number of concurrent operations for AzureService.
            serializer (Serializer, optional): The serializer to use for data serialization and deserialization.
            context (Any, optional): Additional context data to be passed to AzureService.
            singleton (bool, optional): If True, a single instance of AzureService is shared among all instances of
                AzureFactory. If False, a new AzureService instance is created for each factory instance.

        Returns:
            AzureService: The AzureService instance with custom configuration.

        Raises:
            None
        """
        credentials = self._credentials

        operations_pool = operations_pool if operations_pool is not None else self._operations_pool
        context = context if context is not None else self._context
        serializer = serializer if serializer is not None else self._serializer
        singleton = singleton if singleton is not None else self._singleton

        if singleton:

            if self._service_storage is None:
                service = AzureService(credentials.connection_string_storage, operations_pool=operations_pool, serializer=serializer,
                                       context=context)
                self._service_storage = service

            else:
                service = self._service_storage

        else:
            service = AzureService(credentials.connection_string_storage, operations_pool=operations_pool, serializer=serializer,
                                   context=context)

        return service

    def __str__(self):
        """
        Returns a string representation of the AzureFactory.

        Returns:
            str: A string describing the AzureFactory and available data structures.

        Raises:
            None
        """
        return "Azure factory. Check methods to know which data structures are available."

    def __repr__(self):
        """
        Returns a string representation of the AzureFactory.

        Returns:
            str: A string describing the AzureFactory and available data structures.

        Raises:
            None
        """
        return str(self)
