from concurrent.futures import ThreadPoolExecutor

from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueServiceClient

from cloudspeak.storage.azure.blob.container import AzureContainer
from cloudspeak.storage.azure.blob.leases.leases_handler import LeasesHandler
from cloudspeak.storage.azure.blob.progress import ProgressMultiple
from cloudspeak.storage.azure.queue.queue import AzureQueue
from cloudspeak.storage.azure.queue.updates.updates_handler import UpdatesHandler
from cloudspeak.storage.interface.service import Service as Service


class AzureService(Service):

    def __init__(self, connection_string, operations_pool=16, serializer=None, context=None):
        self._serializer = serializer
        self._context = context
        
        self._blob_service = BlobServiceClient.from_connection_string(connection_string)
        self._queue_service = QueueServiceClient.from_connection_string(connection_string)

        self._leases_handler = LeasesHandler()
        self._updates_handler = UpdatesHandler()

        self._progresses = ProgressMultiple(self)
        self._operations_pool = ThreadPoolExecutor(operations_pool)
        self._containers_handler = AzureContainersHandler(self)
        self._queues_handler = AzureQueuesHandler(self)


    @property
    def context(self):
        return self._context

    @context.setter
    def context(self, new_context):
        self._context = new_context

    @property
    def serializer(self):
        return self._serializer

    @property
    def containers(self):
        return self._containers_handler

    @property
    def queues(self):
        return self._queues_handler

    @property
    def pool(self):
        return self._operations_pool

    @property
    def service_raw_blob(self):
        """
        Retrieves the original Azure Service client.
        """
        return self._blob_service

    @property
    def service_raw_queue(self):
        """
        Retrieves the original Azure Service client.
        """
        return self._queue_service

    @property
    def progresses(self):
        return self._progresses

    @property
    def url(self):
        return self._blob_service.url

    @property
    def leases(self):
        return self._leases_handler

    @property
    def updates(self):
        return self._updates_handler

    @property
    def account_name(self):
        return self._blob_service.account_name

    def __str__(self):
        return f"[Azure blob storage ({self.account_name}); serializer: {self.serializer}]"


class AzureContainersHandler:

    def __init__(self, owner):
        self._owner = owner

    @property
    def owner(self):
        return self._owner

    def __getitem__(self, element):
        if type(element) is int:
            result = self.list()[element]
        elif type(element) is str:
            result = AzureContainer(self.owner, element, context=self.owner.context)
        else:
            raise KeyError("Type of container not understood. Try an index or the name of the container (as a string).")

        return result

    def __iter__(self):
        for container_meta in self.owner._blob_service.list_containers():
            yield self[container_meta['name']]

    def __len__(self):
        return len(self.list())

    def __str__(self):
        return f"[Azure blob storage ({self.owner.account_name}); Num. containers: {len(self)}; serializer: {self.owner.serializer}]"

    def __repr__(self):
        return str(self)

    def list(self):
        return [x for x in self]


class AzureQueuesHandler:

    def __init__(self, owner):
        self._owner = owner

    @property
    def owner(self):
        return self._owner

    def __getitem__(self, element):
        if type(element) is int:
            result = self.list()[element]
        elif type(element) is str:
            result = AzureQueue(self.owner, element)
        else:
            raise KeyError("Type of queue not understood. Try an index or the name of the queue (as a string).")

        return result

    def __iter__(self):
        for queue in self.owner.service_raw_queue.list_queues():
            yield queue.name
            
    def __len__(self):
        return len(self.list())

    def __str__(self):
        return f"[Azure queue storage ({self.owner.account_name}); Num. queues: {len(self)}; serializer: {self.owner.serializer}]"

    def __repr__(self):
        return str(self)

    def list(self):
        return [x for x in self]
