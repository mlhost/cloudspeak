from concurrent.futures import ThreadPoolExecutor

from azure.storage.blob import BlobServiceClient

from cloudspeak.api.azure.container import AzureContainer
from cloudspeak.api.azure.leases.leases_handler import LeasesHandler
from cloudspeak.api.azure.progress import ProgressMultiple
from cloudspeak.api.interface.service import Service as Service


class AzureService(Service):

    def __init__(self, connection_string, operations_pool=16, serializer=None):
        self._serializer = serializer
        self._blob_service = BlobServiceClient.from_connection_string(connection_string)
        self._leases_handler = LeasesHandler()
        self._progresses = ProgressMultiple(self)
        self._operations_pool = ThreadPoolExecutor(operations_pool)

    @property
    def serializer(self):
        return self._serializer

    @property
    def pool(self):
        return self._operations_pool

    @property
    def service_raw(self):
        """
        Retrieves the original Azure Service client.
        """
        return self._blob_service

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
    def account_name(self):
        return self._blob_service.account_name

    def __getitem__(self, element):
        if type(element) is int:
            result = self.list()[element]
        elif type(element) is str:
            result = AzureContainer(self, element)
        else:
            raise KeyError("Type of container not understood. Try an index or the name of the container (as a string).")

        return result

    def __iter__(self):
        for container_meta in self._blob_service.list_containers():
            yield self[container_meta['name']]

    def __len__(self):
        return len(self.list())

    def __str__(self):
        return f"[Azure blob storage ({self.account_name}); Num. containers: {len(self)}; serializer: {self.serializer}]"

    def __repr__(self):
        return str(self)

    def list(self):
        return [x for x in self]
