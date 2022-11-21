from azure.core import MatchConditions
from azure.core.exceptions import ResourceExistsError

from cloudspeak.api.azure.file import AzureFile
from cloudspeak.api.interface.container import Container
from cloudspeak.config import get_config
from cloudspeak.utils.time import now

import weakref
import time


class AzureContainer(Container):
    def __init__(self, service, container_name):
        super().__init__(service)
        self._container_name = container_name
        service_raw = service.service_raw
        self._client = service_raw.get_container_client(container_name)
        self._file_weakref_cache = weakref.WeakValueDictionary()

    @property
    def cached_files(self):
        return len(self._file_weakref_cache)

    @property
    def leases(self):
        return self.service.leases

    @property
    def container_raw(self):
        """
        Retrieves the original Azure Container client.
        """
        return self._client

    def create(self, exists_ok=True):
        try:
            self._client.create_container()

        except ResourceExistsError as e:
            if not exists_ok:
                raise e from None

    @property
    def url(self):
        return self._client.url

    @property
    def id(self):
        return f"{id(self)}@{self.url}"

    @property
    def name(self):
        return self._container_name

    @property
    def exists(self):
        return self._client.exists()

    @property
    def locked(self):
        return self.metadata.get('lease', {}).get('status', 'unlocked') == 'locked'

    @property
    def metadata(self):
        container_meta = self._client.get_container_properties()
        return container_meta

    def lock(self, duration_seconds=-1, wait_seconds=-1, poll_interval_seconds=0.5):
        """
        Locks the container in the backend for the specified amount of time.

        If the container is already locked, the process will be blocked for the given `timeout` seconds.

        :param duration_seconds:
            duration of the lock. Note that backend only supports 60 seconds as maximum lock time.
            However, this library supports higher values as a background process renew the locks
            until the desired lock time is reached.

        :param wait_seconds:
            number of seconds that the lock will block the thread in case it is already locked by other
            process, waiting for having it ready. Use the value -1 to block indefinitely.

        :param poll_interval_seconds:
            poll frequency for unlock query.

        :return:
            True if locked by this process. False otherwise.
        """
        # We don't support locking operations on nonexistent containers.
        if not self.exists:
            raise ResourceExistsError("The container does not exist. It must be created first.")

        start = time.time()

        lease = self.leases.get_lease(self.id)

        if lease is not None:
            self.leases.update_lease(self.id, lease_expire_seconds=duration_seconds)
            return True

        # The complex code of trying to acquire a lease... until we do
        while lease is None and (wait_seconds == -1 or (now() - start).total_seconds() < wait_seconds):

            try:
                lease = self._client.acquire_lease(lease_duration=60)

            except ResourceExistsError as e:
                # Raises when the container is already locked by other at the cloud backend.
                # We just have to wait for it to be released. Unfortunately we have to poll.
                # TODO: Exponential wait perhaps?
                time.sleep(poll_interval_seconds)

        if lease is None:
            raise TimeoutError("Could not retrieve a lock in time")

        self.leases.add_lease(self.id,
                              lease,
                              autorenew_seconds=30,
                              lease_expire_seconds=duration_seconds)

    def get_files(self, prefix, delimiter="/"):
        """
        Retrieves all the files by the given pattern.

        Note that folders are not fetched by this command, only files.
        To retrieve folder names, try `get_folder_names()`.

        :param prefix:
            Prefix that files should have to be fetched.

        :param delimiter:
            Delimiter that files should have to be fetched.

        """
        client = self._client

        for x in client.walk_blobs(name_starts_with=prefix, delimiter=delimiter):

            # We discard the special case: folder prefix. AzBlobStorage reports some prefix items for folders.
            if hasattr(x, 'prefix'):
                continue

            yield self[x['name']]

    def get_folder_names(self, prefix):
        """
        Retrieves all the folders by the given prefix.

        Note that only folder names are not fetched by this command.
        To retrieve files, try `get_files()`.

        :param prefix:
            Prefix that files should have to be fetched.

        """
        client = self._client
        delimiter = "/"

        for x in client.walk_blobs(name_starts_with=prefix, delimiter=delimiter):

            # We discard the special case: folder prefix. AzBlobStorage reports some prefix items for folders.
            if not hasattr(x, 'prefix'):
                continue

            yield x['name']

    def query(self, tags_filter):
        """
        Queries for files that matches the given tags filter. Example:

        :param tags_filter:
            A tag query string like follows:
                ->  "\"yourtagname\"='firsttag' and \"yourtagname2\"='secondtag'"

        :return:
            Iterator for files that matches the pattern.
        """
        config = get_config()
        for blob in self._client.find_blobs_by_tags(tags_filter,
                                                    results_per_page=config['blob.query.results_per_page']):
            yield self[blob['name']]

    def unlock(self):
        """
        Unlocks this container if was locked
        """
        lease = self.leases.get_lease(self.id)

        if lease is None:
            return

        self.leases.remove_lease(lease)

    def delete(self):
        self._client.delete_container()

    def __getitem__(self, filename):
        return self.get_file(filename)

    def get_file(self, name, snapshot_id=None):
        file = self._file_weakref_cache.get(name)

        if file is None:
            file = AzureFile(self, name, snapshot_id=snapshot_id)
            self._file_weakref_cache[name] = file

        return file

    def delete_many_files(self, files_list, changed_ok=False):
        """
        Attempts to remove a list of blobs.

        :param files_list:
            List of files to remove from the container.

        :param changed_ok:
            Boolean flag to determine if an error should be raised in case the blob changed in the backend before
            deleting.
        """

        blobs = [f.to_dict() for f in files_list]
        include_snapshots = True

        # We append the MATCH condition (in case) or remove the etag
        for b in blobs:
            if changed_ok and 'etag' in b:
                del b['etag']
            else:
                b['match_condition'] = MatchConditions.IfNotModified

            # If a single blob of the list is a snapshot, we can't pass include_snapshots to the backend.
            if 'snapshot' in b:
                include_snapshots = False

        kwargs = {}

        if include_snapshots:
            kwargs['delete_snapshots'] = 'include'

        self._client.delete_blobs(*blobs, **kwargs)

    def __str__(self):
        return f"[AzureBlobStorage Container; Name: '{self.name}']"

    def __repr__(self):
        return str(self)
