from azure.core import MatchConditions
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import PartialBatchErrorException

from cloudspeak.storage.azure.file import AzureFile
from cloudspeak.storage.interface.container import Container
from cloudspeak.config import get_config
from cloudspeak.utils.time import now

import weakref
import time


class AzureContainer(Container):
    def __init__(self, service, container_name, context=None):
        """
        Constructor of the container.

        :param service:
            Service object that owns this container.

        :param container_name:
            Name of the container. Must be alphanumeric and lower-case, without special characters.

        :param context:
            The context level of the lock. Possible contexts:
                - "instance"  -> Lock at instance level. Other instances can't get lease of this lock in the app.
                - "app"       -> Lock at app level. Other instances CAN get lease of this lock in the app.
                - custom      -> Lock at a custom level. This can be any string.

            If not set, will be inherited from the service owner.
        """
        super().__init__(service)
        self._container_name = container_name
        service_raw = service.service_raw
        self._client = service_raw.get_container_client(container_name)
        self._file_weakref_cache = weakref.WeakValueDictionary()
        self._context = context if context is not None else service.context

    @property
    def context(self):
        return self._context

    @context.setter
    def context(self, new_context):
        self._context = new_context

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

    def lock(self, duration_seconds=-1, wait_seconds=-1, poll_interval_seconds=0.5, context=None):
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

        :param context:
            The context level of the lock. Possible contexts:
                - "instance"  -> Lock at instance level. Other instances can't get lease of this lock in the app.
                - "app"       -> Lock at app level. Other instances CAN get lease of this lock in the app.
                - custom      -> Lock at a custom level. This can be any string.

            If not set, will be used the original File context (inherited from the service).

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
            file = AzureFile(self, name, snapshot_id=snapshot_id, context=self.context)
            self._file_weakref_cache[name] = file

        return file

    def delete_many_files(self, files_list, changed_ok=False, raise_errors=True, context=None):
        """
        Attempts to remove a list of blobs.
        In order to know which files were removed and which couldn't be removed, the result should be checked.

        Example:
            >>> container.delete_many_files([file1, file2], changed_ok=True, raise_errors=False)
            [<HttpClientTransportResponse: 202 Accepted>,
             <HttpClientTransportResponse: 404 The specified blob does not exist., Content-Type: application/xml>]

        :param files_list:
            List of files to remove from the container.

        :param changed_ok:
            Boolean flag to determine if an error should be raised in case the blob changed in the backend before
            deleting.

        :param raise_errors:
            Boolean flag to specify whether an error should be raised in case there was any issue.
            True to raise error. False otherwise.

            An error won't stop from removing the remaining files.

        :param context:
            The Leases context to retrieve leases for each file.

        :return:
            List of HttpClientTransportResponse objects.
            Every object provide information in the attributes `status_code` and `reason`.
            A `status_code` >= 200 and < 300 indicates success.
        """

        blobs = [f.to_dict(include_lease=True, context=context) for f in files_list]
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

        # We need to iterate over the results to tell the File objects that they have been effectively removed.
        operations_report = self._client.delete_blobs(*blobs, **kwargs, raise_on_any_failure=False)
        results = []
        errors = []

        for file_status, file in zip(operations_report, files_list):
            results.append(file_status)
            if 200 <= file_status.status_code < 300:
                file.reset_status()

                # If it has an active lease we need to remove it
                file.unlock(context=context)
            else:
                errors.append((file, file_status))

        if raise_errors and len(errors) > 0:
            raise PartialBatchErrorException(f"{len(errors)} errors in the backend", None, parts=errors)

        return results

    def __str__(self):
        return f"[AzureBlobStorage Container; Name: '{self.name}']"

    def __repr__(self):
        return str(self)
