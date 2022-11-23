import time
import weakref
from threading import Lock

from azure.core import MatchConditions
from azure.core.exceptions import ResourceExistsError, ResourceModifiedError, ResourceNotFoundError

from cloudspeak.storage.azure.progress import ProgressSingle
from cloudspeak.storage.azure.snapshots import Snapshots
from cloudspeak.storage.interface.file import File
from cloudspeak.config import get_config
from cloudspeak.utils.format import size_to_human
from cloudspeak.utils.basics import len_nan
from cloudspeak.utils.time import to_datetime


class AzureFile(File):
    def __init__(self, container, name, snapshot_id=None,
                 max_concurrency_download=None, max_concurrency_upload=None,
                 context=None):
        """
        Instances a new Azure File.

        :param container:
            Container object owner of this File instance.

        :param name:
            Name of the file (blob name).

        :param snapshot_id:
            ID of the snapshot that represents this file.

        :param max_concurrency_download:
            Maximum number of paralell downloads that will be used per blob.
            If not set, retrieved from config parameter 'blob.max_concurrency_download'.

        :param max_concurrency_upload:
            Maximum number of paralell uploads that will be used per blob.
            If not set, retrieved from config parameter 'blob.max_concurrency_upload'.

        :param context:
            The context level of the lock. Possible contexts:
                - "instance"  -> Lock at instance level. Other instances can't get lease of this lock in the app.
                - "app"       -> Lock at app level. Other instances CAN get lease of this lock in the app.
                - custom      -> Lock at a custom level. This can be any string.

            If not set, will be inherited from the container.
        """
        super().__init__(container)

        self._name = name
        self._assigned = False
        self._snapshot_id = snapshot_id

        self._progresses_lock = Lock()
        self._progresses = weakref.WeakValueDictionary()

        self._data = None
        self._etag = None
        self._md5sum = None

        self._max_concurrency_download = max_concurrency_download
        self._max_concurrency_upload = max_concurrency_upload

        self._context = context if context is not None else container.context

        container_raw = container.container_raw
        self._client = container_raw.get_blob_client(name, snapshot=snapshot_id)

    @property
    def context(self):
        return self._context

    @context.setter
    def context(self, new_context):
        self._context = new_context

    @property
    def leases(self):
        return self.container.leases

    def get_current_lease(self, context=None):
        """
        Retrieves the lease for the current blob if any active and known by this process.
        """
        context = context if context is not None else self._context
        lock_id = self._get_lock_id(context)
        return self.leases.get_lease(lock_id)

    @property
    def max_concurrency_download(self):
        config = get_config()
        return self._max_concurrency_download if self._max_concurrency_download is not None \
            else config.get('blob.max_concurrency_download')

    @property
    def max_concurrency_upload(self):
        config = get_config()
        return self._max_concurrency_upload if self._max_concurrency_upload is not None \
            else config.get('blob.max_concurrency_upload')

    @property
    def snapshot_id(self):
        """
        If this blob is a snapshot, a value identifying the snapshot will be outputted here
        """
        return self._snapshot_id

    @property
    def file_raw(self):
        """
        Retrieves the original Azure Blob client
        """
        return self._client

    def _upload(self, progress, overwrite=False, allow_changed=False, tags=None, context=None):
        context = context if context is not None else self._context

        if tags is None:
            tags = {}

        # The serializer is added as tag
        serializer = self.service.serializer

        if serializer is not None:
            tags['serializer'] = serializer.fqdn

        kwargs = {}

        if not allow_changed and self._etag is not None:
            kwargs['etag'] = self._etag
            kwargs['match_condition'] = MatchConditions.IfNotModified

        lease = self.get_current_lease(context=context)

        if lease is not None:
            kwargs['lease'] = lease

        try:
            if not overwrite and self.exists:
                raise ResourceExistsError("Resource already exists in backend.")

            try:
                modified = self.modified

            except ResourceNotFoundError:
                modified = True

            if not modified:
                raise ValueError("Resource already stored without changes.")

            result = self._client.upload_blob(self._data,
                                              overwrite=overwrite,
                                              max_concurrency=self.max_concurrency_upload,
                                              progress_hook=progress.tick_update,
                                              tags=tags,
                                              **kwargs)

            self._etag = result.get('etag')
            self._md5sum = result.get('content_md5')
            self._assigned = False

        except ValueError:
            pass

        finally:
            if self._data is None:
                total_progress = -1

            else:
                total_progress = len_nan(self._data, none_len=0)

            progress.tick_update(total_progress, total_progress)

    def _download(self, progress, offset=None, length=None, chunk_size=1024*1024*100):
        kwargs = {}

        if self._etag is not None:
            kwargs['etag'] = self._etag
            kwargs['match_condition'] = MatchConditions.IfModified

        try:
            chunks = []
            chunk = None

            try:
                stream = self._client.download_blob(offset=offset,
                                                    length=length,
                                                    max_concurrency=self.max_concurrency_download,
                                                    progress_hook=progress.tick_update,
                                                    **kwargs)

                while chunk is None or len(chunk) == chunk_size:
                    chunk = stream.read(chunk_size)
                    chunks.append(chunk)

                self._data = b"".join(chunks)

            except ResourceNotFoundError as e:
                if self._etag is not None:
                    # The item was deleted
                    self.reset_status()

                else:
                    # Otherwise it is an unknown element.
                    raise e

            finally:
                total_progress = len_nan(self._data, none_len=-1)
                progress.tick_update(total_progress, total_progress)

            metadata = self.metadata

            self._etag = metadata.get('etag')
            self._md5sum = metadata.get('content_settings', {}).get('content_md5')
            self._assigned = False

        except ResourceModifiedError:
            total_progress = len(self._data) if self._data is not None else 0
            progress.tick_update(total_progress, total_progress)

    def upload(self, overwrite=False, allow_changed=False, tags=None, context=None):
        """
        Uploads the internal `data` to the blob storage.
        This is an async method.

        A progress object is returned, which can be used to track the operation progress.
        To catch exceptions and/or block the thread, join the progress object `progress.join()`.

        :param overwrite:
            True to overwrite the backend content in case there is already data with the same blob name.
            False to raise an Exception if data found in backend with the same name.

        :param allow_changed:
            True to replace data (if `overwrite` is also True) even if other process updated it before we knew.
            False to raise an exception.

        :param tags:
            Dictionary containing k:v for tags to store in the file.
            Note that tags are useful for making queries in container-level.

        :returns:
            A Progress object that can be used to track the operation progress.
        """
        context = context if context is not None else self._context

        if self._data is None:
            raise ValueError("No data to upload. Either download current version or set new data as binary.")

        with self._progresses_lock:
            progress = self._progresses.get(f"upload_{self._etag}")

            if progress is not None and not progress.finished:
                return progress

            progress = ProgressSingle(self, operation_type="upload")
            promise = self.service.pool.submit(self._upload,
                                               progress=progress,
                                               overwrite=overwrite,
                                               allow_changed=allow_changed,
                                               context=context)
            progress.set_promise(promise)

            self._progresses[f"upload_{self._etag}"] = progress

        return progress

    def download(self, offset=None, length=None, chunk_size=1024*1024*100):
        """
        Downloads the blob storage data into the internal `data`.
        This is an async method.

        A progress object is returned, which can be used to track the operation progress.
        To catch exceptions and/or block the thread, join the progress object `progress.join()`.

        :param offset:
            Byte offset to begin download from.

        :param length:
            Size of the data to download.

        :param chunk_size:
            Chunk size in case of internal chunkizing due to large size.

        :returns:
            A Progress object that can be used to track the operation progress.
            NOTE: The data is stored in `file.data` after this progress finishes.
        """
        with self._progresses_lock:
            progress = self._progresses.get(f"download_{self._etag}")

            if progress is not None and not progress.finished:
                return progress

            progress = ProgressSingle(self, operation_type="download")

            promise = self.service.pool.submit(self._download,
                                               progress=progress,
                                               offset=offset,
                                               chunk_size=chunk_size,
                                               length=length)
            progress.set_promise(promise)

        self._progresses[f"download_{self._etag}"] = progress

        return progress

    @property
    def url(self):
        """
        URL Representation of the blob in the backend Azure Blob Storage.
        """
        return self._client.url

    @property
    def name(self):
        """
        Name of the blob in the blob storage.
        """
        return self._name

    @property
    def id(self):
        """
        Unique ID file-wide.

        Every instance have a different id, even though they are all pointing to the same URL (and snapshot).
        """
        return f"{id(self)}@{self.url}:{self.snapshot_id}"

    @property
    def app_id(self):
        """
        Unique ID application-wide.

        All the instances pointing to the same URL (and snapshot) have the same app ID.
        """
        return f"{self.url}:{self.snapshot_id}"

    @property
    def data(self):
        serializer = self.service.serializer
        data = serializer.deserialize(self._data) if serializer is not None else self._data
        return data

    @data.setter
    def data(self, new_data):
        serializer = self.service.serializer
        self._assigned = True
        self._md5sum = None
        self._data = serializer.serialize(new_data) if serializer is not None else new_data

    @property
    def locked(self):
        """
        Checks whether this blob is locked in the backend by another process or not.
        NOTE: This method is expensive, as requires network communication each query.
        """
        return self.metadata.get('lease', {'status': 'unlocked'}).get('status') == 'locked'

    def create(self):
        """
        Creates the file in the cloud backend storage.

        If the file already exists, nothing is done.
        """
        try:
            self._data = b"" if self._data is None else self._data
            self.upload(overwrite=True).join(tqdm_bar=False)

        except ResourceExistsError:
            pass

    def _get_lock_id(self, context):
        """
        Retrieves the lock ID (for handling leases).
        """
        if context == "instance":
            lock_id = self.id
        elif context == "app":
            lock_id = self.app_id
        else:
            lock_id = f"{context}_{self.app_id}"

        return lock_id

    def lock(self, duration_seconds=-1, wait_seconds=-1, poll_interval_seconds=0.5, changed_ok=False,
             context=None, autocreate=False):
        """
        Locks the file in the backend for the specified amount of time.

        If the file is already locked, the process will be blocked for the given `timeout` seconds.

        :param duration_seconds:
            duration of the lock. Note that backend only supports 60 seconds as maximum lock time.
            However, this library supports higher values as a background process renew the locks
            until the desired lock time is reached.

        :param wait_seconds:
            number of seconds that the lock will block the thread in case it is already locked by other
            process, waiting for having it ready. Use the value -1 to block indefinitely.

        :param poll_interval_seconds:
            poll frequency for unlock query.

        :param changed_ok:
            True to lock the resource even if it was changed in the backend (will lock always).
            False to raise an exception in case it was changed in the backend (won't lock sometimes).

        :param context:
            The context level of the lock. Possible contexts:
                - "instance"  -> Lock at instance level. Other instances can't get lease of this lock in the app.
                - "app"       -> Lock at app level. Other instances CAN get lease of this lock in the app.
                - custom      -> Lock at a custom level. This can be any string.

            If not set, will be used the original File context (inherited from the container).

        :param autocreate:
            True to create the resource automatically if it doesnt exist.
            False to raise a ResourceExistsError. By default is False.

        :return:
            True if locked by this process. False otherwise.
        """
        start = time.time()

        lock_id = self._get_lock_id(context)
        lease = self.leases.get_lease(lock_id)

        if lease is not None:
            self.leases.update_lease(lock_id, lease_expire_seconds=duration_seconds)
            return True

        # The complex code of trying to acquire a lease... until we do
        while lease is None and (wait_seconds == -1 or (time.time() - start) < wait_seconds):

            kwargs = {}

            if not changed_ok and self._etag is not None:
                kwargs['etag'] = self._etag
                kwargs['match_condition'] = MatchConditions.IfModified

            try:
                lease = self._client.acquire_lease(lease_duration=60, **kwargs)

            except ResourceExistsError:
                # Raises when the file is already locked by other at the cloud backend.
                # We just have to wait for it to be released. Unfortunately we have to poll.
                # TODO: Exponential wait perhaps?
                time.sleep(poll_interval_seconds)

            except ResourceNotFoundError as e:
                # Resource not found? we must create it and retry again.
                if autocreate:
                    self.create()
                else:
                    raise e

        if lease is None:
            raise TimeoutError("Could not retrieve a lock in time")

        self.leases.add_lease(lock_id,
                              lease,
                              autorenew_seconds=30,
                              lease_expire_seconds=duration_seconds)

        return True

    def unlock(self, context=None):
        """
        Unlocks this file if was locked.

        :param context:
            The context level of the lock. Possible contexts:
                - "instance"  -> Lock at instance level. Other instances can't get lease of this lock in the app.
                - "app"       -> Lock at app level. Other instances CAN get lease of this lock in the app.
                - custom      -> Lock at a custom level. This can be any string.

        If not set, will be used the original File context (inherited from the container).
        """
        lock_id = self._get_lock_id(context)
        self.leases.remove_lease(lock_id)

    @property
    def tags(self):
        return self._client.get_blob_tags()

    @tags.setter
    def tags(self, new_tags):
        self._client.set_blob_tags(new_tags)

    @property
    def exists(self):
        return self._client.exists()

    @property
    def metadata(self):
        return self._client.get_blob_properties()

    @property
    def snapshots(self):
        return Snapshots(file=self)

    @property
    def modified(self):
        """
        Retrieves whether the last version of the file in the backend has changed w.r.t. the last data/metadata view of
        the file.
        """
        return self.metadata.get("etag") != self._etag or self._assigned

    def delete(self, changed_ok=False, context=None):
        """
        Deletes the file from the backend.
        If the blob has snapshots, they will all be removed.

        :param changed_ok:
            True to delete the blob even if it changed since last check in the backend.
            False to raise an error in case the file changed since last check in the backend.

        :param context:
            The context level of the lock. Possible contexts:
                - "instance"  -> Lock at instance level. Other instances can't get lease of this lock in the app.
                - "app"       -> Lock at app level. Other instances CAN get lease of this lock in the app.
                - custom      -> Lock at a custom level. This can be any string.

            If not set, will be used the original File context (inherited from the container).
        """
        context = context if context is not None else self._context
        lease = self.get_current_lease(context=context)

        kwargs = {}

        if not changed_ok and self._etag is not None:
            kwargs['etag'] = self._etag
            kwargs['match_condition'] = MatchConditions.IfNotModified

        if self.snapshot_id is None:
            kwargs['delete_snapshots'] = "include"

        try:
            self._client.delete_blob(lease=lease,
                                     **kwargs)
        except ResourceNotFoundError:
            # No error. The resource already deleted by someone else.
            pass

        self.reset_status()

    @property
    def size(self):
        return self.metadata.get('size', 0) if not self._assigned else len_nan(self._data, none_len=0)

    @property
    def last_modified(self):
        return to_datetime(self.metadata.get('last_modified'))

    def access_snapshot(self, snapshot_id):
        """
        Returns a wrapper around the specified Snapshot ID.
        This is equivalent to `file.snapshots[snapshot_id]`
        """
        return self.container.get_file(self.name,
                                       snapshot_id=snapshot_id)

    def clear_snapshots(self, context=None):
        """
        Removes all the snapshots of this file.
        This is equivalent to `file.snapshots.clear()`

        :param context:
            The context level of the lock. Possible contexts:
                - "instance"  -> Lock at instance level. Other instances can't get lease of this lock in the app.
                - "app"       -> Lock at app level. Other instances CAN get lease of this lock in the app.
                - custom      -> Lock at a custom level. This can be any string.

            If not set, will be used the original File context (inherited from the container).
        """
        context = context if context is not None else self._context
        lease = self.get_current_lease(context=context)

        self._client.delete_blob(delete_snapshots="only",
                                 lease=lease)

    def md5sum(self, changed_ok=False):
        """
        Retrieves the MD5 sum of the file.

        :param changed_ok:
            True to retrieve the md5sum regardless the changes in the backend.
            False to ensure that the given md5sum matches the last known local data.

        :return:
            Byte array of the MD5SUM
        """
        metadata = self.metadata
        content_md5 = metadata.get('content_settings', {}).get('content_md5')

        if changed_ok and metadata.get('etag') != self._etag:
            raise ValueError("Backend data has been updated since last check. Must be re-downloaded")

        return content_md5

    def __str__(self, context=None):
        """
        Retrieves a string representation of the current file.

        :param context:
            The context level of the lock. Possible contexts:
                - "instance"  -> Lock at instance level. Other instances can't get lease of this lock in the app.
                - "app"       -> Lock at app level. Other instances CAN get lease of this lock in the app.
                - custom      -> Lock at a custom level. This can be any string.

            If not set, will be used the original File context (inherited from the container).
        """
        context = context if context is not None else self._context
        lease = self.get_current_lease(context=context)

        try:
            metadata = self.metadata
        except ResourceNotFoundError:
            metadata = {}

        virtual = len(metadata) == 0
        modified = metadata.get('etag') != self._etag
        size = metadata.get('size') if not self._assigned else len_nan(self._data, none_len=0)
        last_modified = to_datetime(metadata.get('last_modified')) if metadata.get('last_modified') is not None else None

        encouraged_action = "download" if modified and not self._assigned else "upload"

        text_lines = [
            f"{'[VIRTUAL]' if virtual else ''}Azure File: {self.id};\n",
            f"\tSize: {size_to_human(size)}\n",
            f"\tlast_modified: {last_modified}\n",
            f"\tOutdated ({encouraged_action} encouraged)\n" if modified else '',
            f"\tSnapshot: {self.snapshot_id})\n" if self.snapshot_id is not None else '',
            f"\tLEASED: {lease})\n" if lease is not None else '',
            f"\tCACHED: {self._data is not None})"
        ]

        return "".join(text_lines)

    def __repr__(self):
        return str(self)

    def to_dict(self, include_lease=True, context=None):
        """
        Retrieves a dict representation of the current file.

        :param include_lease:
            True to include the lease information. False to exclude leases.

        :param context:
            The context level of the lock. Possible contexts:
                - "instance"  -> Lock at instance level. Other instances can't get lease of this lock in the app.
                - "app"       -> Lock at app level. Other instances CAN get lease of this lock in the app.
                - custom      -> Lock at a custom level. This can be any string.

            If not set, will be used the original File context (inherited from the container).
        """
        context = context if context is not None else self._context
        lease = self.get_current_lease(context=context)
        etag = self._etag
        snapshot = self.snapshot_id

        result = {
            "name": self.name,
        }

        if etag is not None:
            result["etag"] = etag

        if lease is not None and include_lease:
            result['lease_id'] = lease

        if self.snapshot_id is not None:
            result['snapshot'] = snapshot

        return result

    def reset_status(self, clear_lease=False):
        """
        Resets the internal status of the file (as new fresh instance).
        """
        self._etag = None
        self._md5sum = None
        self._data = None
