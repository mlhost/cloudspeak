from azure.core.exceptions import ResourceModifiedError, ResourceNotFoundError

from cloudspeak.api.azure import AzureService
from cloudspeak.serializers import JoblibSerializer


class AzureDictionary:
    INDEX_NAME = "__--REMOTE_DICT--INDEX--__"

    @classmethod
    def from_connection_string(cls, connection_string, container_name, folder_name, indexed=True,
                               serializer=JoblibSerializer(), create_container=True):
        """
        Creates an instance of a Remote Dictionary based on Azure Blob Storage backend.

        :param connection_string:
            Azure Blob Storage connection string.

        :param container_name:
            Name of the container to use as storage.
            If the container doesn't exist, it will be created.

        :param folder_name:
            Name of the folder to use as storage.

        :param serializer:
            Serializer to use as backend storage.
            Allowed serializers can be seen in package `cloudsimple.serializers`.
            None to store/fetch as raw bytes.

        :param create_container:
            Creates the container in case it doesn't exit. NOTE: Adds delay since it must query backend.

        :param indexed:

        """
        service = AzureService(connection_string=connection_string, serializer=serializer)
        container = service[container_name]

        if create_container:
            container.create()

        return cls(container, folder_name, indexed=indexed)

    def __init__(self, container, folder_name, indexed=True):
        if not folder_name.endswith("/"):
            folder_name += "/"

        self._container = container
        self._folder_name = folder_name
        self._index_file = container[self.get_url(self.INDEX_NAME)] if indexed else None

    def get_url(self, item_name):
        """
        Retrieves the full blob storage URL of the given item name.
        """
        return f"{self._folder_name}{item_name}"

    @property
    def indexed(self):
        """
        Returns whether this instance is indexed or not.
        """
        return self._index_file is not None

    @property
    def index(self):
        """
        Return the index content only if this instance is indexed.
        A RemoteDict instance is indexed if injected "indexed=True" in the constructor.
        This property will always retrieve the latest index version.
        """
        if not self.indexed:
            return None

        index_file = self._index_file

        try:
            index_file.download().join(tqdm_bar=False)
            index_content = self._index_file.data

            if index_content is None:
                index_content = []

        except ResourceNotFoundError:
            index_content = []

        return index_content

    @index.setter
    def index(self, new_content):
        """
        Sets the content of the index.

        If content was updated, before setting, this setter will merge the modifications with the ones in the backend.
        """
        if not self.indexed:
            raise KeyError("No index available for this dictionary. Enable indexing by instancing with indexed=True.")

        index_file = self._index_file
        idx_old_content = index_file.data

        if idx_old_content is None:
            idx_old_content = []

        # We try to upload the new index.
        try:
            index_file.lock(duration_seconds=60, context=id(self))
            index_file.data = new_content
            index_file.upload(overwrite=True, allow_changed=False)

        except ResourceModifiedError as e:
            # If the content has changed we need to merge changes.
            index_file.download()
            idx_latest_content = index_file.data

            if idx_latest_content is None:
                idx_latest_content = []

            old_index_set = set(idx_old_content)
            new_index_set = set(idx_latest_content)

            # We take which elements new content added w.r.t. old content:
            elements_added = [x for x in new_content if x not in old_index_set]

            # We take which elements new content removed w.r.t. old content:
            elements_removed = set([x for x in idx_old_content if x not in new_content])

            # We take which elements other instances removed w.r.t. old content
            elements_other_removed = set([x for x in idx_old_content if x not in new_index_set])

            merged_content = [x for x in idx_latest_content if x not in elements_removed]

            # We add elements added if they were not removed by others:
            elements_to_append = [x for x in elements_added if x not in elements_other_removed]

            merged_content.extend(elements_to_append)

            # The index is locked before last download. We are going to have the last version till this line for sure.

            # Recursive assignment: A new attempt is performed until allowed.
            # Since the lock is done within the same context, cascading locks is safe (only first is taken into account)
            index_file.data = merged_content
            index_file.upload(overwrite=True, allow_changed=True)

        finally:
            index_file.unlock(context=id(self))

    def lock(self, key, duration_seconds=30, wait_seconds=-1, poll_interval_seconds=0.5, context=None):
        """
        Locks the given key for the specified seconds in a given context.

        A locked key prevents updating its content or removing it by other processes.

        If a key is already locked in a different context, the process will be blocked until the wait_seconds is met
        or until the key is unlocked again.

        :param duration_seconds:
            Number of seconds to lock the key.

        :param wait_seconds:
            Number of seconds to wait if the key was already locked.

        :param poll_interval_seconds:
            Query interval for the key lock release status.

        :param context:
            String identifying the context of this lock in the current computer Process.
            All operations are allowed under this context as lease is visible.
            Note that this doesn't apply on external processes or machines. Available contexts:
                - "instance"  -> Lock at instance level. Other file instances can't get lease of this lock in the
                                 app.
                - "app"       -> Lock at app level. Other instances CAN get lease of this lock in the app.
                - custom      -> Lock at a custom level. This can be any string.

            By default it is locked under the context of ID of this RemoteDict object.
            This means that only this remote dict object can write/delete/unlock the locked object.

        :returns:
            True if locked successfully. False otherwise.
        """
        if context is None:
            context = str(id(self))

        item = self.get_url(key)
        container = self.container

        return container[item].lock(duration_seconds=duration_seconds,
                                    wait_seconds=wait_seconds,
                                    poll_interval_seconds=poll_interval_seconds,
                                    context=context,
                                    ignore_changes=True)

    def unlock(self, key, context=None):
        """
        Unlocks the given key under the specified context.

        :param key:
            Name of the key to unlock (if locked).

        :param context:
            String identifying the context of this unlock in the current computer Process.
            All operations are allowed under this context as lease is visible.
            Note that this doesn't apply on external processes or machines. Available contexts:
                - "instance"  -> Lock at instance level. Other file instances can't get lease of this lock in the
                                 app.
                - "app"       -> Lock at app level. Other instances CAN get lease of this lock in the app.
                - custom      -> Lock at a custom level. This can be any string.

            By default it is locked under the context of ID of this RemoteDict object.
            This means that only this remote dict object can write/delete/unlock the locked object.
        """
        if context is None:
            context = str(id(self))

        item = self.get_url(key)
        container = self.container

        container[item].unlock(context=context)

    @property
    def container(self):
        """
        Retrieves the Azure Container where this remote dict lies.
        """
        return self._container

    @property
    def service(self):
        """
        Retrieves the Azure Service that handles this service.

        The service can be used to track down download/upload progresses.

        For example, the progresses can be accessed in:

            ```python
            # For overall progress info:
            service.progresses

            # For downloads progress info:
            service.progresses.download

            # For uploads progress info:
            service.progresses.upload
            ```
        """
        return self.container.service

    def __getitem__(self, item):
        is_list = isinstance(item, list)

        if not is_list:
            item = [item]

        progresses = self.async_get(item)

        key_name = None
        try:
            for p in progresses:
                key_name = p.file.name.lstrip(self._folder_name)
                p.join(tqdm_bar=False)

        except ResourceNotFoundError:
            raise KeyError(f"The key \"{key_name}\" does not exist.") from None

        result = [p.file.data for p in progresses]
        result = result if is_list else progresses[0].file.data

        return result

    def async_get(self, item):
        """
        Gets a key -> value from the dictionary in an asynchronous operation.
        It can be retrieved many keys and values at once if lists are specified.

        :param key:
            Key name or list of key names to retrieve values from.

        :return:
            A list of Progress objects or a single Progress if it wasn't a list. The Progress is an object that allows
            tracking the download operations (speeds, times, sizes, ...).
        """
        container = self._container
        is_list = isinstance(item, list)

        if not is_list:
            item = [item]

        # We add the folder prefix to each
        item = [self.get_url(i) for i in item]

        files = [container[f] for f in item]
        progresses = [f.download() for f in files]

        result = progresses if is_list else progresses[0]

        return result

    def async_set(self, key, value, write_index=False):
        """
        Sets a key -> value in the dictionary in an asynchronous operation.
        It can be set many keys and values at once if lists are specified.

        :param key:
            Key name or list of key names to set values.

        :param value:
            Value or list of values to asign for each key (if both are lists).

        :param write_index:
            If True and the instance is indexed, it will be reflected in the index instantly.
            If False, index won't be altered.

        :return:
            A list of Progress objects or a single Progress if it wasn't a list. The Progress is an object that allows
            tracking the upload operations (speeds, times, sizes, ...).
        """
        container = self._container
        is_list = isinstance(key, list)

        if not is_list:
            key = [key]
            value = [value]

        progresses = []

        for k, v in zip(key, value):
            # We add the folder prefix to each
            file = container[self.get_url(k)]
            file.data = v
            progress = file.upload(overwrite=True, allow_changed=True)
            progresses.append(progress)

        result = progresses if is_list else progresses[0]

        if write_index and self.indexed:
            idx_content = self.index
            new_content = [x for x in idx_content if x not in key]
            new_content.extend(key)
            self.index = new_content

        return result

    def __setitem__(self, key, value):
        is_list = isinstance(key, list)

        if not is_list:
            key = [key]
            value = [value]

        progresses = self.async_set(key, value, write_index=False)

        for p in progresses:
            p.join(tqdm_bar=False)

        if self.indexed:
            idx_content = self.index
            new_content = [x for x in idx_content if x not in key]
            new_content.extend(key)
            self.index = new_content

    def __len__(self):
        """
        Returns the length of the dictionary.

        Note: len() is an expensive operation in not-indexed dictionaries as it requires walking all the blobs.
        """
        return len(self.keys())

    def __iter__(self):
        index = self.index

        if index is None:
            # We iterate over the blobs
            for f in self._container.get_files(self._folder_name, delimiter="/"):
                name = f.name.lstrip(self._folder_name)

                # Index name is excluded
                if name == self.INDEX_NAME:
                    continue

                yield name

        else:
            # We have an index to retrieve in one shot
            yield from index

    def keys(self):
        return [x for x in self]

    def values(self):
        for x in self:
            yield self[x]

    def items(self):
        for k, v in zip(self.keys(), self.values()):
            yield k, v

    def __contains__(self, item):
        return self._container[item].exists

    def setdefault(self, key, default=None):
        """
        Sets a default value for a given key in case it doesn't exist, and returns the existing value for the key.

        :param key:
            Key to retrieve and whose default value should be set in case it doesn't exist.

        :param default:
            Value to set if key doesn't exist.

        :return:
            Value associated to the key. Default if it doesn't exist in the dictionary.
        """
        try:
            result = self[key]

        except KeyError:
            result = default
            self[key] = result

        return result

    def get(self, item, default=None):
        try:
            result = self[item]

        except KeyError:
            result = default

        return result

    def __delitem__(self, key):
        """
        Deletes one or many items from the dictionary.
        The items are also deleted from the backend.

        :param key:
            Key name or list of keys to delete.
        """
        is_list = isinstance(key, list)

        container = self.container

        if not is_list:
            key = [key]

        files = [container[self.get_url(k)] for k in key]

        container.delete_many_files(files, changed_ok=True)

        if self.indexed:
            index_content = self.index
            keys_removed = set(key)
            index_content = [k for k in index_content if k not in keys_removed]
            self.index = index_content

    def clear(self):
        """
        Clears the current dictionary. All the elements are marked to remove.
        Note: clear() is an expensive operation in not-indexed dictionaries as it requires walking all the blobs.
        """
        if not self.indexed:
            del self[self.keys()]

        else:
            index_content = self.index
            del self[index_content]
            self.index = []

    def __str__(self):
        indexed = self.indexed

        if indexed:
            indexed_string = f"(Indexed: True; Num elements: {len(self)})"
        else:
            indexed_string = f"(Indexed: False)"

        return indexed_string

    def __repr__(self):
        return str(self)
