from azure.core.exceptions import ResourceModifiedError, ResourceNotFoundError, HttpResponseError
from azure.storage.blob import PartialBatchErrorException

from cloudspeak.storage.azure import AzureService
from cloudspeak.serializers import JoblibSerializer
from cloudspeak.utils.basics import removeprefix


class AzureDictionary:
    INDEX_NAME = "__--REMOTE_DICT--INDEX--__"

    @classmethod
    def from_connection_string(cls, connection_string, container_name, folder_name, indexed=True,
                               serializer=JoblibSerializer(), create_container=True, context=None):
        """
        Creates an instance of a Remote Dictionary based on Azure Blob Storage backend.

        :param connection_string:
            Azure Blob Storage connection string.

        :param container_name:
            Name of the container to use as storage.
            If the container doesn't exist, it will be created.

        :param folder_name:
            Name of the folder to use as storage.

        :param indexed:
            True to store an index along with the storage.
            The index allows to retrieve how many elements are stored faster than not having indexes, but each write or
            delete operation must synchronize the index, which might add delays to the process.

            Disabling the index speed is gained in I/O operations, but knowing the number of elements or iterating over
            them gets more complexity.

        :param serializer:
            Serializer to use as backend storage.
            Allowed serializers can be seen in package `cloudsimple.serializers`.
            None to store/fetch as raw bytes.

        :param create_container:
            Creates the container in case it doesn't exit. NOTE: Adds delay since it must query backend.

        :param context:
            Context name for leases.
        """
        service = AzureService(connection_string=connection_string, serializer=serializer)
        container = service[container_name]

        if create_container:
            container.create()

        return cls(container, folder_name, indexed=indexed, context=context)

    def __init__(self, container, folder_name, indexed=True, context=None):
        if not folder_name.endswith("/"):
            folder_name += "/"

        self._container = container
        self._folder_name = folder_name
        self._context = context
        self._index_file = container[self.get_url(self.INDEX_NAME)] if indexed else None

    def get_url(self, item_name):
        """
        Retrieves the full blob storage URL of the given item name.
        """
        return f"{self._folder_name}{item_name}"

    @property
    def context(self):
        return id(self) if self._context is None else self._context

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
            index_file.lock(duration_seconds=60,
                            context=self.context)
            index_file.data = new_content
            index_file.upload(overwrite=True,
                              allow_changed=False)

        except ResourceModifiedError:
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
            index_file.unlock(context=self.context)

    def lock(self, key, duration_seconds=30, wait_seconds=-1, poll_interval_seconds=0.5, autocreate=False):
        """
        Locks the given key for the specified seconds in a given context.

        A locked key prevents updating its content or removing it by other processes.

        If a key is already locked in a different context, the process will be blocked until the wait_seconds is met
        or until the key is unlocked again.

        :param key:
            Name of the key to lock in the dictionary.

        :param duration_seconds:
            Number of seconds to lock the key.

        :param wait_seconds:
            Number of seconds to wait if the key was already locked.

        :param poll_interval_seconds:
            Query interval for the key lock release status.

        :param autocreate:
            True to create the resource automatically if it doesnt exist.
            False to raise a KeyError in case it doesnt exist.
            By default is False.

        :returns:
            True if locked successfully. False otherwise.
        """
        context = self.context
        item = self.get_url(key)
        container = self.container

        try:
            result = container[item].lock(duration_seconds=duration_seconds,
                                          wait_seconds=wait_seconds,
                                          poll_interval_seconds=poll_interval_seconds,
                                          context=context,
                                          changed_ok=True,
                                          autocreate=autocreate)

        except ResourceNotFoundError as e:
            raise KeyError(f"Key `{removeprefix(item, self._folder_name)}` not found.") from None

        return result

    def unlock(self, key):
        """
        Unlocks the given key under the specified context.

        :param key:
            Name of the key to unlock (if locked).

        """
        context = self.context
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

    def __getitem__(self, key):
        progresses = self.async_get(key)

        is_list = isinstance(key, list)

        # Special case: folders as dictionaries
        if not is_list and type(progresses) is AzureDictionary:
            return progresses

        if not is_list:
            progresses = [progresses]

        key_name = None
        try:
            for p in progresses:
                key_name = removeprefix(p.file.name, self._folder_name)
                p.join(tqdm_bar=False)

        except ResourceNotFoundError:
            raise KeyError(f"The key \"{key_name}\" does not exist.") from None

        result = [p.file.data for p in progresses]
        result = result if is_list else progresses[0].file.data

        return result

    def async_get(self, key):
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
        is_list = isinstance(key, list)

        if not is_list and key.endswith("/") and not self.indexed:
            # We return a new dictionary instance:
            return AzureDictionary(container=self._container,
                                   folder_name=self.get_url(key),
                                   indexed=False,
                                   context=self.context)

        if not is_list:
            key = [key]

        # We add the folder prefix to each
        key = [self.get_url(i) for i in key]

        if self.indexed:
            files = [container[f] for f in key]
        else:
            files = [container[f] for f in key if not f.endswith("/")]

        progresses = [f.download() for f in files]

        if len(files) < len(key):
            raise KeyError("Some of the keys are ending in backslash '/'. Accessing folders in bulk are not supported. "
                           "Please, access one folder at a time since they are converted into an AzureDictionary.")

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
        context = self.context

        is_list = isinstance(key, list)

        if not is_list:
            key = [key]
            value = [value]

        progresses = []

        for k, v in zip(key, value):
            # We add the folder prefix to each
            file = container[self.get_url(k)]
            file.data = v
            progress = file.upload(overwrite=True,
                                   allow_changed=True,
                                   context=context)
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

        write_failed = []
        write_failed_reasons = []

        for p, k in zip(progresses, key):
            try:
                p.join(tqdm_bar=False)
            except HttpResponseError as e:
                write_failed.append(k)
                write_failed_reasons.append(e.reason)

        if self.indexed:
            idx_content = self.index
            exclude_keys = set(key).union(write_failed)
            new_content = [x for x in idx_content if x not in exclude_keys]
            new_content.extend(key)
            self.index = new_content

        if write_failed:
            if is_list:
                error = KeyError(f"There were {len(write_failed)} out of {len(key)} keys that couldn't be written."
                                 f"Access the .keys attribute of this exception to know which keys could not be written "
                                 f"and the reason.")

                error.keys = list(zip(write_failed, write_failed_reasons))
            else:

                reason = write_failed_reasons[0]
                error = KeyError(f"The key {write_failed[0]} could not be written. Reason: {reason}.")
                error.keys = [(write_failed[0], reason)]

            raise error

    def __len__(self):
        """
        Returns the length of the dictionary.

        Note: len() is an expensive operation in not-indexed dictionaries as it requires walking all the blobs.
        """
        return len(self.keys())

    def __iter__(self):
        index = self.index

        if index is None:
            folder_name = self._folder_name

            # We iterate over the blobs
            for f in self._container.get_files(folder_name, delimiter="~"):
                name = removeprefix(f.name, folder_name)

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
        return self._container[self.get_url(item)].exists

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

        context = self.context
        container = self.container
        folder_name = self._folder_name

        if not is_list:
            key = [key]

        if len(key) == 0:
            return

        files = [container[self.get_url(k)] for k in key]

        keys_not_removed = []
        keys_not_removed_reasons = []

        exception = None

        try:
            container.delete_many_files(files, changed_ok=True, raise_errors=True, context=context)
            keys_removed = set(key)

        except PartialBatchErrorException as e:
            files_not_removed, files_reasons = zip(*e.parts)

            for f, r in zip(files_not_removed, files_reasons):
                if r.status_code == 404:
                    # Could not remove because they ARE already removed
                    continue

                keys_not_removed.append(removeprefix(f.name, folder_name))
                keys_not_removed_reasons.append(r)

            keys_removed = set(key).difference(keys_not_removed)
            exception = e

        if self.indexed:
            index_content = self.index
            index_content = [k for k in index_content if k not in keys_removed]
            self.index = index_content

        if len(keys_not_removed) > 0 and exception is not None:
            if is_list:
                error = KeyError(f"Only {len(keys_removed)} out of {len(key)} could be removed. "
                                 f"Access the .keys attribute of this exception to know which keys could not be removed and "
                                 f"the reason.")
                error.keys = list(zip(keys_not_removed, keys_not_removed_reasons))

            else:
                reason = exception.parts[0][1].reason
                error = KeyError(f"The key {key[0]} could not be removed. Reason: {reason}.")
                error.keys = [(key[0], reason)]

            raise error

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

        string_repr = f"[Azure dictionary {indexed_string}]"

        return string_repr

    def __repr__(self):
        return str(self)
