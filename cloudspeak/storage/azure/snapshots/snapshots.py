
class Snapshots:
    """
    Lazy-loaded list of snapshots
    """
    def __init__(self, file=None):
        self._file = file

    @property
    def file(self):
        return self._file

    def __iter__(self):
        file = self.file
        container = file.container
        container_raw = container.container_raw

        for blob in container_raw.walk_blobs(name_starts_with=file.name, include="snapshots", delimiter="/"):
            if blob.name != file.name or blob.snapshot is None:
                continue

            snapshot = file.access_snapshot(blob.snapshot)
            yield snapshot

    def __len__(self):
        return len([x for x in self])

    def to_list(self):
        return list(self)

    def make(self):
        """
        Makes a new snapshot of the current file.
        """
        file = self.file
        client = file.file_raw

        snapshot = client.create_snapshot().get('snapshot')
        return file.access_snapshot(snapshot)

    def __getitem__(self, item):
        if type(item) is int:
            snapshot = self.to_list()[item]
        else:
            file = self.file
            snapshot = file.access_snapshot(item)

        return snapshot

    def __setitem__(self, key, value):
        raise ValueError("Snapshot blobs are read-only.")

    def __delitem__(self, key):
        snapshot = self[key]
        snapshot.delete()

    def clear(self):
        file = self.file
        file.clear_snapshots()

    def __str__(self):
        return f"[Snapshots of {self.file}: {len(self)}]"

    def __repr__(self):
        return str(self)
