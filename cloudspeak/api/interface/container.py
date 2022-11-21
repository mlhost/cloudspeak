
class Container:
    def __init__(self, service):
        self._service = service

    @property
    def service(self):
        return self._service

    def create(self):
        raise NotImplementedError("")

    @property
    def exists(self):
        raise NotImplementedError("")

    @property
    def metadata(self):
        raise NotImplementedError("")

    @metadata.setter
    def metadata(self, new_metadata):
        raise NotImplementedError("")

    def lock(self, duration_seconds=-1):
        raise NotImplementedError("")

    def unlock(self):
        raise NotImplementedError("")

    def delete(self):
        raise NotImplementedError("")

    def __getitem__(self, filename):
        raise NotImplementedError("")

    def __str__(self):
        raise NotImplementedError("")

    def __repr__(self):
        raise NotImplementedError("")
