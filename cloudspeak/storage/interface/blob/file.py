
class File:

    def __init__(self, container):
        self._container = container

    @property
    def container(self):
        return self._container

    @property
    def service(self):
        return self._container.service

    def create(self):
        raise NotImplementedError("")

    def upload(self, overwrite=True, allow_changed=False):
        raise NotImplementedError("")

    def download(self, offset=None, length=None):
        raise NotImplementedError("")

    def lock(self, duration_seconds=-1):
        raise NotImplementedError("")

    def unlock(self):
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

    @property
    def data(self):
        raise NotImplementedError("")

    @data.setter
    def data(self, new_data):
        raise NotImplementedError("")

    def modified(self):
        raise NotImplementedError("")

    def delete(self, snapshot=None):
        raise NotImplementedError("")
