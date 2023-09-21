
class Queue:
    def __init__(self, service_storage):
        self._service_storage = service_storage

    @property
    def service_storage(self):
        return self._service_storage

    def create(self):
        raise NotImplementedError("")
    
    def push(self, messages):
        raise NotImplementedError("Not implemented")

    def pop(self, count=1, wait_time=0, ensure_count=False):
        raise NotImplementedError("Not implemented")

    def __str__(self):
        raise NotImplementedError("Not implemented")

    def delete(self):
        raise NotImplementedError("")
