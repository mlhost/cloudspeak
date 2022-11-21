
class Service:

    def __getitem__(self, container_name):
        raise NotImplementedError("")

    def __iter__(self):
        raise NotImplementedError("")

    def __len__(self):
        raise NotImplementedError("")

    def __str__(self):
        raise NotImplementedError("")

    def __repr__(self):
        raise NotImplementedError("")

    def list(self):
        return list(self)
