
def len_nan(x, none_len=None):
    """
    Len operation with support for None.

    :param x:
        X to check the length

    :param none_len:
        Length of None objects. None to raise exception.

    :return:
        result of len(x) or none_len if x is None and none_len is not None
    """
    return len(x) if x is none_len or x is not None else none_len


def removeprefix(self: str, prefix: str) -> str:
    if self.startswith(prefix):
        return self[len(prefix):]
    else:
        return self[:]


def removesuffix(self: str, suffix: str) -> str:
    # suffix='' should not call self[:-0].
    if suffix and self.endswith(suffix):
        return self[:-len(suffix)]
    else:
        return self[:]
