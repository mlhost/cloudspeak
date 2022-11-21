
def size_to_human(num, suffix="B"):
    if num is None:
        return None

    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:

        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"

        num /= 1024.0

    return f"{num:.1f}Yi{suffix}"
