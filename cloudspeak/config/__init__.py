_config = {
    "blob.max_concurrency_upload": 2,
    "blob.max_concurrency_download": 2,
    "blob.query.results_per_page": 1000,
}


def get_config():
    return _config


__all__ = ["get_config"]
