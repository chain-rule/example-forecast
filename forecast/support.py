def timestamp(object, now):
    if isinstance(object, dict):
        return {name: timestamp(value, now) for name, value in object.items()}
    if isinstance(object, str):
        return now.strftime(object)
    return object


def list_files(path):
    from tensorflow.python.lib.io.file_io import get_matching_files
    return sorted(get_matching_files(path))
