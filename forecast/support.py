def timestamp(object, now):
    if isinstance(object, dict):
        return {name: timestamp(value, now) for name, value in object.items()}
    if isinstance(object, str):
        return now.strftime(object)
    return object
