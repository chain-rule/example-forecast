import argparse
import datetime
import json

from forecast.task import Task


def _format(object, now):
    if isinstance(object, dict):
        return {name: _format(value, now) for name, value in object.items()}
    if isinstance(object, str):
        return now.strftime(object)
    return object


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--action', type=str, required=True)
    parser.add_argument('--config', type=str, required=True)
    arguments = parser.parse_args()
    with open(arguments.config) as file:
        config = _format(json.load(file), datetime.datetime.now())
        Task().run(arguments.action, config)
