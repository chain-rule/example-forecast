import argparse
import datetime
import json

from forecast.support import timestamp
from forecast.task import Task

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--action', type=str, required=True)
    parser.add_argument('--config', type=str, required=True)
    arguments = parser.parse_args()
    with open(arguments.config) as file:
        config = timestamp(json.load(file), datetime.datetime.now())
        Task().run(arguments.action, config)
