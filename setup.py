#!/usr/bin/env python

from setuptools import find_packages
from setuptools import setup

setup(
    name='example-weather-forecast',
    version='0.0.1',
    author='Ivan Ukhov',
    author_email='ivan.ukhov@gmail.com',
    url='https://github.com/chain-rule/example-weather-forecast',
    packages=find_packages(),
    install_requires=[
        'google-cloud-core>=1.0.0',
        'tensorflow-metadata==0.14.0',
        'tensorflow-transform==0.14.0',
    ],
)
