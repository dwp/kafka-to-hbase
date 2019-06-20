#!/usr/bin/env python

""" Setup.py for Kafka To Hbase """

from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = [l.strip() for l in f]

with open("requirements-dev.txt") as f:
    requirements_dev = [l.strip() for l in f]

setup(
    name='kafka-to-hbase',
    version='1.0',
    description='Simple Kafka to Hbase importer',
    author='Craig Andrews',
    author_email='craig.andrews@infinityworks.com',
    url='https://github.com/dwp/kafka-to-hbase',
    packages=find_packages(),
    scripts=['scripts/kafka-to-hbase'],
    install_requires=requirements,
    extras_require={
        'dev': requirements_dev,
    }
)
