#!/usr/bin/env python
# -*- coding: utf-8 -*-
# flake8: noqa
"""The setup script."""
from setuptools import find_packages
from setuptools import setup

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

requirements = ["Click>=7.0", "requests", "simplejson", "confuse", "desert", "marshmallow", "marshmallow_oneofschema", "attrs"]

setup_requirements = ["pytest-runner", "pip"]


setup(
    author="Ryan Murray",
    author_email="rymurr@dremio.com",
    python_requires=">=3.5",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="Python Boilerplate contains all the boilerplate you need to create a Python package.",
    entry_points={"console_scripts": ["nessie_client=nessie_client.cli:cli",],},
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + "\n" + history,
    include_package_data=True,
    keywords="nessie_client",
    name="nessie_client",
    packages=find_packages(include=["nessie_client", "nessie_client.*"]),
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=[],
    url="https://github.com/rymurr/nessie_client",
    version="0.0.1",
    zip_safe=False,
)
