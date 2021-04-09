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

with open("requirements.txt") as fp:
    requirements = fp.read()

setup_requirements = ["pytest-runner", "pip"]

setup(
    author="Ryan Murray",
    author_email="nessie-release-builder@dremio.com",
    python_requires=">=3.5",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    description="Project Nessie: A Git-like Experience for your Data Lake",
    entry_points={
        "console_scripts": [
            "nessie=pynessie.cli:cli",
        ],
    },
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + "\n" + history,
    include_package_data=True,
    keywords="pynessie",
    name="pynessie",
    packages=find_packages(include=["pynessie", "pynessie.*"]),
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=[],
    url="https://github.com/projectnessie/nessie",
    version="0.5.2",
    zip_safe=False,
)
