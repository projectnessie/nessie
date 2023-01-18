# Copyright (C) 2020 Dremio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# flake8: noqa
"""The setup script."""
from setuptools import find_packages
from setuptools import setup

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

# here we have listed all dependencies w/o explicit pins to enable flexibility in client installs.
# we use `requirements.txt` in this directory when testing to ensure a stable test in CI.
requirements = [
    "attrs",  # features we use are not regularly changing
    "botocore",  # features we use are not regularly changing
    "Click<9.0.0,>6.0.0",  # pinning to 7.x or 8.x as we have used w/ both
    "confuse==2.0.0",  # important for config so don't change w/o testing
    "desert",  # features we use are not regularly changing
    "marshmallow",  # features we use are not regularly changing
    "marshmallow_oneofschema",  # features we use are not regularly changing
    "python-dateutil",  # stable
    "requests",  # stable
    "requests-aws4auth",  # stable
    "simplejson",  # stable
]

setup(
    author="Ryan Murray",
    author_email="nessie-release-builder@dremio.com",
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    description="Project Nessie: Transactional Catalog for Data Lakes with Git-like semantics",
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
    url="https://github.com/projectnessie/nessie",
    version="0.47.1",
    zip_safe=False,
)
