=============================
Python API and CLI for Nessie
=============================


.. image:: https://img.shields.io/pypi/v/nessie_client.svg
        :target: https://pypi.python.org/pypi/nessie_client

.. image:: https://img.shields.io/travis/rymurr/nessie_client.svg
        :target: https://travis-ci.com/rymurr/nessie_client

.. image:: https://readthedocs.org/projects/nessie-client/badge/?version=latest
        :target: https://nessie-client.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status


.. image:: https://pyup.io/repos/github/rymurr/nessie_client/shield.svg
     :target: https://pyup.io/repos/github/rymurr/nessie_client/
     :alt: Updates



Python Boilerplate contains all the boilerplate you need to create a Python package.


* Free software: Apache Software License 2.0
* Documentation: https://nessie-client.readthedocs.io.


Installation
------------

For testing purposes it is better to run ``python setup.py develop`` to install the package as a symlink. For permanent installation do:

Currently not on pypi so to install check out this repository and run:
``pip install -e /path/to/nessie/python``

Test & Develop
--------------

First create a virtual environment and install neccessary packaging ``python3 -m venv env && source venv/bin/activate`` then install all packages ``pip install -r requirements_dev.txt -r requirements_lint.txt -r requirements.txt``


We use ``tox`` to test. Tox runs pytest, mypy, flake8 and checks for package vulnerabilities. To run all checks simply run ``tox``. To run against a specific environment run ``tox -e <env>``. Envs include ``py36``, ``py37``, ``py38``, ``flake8`` (the style and mypy env).



Features
--------

* TODO

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
