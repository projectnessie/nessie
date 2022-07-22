=======
History
=======

0.31.0 (2022-07-22)
-------------------

* Change Pynessie dependencies import from relative to absolute
* Use isort instead of flake8-import-order

0.30.0 (2022-05-13)
-------------------

* (No Python related highlights)

0.29.0 (2022-05-05)
-------------------

* (No Python related highlights)

0.28.0 (2022-04-26)
-------------------

* (No Python related highlights)

0.27.0 (2022-04-14)
-------------------

* (No Python related highlights)

0.26.0 (2022-04-12)
-------------------

* (No Python related highlights)

0.25.0 (2022-04-06)
-------------------

* (No Python related highlights)

0.24.0 (2022-03-31)
-------------------

* (No Python related highlights)

0.23.1 (2022-03-23)
-------------------

* (No Python related highlights)

0.23.0 (2022-03-23)
-------------------

* (not released)

0.22.0 (2022-03-11)
-------------------

* (No Python related highlights)

0.21.2 (2022-03-02)
-------------------

* (No Python related highlights)

0.21.1 (2022-03-02)
-------------------

* (No Python related highlights)

0.21.0 (2022-03-01)
-------------------

* (No Python related highlights)

0.20.1 (2022-02-17)
-------------------

* (No Python related highlights)

0.20.0 (2022-02-16)
-------------------

* (No Python related highlights)

0.19.0 (2022-02-07)
-------------------

* Reads using "detached" commit-ids w/o specifying a branch or tag name
* Support for Iceberg views (experimental)

0.18.0 (2022-01-13)
-------------------

* Add new reflog command to the CLI
* Add support for Python 3.10
* Drop support for Python 3.6

0.17.0 (2021-12-08)
-------------------

* Rename --query/--query-expression flag to --filter

0.16.0 (2021-12-03)
-------------------

* Add -x flag to fetch additional metadata for branches/tags
* Add diff command to show the diff between two references

0.15.1 (2021-12-01)
-------------------

* no changes for Python

0.15.0 (2021-12-01)
-------------------

* Enhance commit log to optionally return original commit operations

0.14.0 (2021-11-12)
-------------------

* Updated 'IcebergTable' to track more information
* Better 'ContentKey' handling
* Nessie CLI code cleanups

0.12.1 (2021-11-03)
-------------------

* Update / clarify CLI docs
* Fix 'pynessie.auth' not found error
* Clearer 'nessie log' cli command

0.12.0 (2021-10-25)
-------------------

* Specialize and document Nessie exceptions
* Fix --json on specific branches and tags

0.11.0 (2021-10-20)
-------------------

* Fix Nessie's representation of global and on-reference state (Iceberg tables)
* Support expected contents in Nessie Put operations in CLI
* Fix CLI log -n option

0.10.1 (2021-10-08)
-------------------

* Various fixes and improvements
* Update REST-API calls for new version-store API requirements

0.9.2 (2021-08-26)
------------------

* (No Python related highlights)

0.9.0 (2021-08-09)
------------------

* (No Python related highlights)

0.8.3 (2021-07-19)
------------------

* Fix ser/de of SqlView when listing contents

0.8.2 (2021-07-15)
------------------

* REST-API change: only accept named-references
* REST-API change: Server-side commit range filtering
* OpenAPI: more explicit constraints on parameters
* Commit-log filtering on all fields of CommitMeta
* Use "Common Expression Language" for commit-log and entries filtering
* Prepare for multi-tenancy
* Fix ser/de of DeltaLakeTable when listing contents

0.7.0 (2021-06-15)
------------------

* Fix naming in nessie client merge operation
* Distinguish between author & committer in the Python CLI
* Allow setting author when committing via Python CLI
* Loosen pins for client install on Python cli

0.6.1 (2021-05-25)
------------------

(no Python relevant changes)

0.6.0 (2021-05-12)
------------------

* create-reference and commit operations return the new commit-hash
* dependency updates

0.5.1 (2021-04-09)
------------------

(no Python relevant changes)

0.5.0 (2021-04-08)
------------------

* dependency updates
* endpoint updates for object type and new commit metadata object

0.4.0 (2021-03-08)
------------------

* dependency updates

0.3.0 (2020-12-30)
------------------

* support for python3.9
* correct display of contents in the cli
* better type checking

0.2.1 (2020-10-30)
------------------

* fix install requirements in setup.py

0.2.0 (2020-10-30)
------------------

* git-like cli interface
* more complete coverage of REST endpoints
* better testing

0.1.1 (2020-10-01)
------------------

* First release on PyPI.
