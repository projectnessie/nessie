# Releases

## 0.23.1 Release (March 23, 2022)

* Support Namespaces
* CI "perf tests" improvements
* SQL Extension: Fix Create reference from a hash on non-default reference
* Enhance authorization checks
* Support custom annotations on Nessie Helm service

## 0.23.0 Release (March 23, 2022)

(not properly released)

## 0.22.0 Release (March 11, 2022)

* Improve performance of `getValues`
* Global-log compaction
* Store-level maintenance CLI
* Reduce number of tags for micrometer
* Grafana Dashboard for Nessie service
* Add new commands to generate-content tool

## 0.21.2 Release (March 2, 2022)

* Fix serialization issue

## 0.21.1 Release (March 2, 2022)

* (no user visible changes)

## 0.21.0 Release (March 1, 2022)

* Add tracing to database-adapter internals
* Introduce compatibility and upgrade tests
* Refactor StreamingUtil class
* Support for Spark 3.1 + 3.2.1 in Nessie SQL extensions
* Proper usage of commit-id in Spark SQL extensions
* Add DELETE_DEFAULT_BRANCH access check

## 0.20.1 Release (February 17, 2022)

* (no user visible changes)

## 0.20.0 Release (February 16, 2022)

* Enable metrics for MongoDB by default
* Make try-loop-state configurable and add metrics
* Reorganize routes in UI
* Improve error reporting in Nessie Java client
* Various test improvements

## 0.19.0 Release (February 7, 2022)

* Reads using "detached" commit-ids w/o specifying a branch or tag name
* Bump Nessie client version in Nessie Spark-Extensions
* Support for Iceberg views (experimental)
* Diff endpoint supports named-references + commit-on-reference as well
* Add filtering for ref-log
* Rework and simplification of the Nessie UI code

## 0.18.0 Release (January 13, 2022)

* Add reflog support
* Uses commit-timestamp "now" for merged/transplanted commits
* Add new reflog command to the CLI
* Add support for Python 3.10
* Drop support for Python 3.6

## 0.17.0 Release (December 08, 2021)

* Rename 'query_expression' query param to 'filter'
* Rename 'max' query param to 'maxRecords'
* Rename 'fetchAdditionalInfo' query param to 'fetch' for better extensibility

## 0.16.0 Release (December 03, 2021)

* Mark optional fields as @Nullable / add validation for required fields in param classes
* Add CEL-filter to get-all-references
* Fix NPE for unchanged operation for fetching commit log with additional metadata
* Allow CEL-filtering on optional operations in get-commit-log

## 0.15.1 Release (December 01, 2021)

* Fix wrongly placed validation annotation

## 0.15.0 Release (December 01, 2021)

* Enhance commit log to optionally return original commit operations
* Optionally return commits ahead/behind, HEAD commit-meta, commit count,
  common ancestor for named references
* Add missing REST endpoint to retrieve diff between two references
* Web UI improvements

## 0.14.0 Release (November 12, 2021)

* Updated `IcebergTable` to track more information
* UI dependencies cleanup
* OpenAPI/REST API cleanup (breaking change)

## 0.12.1 Release (November 3, 2021)

* Test code improvements
* Swagger examples fixes
* Web UI improvements
* Faster local builds w/ `./mvnw -Dquickly`

## 0.12.0 Release (October 25, 2021)

* Specialize and document Nessie exceptions
* Adopt Helm chart with new Nessie server settings
* Bump to GraalVM 21.3

## 0.11.0 Release (October 20, 2021)

* Various doc + site improvements
* Fix Nessie's representation of global and on-reference state (Iceberg tables)
* Fix CLI log -n option
* Spark SQL extension improvements

## 0.10.1 Release (October 8, 2021)

* Spark SQL extension changes
* Various (Open)API and client (Java, Python) implementation changes to prepare for better
  backwards compatibility.
* JUnit extension based test support against different database/store types and configurations
* Unified version-store implementations into a part w/ the Nessie logic and a tier implementing
  database access (MongoDB, DynamoDB, RocksDB, PostgreSQL).
* Remove JGit

## 0.9.2 Release (August 26, 2021)

* Cleanup & fixes to OpenAPI examples, for Swagger UI
* Update Deltalake client to use version 1.0.0
* Drop Deltalake support for Spark 2
* Remove Hive-Metastore bridge
* Preparations for backwards-compatible Client-API
* Spark SQL Extensions: Introduce `IF NOT EXISTS` for `CREATE BRANCH`/`CREATE TAG`
* Spark SQL Extensions: Updates to work with Iceberg 0.12.0

## 0.9.0 Release (August 9, 2021)

* Support for the upcoming Iceberg `0.12.0` release for both Spark 3.0 + 3.1
* Add docs for Nessie's metadata authorization
* Add SPI for Nessie authorization with Reference implementation
* Create Helm chart for Nessie

## 0.8.3 Release (July 19, 2021)

* Fix issue in spark sql extensions
* Python CLI: Fix ser/de of DeltaLakeTable when listing contents

## 0.8.2 Release (July 15, 2021)

* Add JAX-RS server implementation based on Glassfish/Jersey/Weld for integration testing
  in Iceberg
* REST-API change: only accept named-references
* REST-API change: support time-travel on named-references
* REST-API change: Server-side commit range filtering
* OpenAPI: more explicit constraints on parameters
* OpenAPI: include OpenAPI yaml+json files in nessie-model artifact
* Remove already deprecated methods from ContentsApi
* Commit-log filtering on all fields of CommitMeta
* Use "Common Expression Language" for commit-log and entries filtering
* Spark-extensions for Iceberg
* Prepare for multi-tenancy
* Gatling support + simulations
* Python CLI: Fix ser/de of DeltaLakeTable when listing contents

## 0.7.0 Release (June 15, 2021)

* Server-side filtering improvements for entries-listing and log-listing
* Distinguish between author & committer in the Python CLI
* Allow setting author when committing via Python CLI
* Loosen pins for client install on Python cli
* Fix edge case when merging using in memory + jgit stores
* Gradle plugin improvements
* (Development) change to Google Code Style, add spotless plugin
* (CI) Add OWASP Dependency Check

## 0.6.1 Release (May 25, 2021)

* Gradle plugin improvements

## 0.6.0 Release (May 12, 2021)

* TreeApi.createReference() + commitMultipleOperations() return commit information
* Iceberg GC actions and a process to execute GC algorithm

## 0.5.1 Release (April 9, 2021)

* Fix Gradle plugin (non-deterministic order of dependencies causing failures)
* Fix Web-UI

## 0.5.0 Release (April 8, 2021)

* Iceberg table GC support
* Consistency fixes under high load
* Breaking changes to the backend to support richer commit metadata and data types
* Performance, metrics and tracing improvements
* Gradle plugin improvement for incremental builds

## 0.4.0 Release (March 8, 2020)

* rename base package to org.projectnessie
* NessieClient is now an interface and some easier builders
* initial implementation of GC algorithm
* major refactor of tiered classes for better modularity and extensibility
* observability improvements including better DynamoDB metrics and opentracing support for the client

## 0.3.0 Release (December 30, 2020)

* 118 commits since 0.2.1
* Replace jax-rs client with one based on HttpURLConnection
* Update Quarkus to 1.10.5
* Improvements to Server including better UI routing, validation checks on inputs etc
* Various improvements to python client and cli. Including python3.9 support

## 0.2.1 Release (October 30, 2020)

* Fix missing dateutil requirement for pynessie install
* Address path discovery in Gradle plugin (for testing in external integrations)

## 0.2.0 Release (October 29, 2020)

* Update [Nessie CLI](../tools/cli.md) commands to better match `git` syntax
* Update [REST Apis](../develop/rest.md) to be more consistent and better
* Add support for merge & cherry-pick in DynamoDB storage backend
* Add [WebUI](../tools/ui.md)
* Introduce new DynamoDB optimizations to support faster log and entry retrieval
* Update to Quarkus 1.9.1
* Expose the new [Store interface](https://github.com/projectnessie/nessie/blob/main/versioned/dynamodb/src/main/java/org/projectnessie/versioned/store/Store.java) for low level storage implementations
* Introduce Quarkus Gradle runner plugin for easier third-party testing (e.g. Iceberg)
* Enable [swagger-ui](../tools/ui.md) by default in Nessie service

## 0.1.0 Release (October 1, 2020)

* Initial release
