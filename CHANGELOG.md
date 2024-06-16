# Nessie Changelog

The Nessie changelog is used to give users and contributors more information than just the list of commits.
Entries are grouped in sections like _Highlights_ or _Upgrade notes_, the provided sections can be adjusted
as necessary. Empty sections will not end in the release notes.

## [Unreleased]

### Highlights

### Upgrade notes

### Breaking changes

### New Features

### Changes

### Deprecations

### Fixes

### Commits

## [0.90.4] Release (2024-06-13)

### New Features

- Support for token exchange in the Nessie client has been completely redesigned. The new API and
  configuration options are described in the [Nessie authentication settings]. If this feature is
  enabled, each time a new access token is obtained, the client will exchange it for another one by
  performing a token exchange with the authorization server. We hope that this new feature will
  unlock many advanced use cases for Nessie users, such as impersonation and delegation. Please note
  that token exchange is considered in beta state and both the API and configuration options are
  subject to change at any time; we appreciate early feedback, comments and suggestions.

## [0.90.2] Release (2024-06-11)

### Highlights

- Nessie has got support for Iceberg REST.
- MySQL users can now configure the JDBC connection using the `quarkus.datasource.mysql.*`
  properties. Also, JDBC URLs can now use the `mysql` prefix, e.g.
  `jdbc:mysql://example.com:3306/my_db`.

### Upgrade notes

- Helm chart: the `cassandra` section now has support for pulling credentials from a secret. The
  old `cassandra.auth.username` and `cassandra.auth.password` properties still work, but are now
  deprecated and will be removed in a future release. Use the `cassandra.secret` property instead.

### New Features

- Support for Iceberg REST is in "beta" state. We appreciate early feedback, comments and suggestions.
  Take a look at the [Guides](http://projectnessie.org/guides/) and [Docs](http://projectnessie.org/docs/)
  on our web site projectnessie.org for more information.
- CEL access check scripts now receive the variable `roles` that can be used to check whether the current
  principal has a role assigned using a CEL expression like `'rolename' in roles`.

### Deprecations

- Support for Java 8 is officially deprecated and users are encouraged to upgrade all clients to
  at least Java 11, better Java 17 or 21, if possible. Current Spark versions 3.3, 3.4 and 3.5 
  work with Java 11 and 17. Support for Java 8 will eventually be removed.
- For JDBC version stores, the following settings, which never worked as expected, are now 
  deprecated and will be removed in a future release. The catalog and the schema must always be
  specified explicitly in the JDBC URL.
  - `nessie.version.store.persist.jdbc.catalog`
  - `nessie.version.store.persist.jdbc.schema`
- Support for token exchange in the Nessie client, in its current form, is now deprecated. The token
  exchange flow was being used to exchange a refresh token for an access token, but this is not its
  recommended usage. From now on, if a refresh token is provided by the authorization server, the
  Nessie client will use the `refresh_token` grant type to obtain a new access token; if a refresh
  token is not provided, the client will use the configured initial grant type to fetch a new access
  token. _This change should thus be transparent to all users._ The `token_exchange` flow will be
  redesigned in a future release to support more advanced use cases.

### Fixes

- A bug in the API compatibility filter has been discovered and fixed: when OAuth2 authentication is
  being used, the filter causes the OAuth2 client to close prematurely, thus triggering unauthorized
  errors. A workaround is to simply disable the filter (set `nessie.enable-api-compatibility-check` 
  to `false`), but this is no longer necessary.

## [0.83.2] Release (2024-05-23)

(Note: the 0.83.1 and 0.83.0 versions failed to fully release all artifacts for technical reasons.)

### Highlights

- New Nessie CLI tool + REPL, replacing the old Python based CLI, based on Java.
  SQL-ish syntax, built-in online `HELP` command, auto-completion of commands, keywords
  and reference names, syntax highlighting, paging of long results, command history.
- Nessie now includes built-in support for MariaDB, with full compatibility with MySQL servers. New
  users wishing to try MariaDB (or MySQL) should:
  1. Specify the new configuration property: `nessie.version.store.persist.jdbc.datasource=mariadb`;
  2. Provide all the MariaDB (or MySQL) connection details using `quarkus.datasource.mariadb.*`
     configuration properties.
- The Nessie GC tool is now also compatible with MariaDB and MySQL (using the MariaDB connector).
- The Nessie Server Admin tool is now also compatible with MariaDB and MySQL (using the MariaDB
  connector).

### Upgrade notes

- Due to the newly-introduced support for MariaDB, existing PostgreSQL users can continue to use
  their current JDBC configuration, but are encouraged to update it as follows:
  1. Specify the new configuration property: 
     `nessie.version.store.persist.jdbc.datasource=postgresql`;
  2. Migrate any property under `quarkus.datasource.*` to `quarkus.datasource.postgresql.*`. Support
     for the old `quarkus.datasource.*` properties will be removed in a future release.
- For the same reason, the Nessie Helm chart has been updated. The old `postgres` section is now
  called `jdbc`. Existing Helm chart configurations should be updated accordingly, e.g.
  `postgres.jdbcUrl` now becomes `jdbc.jdbcUrl`. Although the old `postgres` section is still
  honored, it won't be supported in future releases. The right datasource will be chosen based on
  the `jdbcUrl` contents.

### Breaking changes

- `nessie-quarkus-cli`, the low-level tool to for example export/import Nessie repositories, has been renamed
  to `nessie-server-admin-tool`.

### New Features

- More verbose exceptions from Nessie GC.

## [0.82.0] Release (2024-05-06)

### Breaking changes

- The readiness, liveness, metrics and Swagger-UI endpoints starting with `/q/` have been moved from the
  HTTP port exposing the REST endpoints to a different HTTP port (9000),
  see [Quarkus management interface reference docs](https://quarkus.io/guides/management-interface-reference).
  Any path starting with `/q/` can be safely removed from a possibly customized configuration
  `nessie.server.authentication.anonymous-paths`.
- The move of the above endpoints to the management port requires using the Nessie Helm chart for this
  release or a newer release. Also, the Helm Chart for this release will not work with older Nessie
  releases.

## [0.81.1] Release (2024-05-03)

### Fixes

- GC: Fix handling of quoted column names in Iceberg

## [0.81.0] Release (2024-05-01)

### Highlights

- The Nessie client now supports public clients when using OAuth 2 authentication. Public clients 
  are clients that do not have a client secret; they are compatible with the `password`, 
  `authorization_code`, and `device_code` grant types. See the
  [Nessie documentation](https://projectnessie.org/tools/client_config/#authentication-settings) 
  for details.

### New Features

- The Nessie Helm chart now supports AWS profiles. There are now two ways to configure AWS 
  credentials in the Helm chart:
  - Using a secret. The secret name can be set in the `dynamodb.secret` value.
  - Using an AWS profile (new). The profile name can be set in the `dynamodb.profile` value.

## [0.80.0] Release (2024-04-21)

### Upgrade notes

- GC Tool: the Nessie GC tool is not published anymore as a Unix executable file, but as a regular 
  jar named `nessie-gc.jar`. Instead of running `nessie-gc`, you now run `java -jar nessie-gc.jar`.

### New Features

- Nessie clients can now use the Apache HTTP client, if it's available on the classpath
- Nessie clients now return more readable error messages

### Fixes

- Helm chart: Fix incorrect OpenTelemetry variable names
- SQL extensions: Respect comments in SQL
- GC tool: perform commit after schema creation

## [0.79.0] Release (2024-03-12)

### New Features

- Iceberg bumped to 1.5.0

### Changes

- SQL extensions for Spark 3.2 have been removed
- Experimental iceberg-views module has been removed

## [0.78.0] Release (2024-03-07)

### New Features

- GC Tool: ability to skip creating existing tables (IF NOT EXISTS)
- Make `Authorizer` pluggable
- Helm chart: add option to set sessionAffinity on Service

### Fixes

- Handle re-added keys when creating squash commits
- JDBC backend: infer catalog and schema if not specified

## [0.77.0] Release (2024-02-14)

### Highlights

- The Nessie GC tool is now published as a Docker image. See the [GC Tool documentation
  page](https://projectnessie.org/features/gc) for more.
- Remove synchronizing to docker.io container registry, only publish to ghcr.io and quay.io.

### Upgrade notes

- Projectnessie no longer publishes container images to docker.io/Docker Hub. Container images are
  available from ghcr.io and quay.io.

### New Features

- Add some configuration checks to highlight probably production issues
- Publish Docker images for the GC tool

### Changes

- Disable default OIDC tenant when authentication is disabled
- Disable OpenTelemetry SDK when no endpoint is defined

### Fixes

- Fix VersionStore panels of the Grafana dashboard

## [0.76.4] Release (2024-01-26)

### Changes

- Nessie Docker images now contain Java 21
- Helm: Make ingressClassName configurable
- Helm: Use auto scaling
- Improve error message when JDBC/C* columns are missing

## [0.76.0] Release (2024-01-02)

### Highlights

- The Nessie client supports two new authentication flows when using OAuth 2 authentication:
  the Authorization Code flow and the Device Code flow. These flows are well suited for use within 
  a command line program, such as a Spark SQL shell, where a user is interacting with Nessie using a
  terminal. In these flows, the user must use their web browser to authenticate with the identity
  provider. See the 
  [Nessie documentation](https://projectnessie.org/tools/client_config/#authentication-settings) 
  for details. The two new flows are enabled by the following new grant types:
  - `authorization_code`: enables the Authorization Code flow; this flow can only be used with
    a local shell session running on the user's machine.
  - `device_code`: enables the Device Code flow; this flow can be used with either a local or a 
    remote shell session. 
- The Nessie client now supports endpoint discovery when using OAuth 2 authentication. If an 
  identity provider supports the OpenID Connect Discovery mechanism, the Nessie client can be 
  configured to use it to discover the OAuth 2 endpoints. See the 
  [Nessie documentation](https://projectnessie.org/tools/client_config/#authentication-settings) 
  for details.

### New Features

- Nessie client: the OAUTH2 authentication provider now supports programmatic configuration. See the 
  [Nessie documentation](https://projectnessie.org/develop/java/#authentication) for details.

### Fixes

- Fix potential NPE when fetching commit log with fetch option `ALL` and access checks enabled.

## [0.75.0] Release (2023-12-15)

### Upgrade notes

- Nessie Quarkus parts are now built against Java 17 and Java 17 is required to run Nessie Quarkus Server directly.
  If you use the Docker image, nothing needs to be done, because the image already contains a compatible Java runtime.
- Due to the introduction of new object types in the storage layer, some storage backends
  will require a schema upgrade:
  - JDBC: the following SQL statement must be executed on the Nessie database (please adapt the
    statement to the actual database SQL dialect):
    ```sql
    ALTER TABLE objs 
      ADD COLUMN x_class VARCHAR,
      ADD COLUMN x_data BYTEA,
      ADD COLUMN x_compress VARCHAR,
      ADD COLUMN u_space VARCHAR,
      ADD COLUMN u_value BYTEA;
    ```
  - Cassandra: the following CQL statement must be executed on the Nessie database and keyspace:
    ```cql
    ALTER TABLE <keyspace>.objs 
      ADD x_class text, 
      ADD x_data blob,
      ADD x_compress text,
      ADD u_space text,
      ADD u_value blob;
    ```
- When using one of the legacy and deprecated version-store implementations based on "database adapter",
  make sure to migrate to the new storage model **before** upgrading to this version or newer Nessie
  versions.

### Breaking changes

- The deprecated version-store implementations based on "database datapter" have been removed from the
  code base.

## [0.74.0] Release (2023-11-21)

### New Features

- Nessie-GC: Support Google Cloud Storage (GCS) (experimental)
- Nessie-GC: Support Azure Blob Storage (experimental)

### Fixes

- Add namespace validation for rename operation.
- Namespace validation now correctly reports only one conflict when deleting a namespace that has
  children, whereas previously it reported one conflict for each child.

## [0.73.0] Release (2023-10-27)

### Highlights

- Nessie API spec was upgraded to 2.1.3. The only change is that when a commit attempts to create a content
  inside a non-existing namespace, the server will not only return a `NAMESPACE_ABSENT` conflict for the
  non-existing namespace itself, but will also return additional `NAMESPACE_ABSENT` conflicts for all the
  non-existing ancestor namespaces.

### New Features

- Nessie client: the OAuth2 authentication provider is now able to recover from transient failures when
  refreshing the access token.

## [0.72.4] Release (2023-10-24)

### Fixes

- Docker images again honor environment variables such as `JAVA_OPTS_APPEND` that are used to pass
  additional JVM options to the Nessie server. See the 
  [ubi8/openjdk-17](https://catalog.redhat.com/software/containers/ubi8/openjdk-17/618bdbf34ae3739687568813)
  base image documentation for the list of all supported environment variables.

## [0.72.2] Release (2023-10-19)

### New Features

- Exposes object cache metrics, meters use the tags `application="Nessie",cache="nessie-objects"`

### Fixes

- Fix incorrectly calculated object cache maximum weight.

## [0.72.0] Release (2023-10-13)

### New Features

- Spark SQL extensions now support the `DROP ... IF EXISTS` syntax for branches and tags.
- `table-prefix` configuration option added to DynamoDB version store.
- Ability to export repositories in V1 format. This is useful for migrating repositories to older 
  Nessie servers that do not support the new storage model.
- Added support for Spark 3.5, removed support for Spark 3.1 - along with the version bump of Apache Iceberg to 1.4.0.
- Functionality that records current-HEAD changes of named references and APIs to expose the information.
  This is useful to recover from a scenario when a "primary data center/region/zone" has been lost and
  replication of a distributed database has been interrupted.

### Changes

- Introduces sizing of the Nessie object cache using a relative value of the max Java heap size.
  The defaults have been changed to 70% of the Java max heap size (from the previous default of 64MB).
  If a fixed cache size setting has been explicitly configured, consider to change it to the fraction based one.
- Relative hashes are now supported in table references, thus allowing SQL queries to specify a relative hash
  in the `FROM` clause, e.g. `FROM table1@main#1234^1`.
- BigTable backend: ability to disable telemetry (which is enabled by default).
- Spark SQL extensions use Nessie API V2 now.
- DynamoDB backend now supports table prefixes.
- Advanced configuration options for BigTable backend.

### Fixes

- Quarkus 3.4.3 includes a Netty version bump to address [CVE-2023-44487](https://github.com/advisories/GHSA-qppj-fm5r-hxr3) (HTTP/2 rapid reset). Note: Nessie uses undertow only for testing purposes, so the undertow release used in Nessie does _not_ expose this CVE to users.

## [0.71.0] Release (2023-09-21)

### Changes

- Configuration of the `NessieClientBuilder` now uses system properties, environment, `~/.config/nessie/nessie-client.properties` and `~/.env`

### Deprecations

- `HttpClientBuilder` class has been deprecated for removal, use `NessieClientBuilder` instead.

## [0.70.2] Release (2023-09-12)

### Fixes

- Fix wrong `New value for key 'some-key' must not have a content ID` when swapping tables.

## [0.70.1] Release (2023-09-12)

### Changes

- Content Generator tool: added new `--limit` parameter to `commits`, `references` and `entries` 
  commands.
- Content Generator tool: tool now prints the total number of elements returned when running the 
  `commits`, `references` and `entries` commands.
- Helm charts: OpenTelemetry SDK is now completely disabled when tracing is disabled.
- Helm charts: when auth is disabled, Quarkus OIDC doesn't print warnings anymore during startup.

### Fixes

- GC: Handle delete manifests and row level delete files

## [0.70.0] Release (2023-08-31)

### New Features

- Content Generator tool: added new `--hash` parameter to `commits`, `content` and `entries` 
  commands.

### Changes

- Content Generator tool: commit hashes are now printed in full when running the `commits` command.
- For a "get-keys" operation that requests the content objects as well, the content objects are now
  fetched using bulk-requests.

### Fixes

- Fixed potential index corruption when importing repositories with many keys into the new storage 
  model that could cause some contents to become inaccessible.

## [0.69.0] Release (2023-08-25)

### Fixes

- Nessie CLI: check-content command was incorrectly reporting deleted keys as missing content, when
  using new storage model.
- GC Tool handles JDBC config via environment correctly

## [0.68.0] Release (2023-08-24)

### Upgrade notes

- If a repository has been imported using Nessie CLI 0.68.0 or higher, then this repo cannot be
  later served by a Nessie server whose version is lower than 0.68.0. This is due to a change in the
  internal repository description format.

### New Features

- Support BigTable in Helm charts
- NessieCLI check-content command is now compatible with Nessie's new storage model

### Changes

- Java client API to assign/delete reference operations without specifying a concrete reference type
  (no change to REST API).
- Creating and assigning references now requires a target hash to be specified.

### Fixes

- Secondary commit parents are now properly exported and imported
- Fix volume declarations for RocksDB in Helm
- Remove unnecessary repository-deletion when importing a legacy Nessie repo
- GC Tool uber-jar now includes AWS STS classes
- GC Tool now logs at `INFO` instead of `DEBUG`
- GC Tool now correctly works against PostgreSQL

## [0.67.0] Release (2023-08-02)

### Upgrade notes

- Tracing and metrics have been migrated to Quarkus "native". The options to en/disable metrics and tracing have been removed. Please remove the options `nessie.version.store.trace.enable`, `nessie.version.store.metrics.enable` from your Nessie settings.

### Changes

- Nessie API spec upgraded to 2.1.1
- Support for relative hashes has been standardized and is now allowed in all v2 endpoints
- Migrate to Quarkus metrics and tracing

## [0.66.0] Release (2023-07-31)

### New Features

- New `entries` command in Content-Generator tool
- New `--all` option to the `content-refresh` Content-Generator tool command
- Helm chart: add `podLabels` for Nessie Pods

### Changes

- Add/fix `info` section in OpenAPI spec, add templates to `servers` section

### Fixes

- Fix handling of not present and wrong reference-type for create/assign/delete-reference API calls

## [0.65.1] Release (2023-07-19)

### Changes

- Add validation of cutoff-definitions in `GarbageCollectorConfig`
- Fix self-reference in OpenAPI spec
- Add `servers` section to OpenAPI spec

## [0.65.0] Release (2023-06-14)

- Revert Gradle 8.2.1 (#7239)
- Add Nessie as a Source announcement blog from Dremio website (#7236)
- Add `--author` option to `content-generator` commands (#7232)
- Add repository configuration objects (#7233)
- Fix retrieval of default branch (#7227)
- Allow re-adds in same commit (#7225)
- Allow snapshot versions in dependencies (#7224)
- IDE: Cleanup Idea excludes (#7223)
- Spark-tests: disable UI (#7222)
- Compatibility tests: move to new storage model (#6910)
- Use testcontainers-bom (#7216)
- Reference keycloak-admin-client-jakarta (#7215)
- Post Quarkus 3: Remove no longer needed dependency exclusion (#7214)
- Bump to Quarkus 3.2.0.Final (#6146)
- CI: Add some missing `--scan` Gradle flags (#7210)
- Update main README after UI sources moved (#7207)
- Forbid relative hashes in commits, merges and transplants (#7193)
- Remove misplaced license header (#7203)
- More diff-tests (#7192)
- removed extra tab (#7189)
- Tests: Make `ITCassandraBackendFactory` less flaky (#7186)
- IntelliJ: Exclude some more directories from indexing (#7181)

[Unreleased]: https://github.com/projectnessie/nessie/compare/nessie-0.90.4...HEAD
[0.90.4]: https://github.com/projectnessie/nessie/compare/nessie-0.90.2...nessie-0.90.4
[0.90.2]: https://github.com/projectnessie/nessie/compare/nessie-0.83.2...nessie-0.90.2
[0.83.2]: https://github.com/projectnessie/nessie/compare/nessie-0.82.0...nessie-0.83.2
[0.82.0]: https://github.com/projectnessie/nessie/compare/nessie-0.81.1...nessie-0.82.0
[0.81.1]: https://github.com/projectnessie/nessie/compare/nessie-0.81.0...nessie-0.81.1
[0.81.0]: https://github.com/projectnessie/nessie/compare/nessie-0.80.0...nessie-0.81.0
[0.80.0]: https://github.com/projectnessie/nessie/compare/nessie-0.79.0...nessie-0.80.0
[0.79.0]: https://github.com/projectnessie/nessie/compare/nessie-0.78.0...nessie-0.79.0
[0.78.0]: https://github.com/projectnessie/nessie/compare/nessie-0.77.0...nessie-0.78.0
[0.77.0]: https://github.com/projectnessie/nessie/compare/nessie-0.76.4...nessie-0.77.0
[0.76.4]: https://github.com/projectnessie/nessie/compare/nessie-0.76.0...nessie-0.76.4
[0.76.0]: https://github.com/projectnessie/nessie/compare/nessie-0.75.0...nessie-0.76.0
[0.75.0]: https://github.com/projectnessie/nessie/compare/nessie-0.74.0...nessie-0.75.0
[0.74.0]: https://github.com/projectnessie/nessie/compare/nessie-0.73.0...nessie-0.74.0
[0.73.0]: https://github.com/projectnessie/nessie/compare/nessie-0.72.4...nessie-0.73.0
[0.72.4]: https://github.com/projectnessie/nessie/compare/nessie-0.72.2...nessie-0.72.4
[0.72.2]: https://github.com/projectnessie/nessie/compare/nessie-0.72.0...nessie-0.72.2
[0.72.0]: https://github.com/projectnessie/nessie/compare/nessie-0.71.0...nessie-0.72.0
[0.71.0]: https://github.com/projectnessie/nessie/compare/nessie-0.70.2...nessie-0.71.0
[0.70.2]: https://github.com/projectnessie/nessie/compare/nessie-0.70.1...nessie-0.70.2
[0.70.1]: https://github.com/projectnessie/nessie/compare/nessie-0.70.0...nessie-0.70.1
[0.70.0]: https://github.com/projectnessie/nessie/compare/nessie-0.69.0...nessie-0.70.0
[0.69.0]: https://github.com/projectnessie/nessie/compare/nessie-0.68.0...nessie-0.69.0
[0.68.0]: https://github.com/projectnessie/nessie/compare/nessie-0.67.0...nessie-0.68.0
[0.67.0]: https://github.com/projectnessie/nessie/compare/nessie-0.66.0...nessie-0.67.0
[0.66.0]: https://github.com/projectnessie/nessie/compare/nessie-0.65.1...nessie-0.66.0
[0.65.1]: https://github.com/projectnessie/nessie/compare/nessie-0.65.0...nessie-0.65.1
[0.65.0]: https://github.com/projectnessie/nessie/compare/nessie-0.64.0...nessie-0.65.0
[Nessie authentication settings]: https://projectnessie.org/tools/client_config/#authentication-settings
