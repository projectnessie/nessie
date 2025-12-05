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

## [0.106.0] Release (2025-12-05)

### Fixes

- Catalog/S3: Add a per-bucket configuration option `chunked-encoding-enabled` (defaults to `true`)
  so deployments targeting Oracle Cloud Infrastructure (OCI) or other S3-compatible stores that
  reject chunked payload signatures can disable AWS SDK chunked encoding without downgrading
  Nessie. Fixes [#11441](https://github.com/projectnessie/nessie/issues/11441).

## [0.105.7] Release (2025-11-06)

### Fixes

- Fixes an issue when defining a nested schema field as an identifier field of an Iceberg schema.

## [0.105.6] Release (2025-10-24)

### Fixes

- Fixes an issue where Iceberg batch deletions did not work with Nessie remote S3 signing.

## [0.105.3] Release (2025-09-24)

### Fixes

- Iceberg REST: adapt the deprecation of `lastColumnId` in `AddSchema` table metadata update, fiel field is optional now

## [0.105.2] Release (2025-09-19)

### Highlights

- Nessie UI: CSS + fonts are now fetched from Nessie instead of external sources (CDNs).

## [0.105.1] Release (2025-09-16)

### Highlights

- Bump Iceberg from version 1.9.2 to 1.10.0

## [0.105.0] Release (2025-09-03)

### Fixes

- A bug has been resolved in the OAuth2 Authorization Code grant type. This fix addresses an issue
  where the `extra-params` configuration option was not being properly included in the authorization
  URI. Consequently, users, particularly Auth0 users passing the `audience` parameter, were receiving
  opaque tokens instead of JWTs.

## [0.104.2] Release (2025-06-12)

### Changes

- JDBC: Previously, the JDBC backends and Nessie GC left the fetch-size to its default, which is to fetch
  all rows when a `SELECT` statement is executed. This is changed to default to a fetch-size of 100 rows.
  To revert to the old behavior, set the corresponding configuration option to `0` (not recommended).
  A corresponding, new command line option for Nessie GC has been introduced as well.

## [0.103.6] Release (2025-05-01)

### Changes

- OpenAPI specs are no longer published to swaggerhub.

## [0.103.5] Release (2025-04-26)

### New Features

- Add PDB support to helm chart

## [0.103.4] Release (2025-04-24)

### Changes

- Change default of `nessie.version.store.persist.cache-enable-soft-references` to `false`

## [0.103.3] Release (2025-04-08)

### New Features

- Introduces a hard objects-cache capacity limit to ensure that the cache does never consume more than
  the configured cache-capacity plus a configurable "overshoot" (defaults to 10%). New cache entries are
  admitted as long as the current cache size is less than the "cache-capacity + overshoot".

### Changes

- Nessie's REST API endpoints now accept "truncated timestamps" in relative-commit-specs, aka without the
  second-fraction.

## [0.103.2] Release (2025-03-21)

### New Features

- Catalog/S3/request-signing: Add a per-S3-bucket config option `url-signing-expire` to override the default
  3-hour lifetime of S3-URL-signing URLs.

## [0.103.1] Release (2025-03-18)

### Highlights

- Configuration option `nessie.version.store.persist.cache-enable-soft-references` defaults to 
  `false` now. Some feedback suggests that using soft references in the Nessie cache may not be
  optimal with respect to GC overhead in some environments, so defaulting to `false` is safer.

### New Features

- Helm: Allow annotations on the configmap

### Fixes

- Catalog: Return consistent metadata-location for Iceberg REST APIs

## [0.103.0] Release (2025-02-18)

### Highlights

- If you are using Iceberg/Java 1.8.0 it is STRONGLY RECOMMENDED to upgrade to this or a newer Nessie release!

### Upgrade notes

- This Nessie version is compatible with Iceberg/Java version 1.8.0 via Iceberg REST.
  Iceberg 1.8.0 contains changes that breaks compatibility with previous Nessie versions!
- Iceberg table spec v3 is not supported in Nessie, because it is still under active development.

### Changes

- Dependencies that are only licensed using GPL+CE are no longer included in Nessie CLI, server and admin tool.

## [0.102.5] Release (2025-02-05)

### Note

- This release has no code changes.
- NOTICE and LICENSE files clarifications, included in jars published to Maven Central.

## [0.102.4] Release (2025-01-31)

### Note

- This release has no code changes except NOTICE file(s) clarifications.

## [0.102.3] Release (2025-01-30)

### New Features

- Catalog: Iceberg table configurations overrides are now available in storage configuration settings.
  Example: `nessie.catalog.service.s3.default-options.table-config-overrides.py-io-impl=pyiceberg.io.pyarrow.PyArrowFileIO`

## [0.102.2] Release (2025-01-23)

### Fixes

- Nessie re-assigns IDs for new schemas/partition-specs/sort-orders. The check that the provided ID for
  those must be valid (>= 0) is therefore superfluous, it can actually unnecessarily lead to problems. This
  change also fixes an issue that the last-added schema/spec/sort ID is set to -1, if the schema/spec/sort
  already existed. This lets the set-current-schema/set-default-partition-spec/set-default-sort-order
  updates with `-1` for the last-added one fail, but it should return the ID of the schema/spec/sort ID that
  already existed.

## [0.102.1] Release (2025-01-22)

### Fixes

- Catalog/ADLS: Fix an issue that prevented the usage of retry-options for ADLS.

## [0.102.0] Release (2025-01-21)

### New Features

- When using OAuth authentication, the Nessie client now supports including extra parameters in
  requests to the token endpoint. This is useful for passing custom parameters that are not covered
  by the standard OAuth 2.0 specification. See the [Nessie
  documentation](https://projectnessie.org/tools/client_config/#authentication-settings) for
  details.
- Add a configuration option `nessie.version.store.persist.cache-enable-soft-references` (defaults to 
  `true`) to optionally disable the additional caching the constructed Java objects via soft references.
  Having the already constructed Java object is faster when getting object from the cache, but a Java object
  tree implies a rather unpredictable heap pressure, hence these object are referenced via Java soft
  references. This optimization however can cause heap issues in rare scenarios, and disabling this
  optimization can help there.

### Fixes

- Fix an issue that prevents the Nessie Server Admin tool to purge unreferenced data in the backend
  database, for data being written before Nessie version 0.101.0.
- Fix an issue that prevents using nested fields in partition-spec and sort-order.
  Given a schema having a `struct < field_a, field_b >`, it was not possible to reference
  `field_a` or `field_b` in a partition-spec or sort-order. There was no issue however using fields
  at the "top level" (a schema like `field_a, field_b`).

## [0.101.3] Release (2024-12-18)

### New Features

- Add the `cut-history` command to the Nessie server admin tool. This command allows advanced users to 
  detach certain commits from their predecessors (direct and merge parents).

## [0.101.2] Release (2024-12-12)

### Fixes

- Fix large index processing in the `cleanup-repository` admin command.

## [0.101.1] Release (2024-12-09)

### Fixes

- Fix handling of Iceberg update-requirement "no current snapshot"

## [0.101.0] Release (2024-12-06)

### New Features

- Helm: Add clusterIP and traffic policy to helm service config
- Add functionality to the Nessie server admin tool, the `cleanup-repository` command, to delete
  unneeded objects from a Nessie repository (backend database).

## [0.100.3] Release (2024-12-02)

### New Features

- Add `deploymentStrategy` to Helm deployment configs

### Fixes

- Allow multiple `SetProperties` updates via Iceberg REST.

## [0.100.1] Release (2024-11-19)

### Highlights

- Export: ZIP file exports were broken in all Nessie versions from 0.92.1 until 0.100.0.
  If you are using any of these versions, you must not use ZIP export mode, but use the
  file (directory) based exporter (`--output-format=DIRECTORY`)!

### Fixes

- Export: ZIP file exports are fixed with this Nessie version 0.100.1.

## [0.100.0] Release (2024-11-12)

### Upgrade notes

- Helm chart: the old `logLevel` field has been replaced with a new `log` section with many more
  options to configure logging. You can now configure console- and file-based logging separately. It
  is also possible to enable JSON logging instead of plain text (but this feature requires Nessie >=
  0.99.1). For file-based logging, it is also possible to configure rotation and retention policies,
  and a persistent volume claim is now automatically created when file-based logging is enabled.
  Furthermore, Sentry integration can also be enabled and configured. And finally, it is now
  possible to configure the log level for specific loggers, not just the root logger. The old
  `logLevel` field is still supported, but will be removed in a future release.

### Changes

- The persistence cache tries to avoid deserialization overhead when getting an object from the
  cache by using Java's `SoftReference`. There is no guarantee that cached objects keep their
  Java object tree around, but it should eventually for the majority of accesses to frequently
  accessed cached objects. The default cache capacity fraction has been reduced from 70% of the
  heap size to 60% of the heap size. However, extreme heap pressure may let Java GC clear all
  `SoftReference`s.
- Sends the following default options, which are convenient when using pyiceberg:
  * `py-io-impl=pyiceberg.io.fsspec.FsspecFileIO`
  * `s3.signer=S3V4RestSigner` when S3 signing is being used
- Iceberg REST: No longer return `*FileIO` options from the Iceberg REST config endpoint

### Fixes

- GC: Consider referenced statistics (and partition statistics) files as 'live'.
- JDBC: Perform JDBC commit when auto-creating tables to please transactional schema changes.

## [0.99.0] Release (2024-09-26)

### Breaking changes

- The Events API has been redesigned to import the Nessie Model API directly, instead of using
  specific DTO classes. This change is intended to simplify the API and facilitate consumption of
  the events. The following classes from the `org.projectnessie.events.api` package have been
  removed and replaced with their respective model classes from the `org.projectnessie.model`
  package:
    - `CommitMeta`
    - `Content` and its subclasses
    - `ContentKey`
    - `Reference` and its subclasses
- Helm chart: the `service` section has been redesigned to allow for extra services to be defined.
  If you have customized the `service.ports` field, beware that this field is now an array. Also,
  the management port configuration has been moved to a new `managementService` section. And
  finally, a new `extraServices` section has been added to allow for additional services to be
  defined.
- ADLS: The way how storage URIs are resolved to ADLS "buckets" (container @ storage-account) has been
  changed (fixed). An ADLS "bucket" is technically identified by the storage-account, optionally further
  identified by a container/file-system name. It is recommended to specify the newly added via the
  `nessie.catalog.service.adls.file-systems.<key>.authority=container@storageAccount` option(s).
  The `container@storageAccount` part is what is mentioned as `<file_system>@<account_name>` in the [Azure
  docs](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri).

### New Features

- Access check SPI has been enhanced to provide richer information in the `Check` type about the receiving
  API (Nessie REST or Iceberg REST) and about the individual changes, especially during a commit operation.

### Changes

- S3/GCS/ADLS: Bucket settings
  - The resolution of the specific bucket options has been enhanced to select the specific bucket options
    using the longest matching option including an optional path-prefix.
  - All bucket specific options (`nessie.catalog.service.adls.buckets.<key>.`,
    `nessie.catalog.service.gcs.buckets.<key>.`, `nessie.catalog.service.adls.file-systems.<key>.`) got a
    new option `path-prefix`, which is used to restrict settings to a specific object store path prefix.
  - All bucket specific options (`nessie.catalog.service.adls.buckets.<key>.`,
    `nessie.catalog.service.gcs.buckets.<key>.`, `nessie.catalog.service.adls.file-systems.<key>.`) got a
    new option `authority`, which is recommended to specify the technical bucket name. If `authority` is
    not specified, it will default to the value of the `name` option, then default to the `key` part of the
    formerly mentioned maps.
- The base `location` of a new entity (e.g. tables) created via Iceberg REST is derived from the nearest
  parent namespace that has an explicitly set `location` property. (Path separator character is `/`.)
- The `location` property on tables (and view) created via Iceberg REST may be explicitly configured, as
  long as it can be resolved against the configured object storage locations. (Path separator character
  is `/`.)

### Fixes

- CLI: Fix connecting to Nessie's Iceberg REST

## [0.97.1] Release (2024-09-19)

### Highlights

- Alert: If you are using MySQL or MariaDB, make sure to update `objs` table immediately:
  ```sql
  ALTER TABLE objs MODIFY c_headers LONGBLOB;
  ALTER TABLE objs MODIFY c_incremental_index LONGBLOB;
  ALTER TABLE objs MODIFY c_reference_index_stripes LONGBLOB;
  ALTER TABLE objs MODIFY i_index LONGBLOB;
  ALTER TABLE objs MODIFY i_stripes LONGBLOB;
  ALTER TABLE objs MODIFY s_text LONGBLOB;
  ALTER TABLE objs MODIFY t_headers LONGBLOB;
  ALTER TABLE objs MODIFY t_signature LONGBLOB;
  ALTER TABLE objs MODIFY u_value LONGBLOB;
  ALTER TABLE objs MODIFY v_data LONGBLOB;
  ALTER TABLE objs MODIFY x_data LONGBLOB;
  ```

### Fixes

- MySQL: Change type of binary columns from `BLOB` to `LONGBLOB`.

## [0.96.1] Release (2024-09-12)

### New Features

- Helm chart: support has been added for the `DYNAMODB2`, `MONGODB2`, `CASSANDRA2`, and `JDBC2`
  version store types, introduced in Nessie 0.96.0. Also, support for legacy version store types
  based on the old "database adapter" code, which were removed in Nessie 0.75.0, has also been
  removed from the Helm chart.

### Fixes

- Helm chart: fixed a regression where a datasource secret would result in a failure to deploy the
  chart.

## [0.96.0] Release (2024-09-11)

### Upgrade notes

- Support for Java 8 has been removed, even for Nessie clients. Minimum runtime requirement for clients
  is Java 11.
- Nessie Docker images now all execute as user `nessie` (UID 10000 and GID 10001). They would
  previously execute as user `default` (UID 185 and GID 0). This is a security improvement, as the
  Nessie images no longer run with a UID within the privileged range, and the GID is no longer 0
  (root). If you have any custom configurations, especially Kubernetes manifests containing security
  contexts, that rely on the previous user `default` (UID 185 and GID 0), you will need to adjust
  them to reference the new user `nessie` (UID 10000 and GID 10001) from now on.
- Helm chart: the chart now comes with sane defaults for both pod and container security contexts.
  If you have customized these settings, you don't need to do anything. If you have not customized these
  settings, you may need to check if the new defaults are compatible with your environment.

### Breaking changes

- The deprecated JDBC configuration properties for `catalog` and `schema` have been removed.
- Catalog/Object store secrets: Secrets are now referenced via a URN as requirement to introduce support
  for secret managers like Vault or those offered by cloud vendors. All secret reference URNs use the
  pattern `urn:nessie-secret:<provider>:<secret-name>`.
  The currently supported provider is `quarkus`, the `<secret-name>` is the name of the Quarkus
  configuration entry, which can also be an environment variable name.
  Make sure to use the new helm chart.
  See [Nessie Docs](https://projectnessie.org/nessie-latest/configuration/#secrets-manager-settings).
- Catalog/Object store secrets: secrets are now handled as immutable composites, which is important
  to support secrets rotation with external secrets managers.
  See [Nessie Docs](https://projectnessie.org/nessie-latest/configuration/#secrets-manager-settings).

### New Features

- Catalog/ADLS: Added **experimental** support for short-lived SAS tokens passed down to clients. Those
  tokens still have read/write access to the whole file system and are **not** scoped down.
- Catalog/GCS: Added **experimental** support for short-lived and scoped down access tokens passed down
  to clients, providing a similar functionality as vended-credentials for S3, including object-storage
  file layout.
- Client-configs: Commit authors, signed-off-by, message can be customized per REST/HTTP request. Those
  can be configured for both the [Nessie client API](https://projectnessie.org/nessie-latest/client_config/)
  and for [Iceberg REST catalog clients](https://projectnessie.org/guides/iceberg-rest/).
- Support for Servlet Spec v6 w/ strict URI path validation has been added and will be transparently
  used by Nessie REST API v2 clients since this version. This steps is a preparation for when Quarkus
  introduces that Servlet Spec. Content keys in URL paths may look different than before. More information
  [here](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md#content-key-and-namespace-string-representation-in-uri-paths).
- The Swagger UI and OpenAPI generation by Quarkus has been disabled, because the contents/results were
  wrong. Instead, refer to [SwaggerHub](https://app.swaggerhub.com/apis/projectnessie/nessie). You can
  also fetch the Nessie REST OpenAPI yaml from Nessie `/nessie-openapi/openapi.yaml` (for example via
  `curl http://127.0.0.1:19120//nessie-openapi/openapi.yaml`)
- Nessie commit author(s) and "signed off by" can now be configured for both Nessie clients and Iceberg
  REST clients. More info on
  [projectnessie.org](https://projectnessie.org/guides/iceberg-rest/#customizing-nessie-commit-author-et-al). 
- Enable authentication for the Nessie Web UI
- Introduce new `JDBC2` version store type, which is has the same functionality as the `JDBC` version
  store type, but uses way less columns, which reduces storage overhead for example in PostgreSQL a lot.
- Introduce new `CASSANDRA2` version store type, which is has the same functionality as the `CASSANDRA` version
  store type, but uses way less attributes, which reduces storage overhead.
- Introduce new `DYNAMODB2` version store type, which is has the same functionality as the `DYNAMODB` version
  store type, but uses way less attributes, which reduces storage overhead.
- Introduce new `MONGODB2` version store type, which is has the same functionality as the `MONGODB` version
  store type, but uses way less attributes, which reduces storage overhead.
- Added functionality to optionally validate that referenced secrets can be resolved, opt-in.

### Deprecations

- The current version store type `JDBC` is deprecated, please migrate to the new `JDBC2` version store
  type. Please use the [Nessie Server Admin Tool](https://projectnessie.org/nessie-latest/export_import)
  to migrate from the `JDBC` version store type to `JDBC2`.
- The current version store type `CASSANDRA` is deprecated, please migrate to the new `CASSANDRA2` version store
  type. Please use the [Nessie Server Admin Tool](https://projectnessie.org/nessie-latest/export_import)
  to migrate from the `CASSANDRA` version store type to `CASSANDRA2`.
- The current version store type `MONGODB` is deprecated, please migrate to the new `MONGODB2` version store
  type. Please use the [Nessie Server Admin Tool](https://projectnessie.org/nessie-latest/export_import)
  to migrate from the `MONGODB` version store type to `MONGODB2`.

### Fixes

- CLI: fixed a bug that was preventing the tool from running properly when history is disabled.

## [0.95.0] Release (2024-08-07)

### Catalog S3 bucket configuration changes / breaking

- The S3 bucket configuration option `client-authentication-mode` has been removed (defaulted to `REQUEST_SIGNING`).
- A new S3 bucket configuration option `request-signing-enabled` has been added (defaults to `true`).

### Breaking changes

- See above for breaking changes to S3 bucket configurations. 

### New Features

- Catalog/Trino: Add convenience REST endpoint to provide a _starter_ Trino catalog configuration.
  Use `/iceberg-ext/v1/client-template/trino?format=static` for Trino 'static' catalog configurations,
  `/iceberg-ext/v1/client-template/trino?format=dynamic` for Trino 'dynamic' catalog configurations.
  Please take a look at the [Trino page](https://projectnessie.org/nessie-latest/trino/) for known
  limitations in Trino.
- Catalog: The Iceberg REST header `X-Iceberg-Access-Delegation` is now respected. The functionality
  depends on the S3 bucket configuration options `request-signing-enabled` and `assume-role-enabled`.

### Changes

- Catalog: Only general object store configurations are returned via the `/iceberg/v1/config` endpoint.
- Catalog: Table specific options are returned for each individual table, including scoped-down S3
  credentials, if applicable.
- The Nessie Spark SQL extensions are now based on the same syntax and options that are provided by the
  Nessie CLI. A reference docs page for the Nessie Spark SQL command syntax was added to the web site.

### Fixes

- Declare the `contentType` variable for CEL Authorization rules.
- Catalog: Make Nessie time-travel functionality available to all use cases, including DDL.

## [0.94.3] Release (2024-07-29)

### New Features

- Helm chart: liveness and readiness probes are now configurable via the `livenessProbe` and 
  `readinessProbe` Helm values.

## [0.94.2] Release (2024-07-26)

### Highlights

- Helm chart: it is now possible to use Helm templating in all values; any [built-in
  object](https://helm.sh/docs/chart_template_guide/builtin_objects/) can be specified. This is
  particularly useful for dynamically passing the namespace to the Helm chart, but cross-referencing
  values from different sections is also possible, e.g.:

  ```yaml
  mongodb:
    name: nessie
    connectionString: mongodb+srv://mongodb.{{ .Release.Namespace }}.svc.cluster.local:27017/{{ .Values.mongodb.name }}
  ```

  The above would result in the following properties when deploying to namespace `nessie-ns`:

  ```properties
  quarkus.mongodb.database=nessie
  quarkus.mongodb.connection-string=mongodb://mongodb.nessie-ns.svc.cluster.local:27017/nessie
  ```

## [0.94.1] Release (2024-07-25)

### Upgrade notes

- Helm chart: the `logLevel` configuration option now only sets the log level for the console and
  file appenders, _but does not change the `io.quarkus` logger level anymore_. To actually modify a
  logger level, use the `advancedConfig` section and set the
  `quarkus.log.category."<category>".level` configuration option, e.g.
  `quarkus.log.category."io.quarkus".level=DEBUG` would set the log level for the `io.quarkus`
  logger to `DEBUG`, effectively achieving the same as setting `logLevel` to `DEBUG` in previous
  versions.

## [0.94.0] Release (2024-07-25)

### Upgrade notes

- Helm chart: the `service.port` configuration option has been deprecated by `service.ports`, which
  is a map of port names to port numbers to expose on the service. The old `service.port` is still
  supported, but will be removed in a future release.

### Breaking changes

- Catalog/ADLS: The authorization type for ADLS file systems must be explicitly specified using the new
  `auth-type` configuration option. Valid values are: `STORAGE_SHARED_KEY`, `SAS_TOKEN` or `APPLICATION_DEFAULT`
  (new, container/pod credentials) or `NONE` (new, default, anonymous access).
- Helm chart/ADLS: The authorization type for ADLS file systems must be explicitly specified using the new
  `catalog.storage.adls.defaultOptions.authType` configuration option (overridable on a per-filesystem basis).
  Valid values are the same as above, the default is `NONE`.

### New Features

- Catalog/ADLS: Add mandatory `auth-type` configuration option for ADLS file systems, see above.
- GC: Iceberg view metadata is now expired as well.

### Fixes

- All application-level errors like "content not found" were logged at `INFO` level with a stack trace.
- Catalog: Fixed a potential fallback to the default auth-mode for S3 and GCS buckets.
- JDBC backend: fix a potential infinite recursion when creating/checking the required tables.

## [0.93.1] Release (2024-07-19)

### Breaking changes

- The `throttled-retry-after` advanced configuration property was renamed from
  `nessie.catalog.service.s3.throttled-retry-after` to
  `nessie.catalog.error-handling.throttled-retry-after`. The old property name is ignored.
- Helm chart: a few ADLS-specific options under `catalog.storage.adls` were incorrectly placed and
  therefore effectively ignored by Nessie; if you are using ADLS, please re-check your configuration
  and adjust it accordingly.

### New Features

- CLI: New `REVERT CONTENT` command to update one or more tables or views to a previous state.
- Catalog: Support external secrets managers AWS and Vault, experimental for Azure + GCP.

### Changes

- Catalog: ADLS + GCS credentials are no longer sent to the client. It is considered insecure to expose the
  server's credentials to clients, even if this is likely very convenient. Unless we have a secure mechanism
  to provide per-client/table credentials, users have to configure object store credentials when using GCS or
  ADLS via the local Iceberg configuration(s).

### Fixes

- GC: Fix behavior of cutoff policy "num commits", it was 'off by one' and considered the n-th commit as non-live
  vs the n-th commit as the last live one.
- GC: Record failed "sweep"/"expire" runs in the repository. Before this fix, failures were reported on the console.
- GC: Fix handling of broken manifest files written by pyiceberg up to 0.6.1
- Catalog/ADLS: Don't let endpoint default to warehouse/object-store URI
- Catalog/ADLS: More informative error message if mandatory `endpoint` is missing.
- Catalog/ADLS: Use a less restrictive endpoint in the 'ObjectIO.ping' function used for health checks.

## [0.92.1] Release (2024-07-13)

### Fixes

- Catalog: fix field-ID reassignment and last-column-id calculation

## [0.92.0] Release (2024-07-11)

### Breaking changes

- Catalog: The `nessie.catalog.s3.default-options.auth-mode` configuration property has been renamed
  to `nessie.catalog.s3.default-options.client-auth-mode` to better reflect its purpose. The old
  property name is not supported anymore and must be updated in customized Helm values and/or 
  Quarkus configurations.

### New Features

- Catalog: Exported Nessie repositories now include the contents for Nessie Catalog
- Catalog: Improve indicated health check errors
- Catalog/GCS: Support using the default application credentials
- Catalog/S3: Allow custom key+trust stores
- Catalog: Check privileges earlier
- Catalog: cleanup non-committed metadata objects

### Changes

- Helm chart improvements

### Fixes

- Fix potential class-loader deadlock via `Namespace.EMPTY`
- Catalog: Fix double write of metadata objects to S3
- GC/ADLS: Handle `BlobNotFound` as well
- Fix behavior of metadata-update/set-statistics + set-partition-statistics
- Fix duplicate OAuth interactive flows when the Nessie API compatibility filter is enabled

## [0.91.3] Release (2024-06-28)

### Breaking changes

- The config properties for the object storage defaults for S3, GCS and ADLS have been moved under the
  `default-options` composite object. Inconsistent property names between the old defaults and the per-bucket
  property names have been resolved.

### New Features

- Nessie CLI now has its own Docker images. Running Nessie CLI is now as simple as: `docker run -it
  ghcr.io/projectnessie/nessie-cli`. Read more about it
  [here](https://projectnessie.org/nessie-latest/cli/).

### Fixes

- Fix console output during `CONNECT` in CLI/REPL (the bug was introduced in 0.91.1)

## [0.91.2] Release (2024-06-24)

### Breaking changes

- We have improved Nessie client's support for impersonation scenarios using the token exchange
  grant type. A few options starting with `nessie.authentication.oauth2.token-exchange.*` were
  renamed to `nessie.authentication.oauth2.impersonation.*`. Check the [Nessie authentication
  settings] for details. Please note that token exchange and impersonation are both considered in
  beta state. Their APIs and configuration options are subject to change at any time.

## [0.91.1] Release (2024-06-22)

### New Features

- Nessie's metrics now support custom, user-defined tags (dimensional labels). To 
  define a custom tag, set the `nessie.metrics.tags.<tag-name>=<tag-value>` configuration property.
  Such tags are added to all metrics published by Nessie.
- Readiness/health check testing the general availability of the object stores configured for the warehouses.
- Helm chart with support for Iceberg REST

### Fixes

- S3 request signing, when using Iceberg REST, did not work with Iceberg (Java) before 1.5.0. Iceberg
  S3 request signing before 1.5.0 works now. 
- Fix service-name resolution in k8s, spamming the Nessie log.

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

[Unreleased]: https://github.com/projectnessie/nessie/compare/nessie-0.106.0...HEAD
[0.106.0]: https://github.com/projectnessie/nessie/compare/nessie-0.105.7...nessie-0.106.0
[0.105.7]: https://github.com/projectnessie/nessie/compare/nessie-0.105.6...nessie-0.105.7
[0.105.6]: https://github.com/projectnessie/nessie/compare/nessie-0.105.3...nessie-0.105.6
[0.105.3]: https://github.com/projectnessie/nessie/compare/nessie-0.105.2...nessie-0.105.3
[0.105.2]: https://github.com/projectnessie/nessie/compare/nessie-0.105.1...nessie-0.105.2
[0.105.1]: https://github.com/projectnessie/nessie/compare/nessie-0.105.0...nessie-0.105.1
[0.105.0]: https://github.com/projectnessie/nessie/compare/nessie-0.104.2...nessie-0.105.0
[0.104.2]: https://github.com/projectnessie/nessie/compare/nessie-0.103.6...nessie-0.104.2
[0.103.6]: https://github.com/projectnessie/nessie/compare/nessie-0.103.5...nessie-0.103.6
[0.103.5]: https://github.com/projectnessie/nessie/compare/nessie-0.103.4...nessie-0.103.5
[0.103.4]: https://github.com/projectnessie/nessie/compare/nessie-0.103.3...nessie-0.103.4
[0.103.3]: https://github.com/projectnessie/nessie/compare/nessie-0.103.2...nessie-0.103.3
[0.103.2]: https://github.com/projectnessie/nessie/compare/nessie-0.103.1...nessie-0.103.2
[0.103.1]: https://github.com/projectnessie/nessie/compare/nessie-0.103.0...nessie-0.103.1
[0.103.0]: https://github.com/projectnessie/nessie/compare/nessie-0.102.5...nessie-0.103.0
[0.102.5]: https://github.com/projectnessie/nessie/compare/nessie-0.102.4...nessie-0.102.5
[0.102.4]: https://github.com/projectnessie/nessie/compare/nessie-0.102.3...nessie-0.102.4
[0.102.3]: https://github.com/projectnessie/nessie/compare/nessie-0.102.2...nessie-0.102.3
[0.102.2]: https://github.com/projectnessie/nessie/compare/nessie-0.102.1...nessie-0.102.2
[0.102.1]: https://github.com/projectnessie/nessie/compare/nessie-0.102.0...nessie-0.102.1
[0.102.0]: https://github.com/projectnessie/nessie/compare/nessie-0.101.3...nessie-0.102.0
[0.101.3]: https://github.com/projectnessie/nessie/compare/nessie-0.101.2...nessie-0.101.3
[0.101.2]: https://github.com/projectnessie/nessie/compare/nessie-0.101.1...nessie-0.101.2
[0.101.1]: https://github.com/projectnessie/nessie/compare/nessie-0.101.0...nessie-0.101.1
[0.101.0]: https://github.com/projectnessie/nessie/compare/nessie-0.100.3...nessie-0.101.0
[0.100.3]: https://github.com/projectnessie/nessie/compare/nessie-0.100.1...nessie-0.100.3
[0.100.1]: https://github.com/projectnessie/nessie/compare/nessie-0.100.0...nessie-0.100.1
[0.100.0]: https://github.com/projectnessie/nessie/compare/nessie-0.99.0...nessie-0.100.0
[0.99.0]: https://github.com/projectnessie/nessie/compare/nessie-0.97.1...nessie-0.99.0
[0.97.1]: https://github.com/projectnessie/nessie/compare/nessie-0.96.1...nessie-0.97.1
[0.96.1]: https://github.com/projectnessie/nessie/compare/nessie-0.96.0...nessie-0.96.1
[0.96.0]: https://github.com/projectnessie/nessie/compare/nessie-0.95.0...nessie-0.96.0
[0.95.0]: https://github.com/projectnessie/nessie/compare/nessie-0.94.3...nessie-0.95.0
[0.94.3]: https://github.com/projectnessie/nessie/compare/nessie-0.94.2...nessie-0.94.3
[0.94.2]: https://github.com/projectnessie/nessie/compare/nessie-0.94.1...nessie-0.94.2
[0.94.1]: https://github.com/projectnessie/nessie/compare/nessie-0.94.0...nessie-0.94.1
[0.94.0]: https://github.com/projectnessie/nessie/compare/nessie-0.93.1...nessie-0.94.0
[0.93.1]: https://github.com/projectnessie/nessie/compare/nessie-0.92.1...nessie-0.93.1
[0.92.1]: https://github.com/projectnessie/nessie/compare/nessie-0.92.0...nessie-0.92.1
[0.92.0]: https://github.com/projectnessie/nessie/compare/nessie-0.91.3...nessie-0.92.0
[0.91.3]: https://github.com/projectnessie/nessie/compare/nessie-0.91.2...nessie-0.91.3
[0.91.2]: https://github.com/projectnessie/nessie/compare/nessie-0.91.1...nessie-0.91.2
[0.91.1]: https://github.com/projectnessie/nessie/compare/nessie-0.90.4...nessie-0.91.1
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
[0.65.0]: https://github.com/projectnessie/nessie/commits/nessie-0.65.0
[Nessie authentication settings]: https://projectnessie.org/tools/client_config/#authentication-settings
