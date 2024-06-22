# Release Notes

**See [Nessie Server upgrade notes](server-upgrade.md) for supported upgrade paths.**

## 0.90.4 Release (June 13, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.90.4).

### New Features

- Support for token exchange in the Nessie client has been completely redesigned. The new API and
  configuration options are described in the [Nessie authentication settings]. If this feature is
  enabled, each time a new access token is obtained, the client will exchange it for another one by
  performing a token exchange with the authorization server. We hope that this new feature will
  unlock many advanced use cases for Nessie users, such as impersonation and delegation. Please note
  that token exchange is considered in beta state and both the API and configuration options are
  subject to change at any time; we appreciate early feedback, comments and suggestions.

### Commits
* Skip 0.90.3
* Fix compilation failure (#8817)
* Revert commits for 0.90.3 release
* JDBC: add proper support for H2 (#8751)
* Fix cache-deserialization for `UpdateableObj` (#8815)
* OAuth2Client: refactor scopes (#8814)
* Token exchange: documentation enhancements (#8813)
* Token exchange: more flexible subject / actor configuration (#8812)
* OAuth2Client: integration tests for token exchange (#8810)
* Fix docker-compose files after Keycloak upgrade to 25.0 (#8804)
* HTTP client: introduce specialized HttpClientResponseException (#8806)
* OAuth2Client: internal code cleanup (#8807)
* OAuthClient: configurable subject and actor token types (#8805)
* OAuth2Client: introduce proper support for token exchange (#8803)
* OAuth2Client: don't refresh tokens using token exchange (#8790)
* Disable a test on macOS (#8799)
* Helm chart: minor improvement to JDBC section in docs (#8717)
* Nessie w/ Iceberg REST / post-release site changes (#8510)
* Ninja: fix release-create workflow issues in "create github release" job

## 0.90.2 Release (June 11, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.90.2).

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

### Fixes

- A bug in the API compatibility filter has been discovered and fixed: when OAuth2 authentication is
  being used, the filter causes the OAuth2 client to close prematurely, thus triggering unauthorized
  errors. A workaround is to simply disable the filter (set `nessie.enable-api-compatibility-check` 
  to `false`), but this is no longer necessary.

### Commits
* Manually bump to 0.90.1-SNAPSHOT for next release
* Revert "[release] release nessie-0.90.1"
* Another release-publish bug
* Manually bump to 0.90.1
* Revert "[release] release nessie-0.90.0"
* Revert 0.90.0 commits
* Ninja: fix
* Revert 0.90.0 release commits
* Ninja: Fix Swaggerhub
* Prepare bump to v0.90.0 (#8792)
* Persist: introduce type-safe bulk-fetch functions (#8763)
* Persist-cache: add negative obj cache (#8762)
* Remove NessieDockerTestResourceLifecycleManager (#8788)
* Catalog tests: disable token refresh (#8786)
* OAuth2Client: fix race condition when scheduling refreshes (#8787)
* OAuth2 client: add support for RFC 8414 metadata discovery (#8789)
* Support distributed invalidation for `Persist` cache (#8463)
* API compatibility filter: don't share authentication with main client (#8769)
* [Catalog] Restrict S3 signing to URIs within table base prefix (#8654)
* Persist: add `fetchObjsIfExist()` (#8761)
* Refactor Backend.setupSchema to return schema info (#8753)
* Add workaround for nip.io for macOS, optimize for Linux (#8759)
* Cleanup nessie-quarkus build file (#8757)
* [Catalog] Recognize `s3a` and `s3n`, behave like `s3` (#8755)
* Expose 'roles' in CEL access check scripts (#8752)
* Site: Add RSS+JSON feeds (#8754)
* Docs-gen: more documented types, don't expose "Java types" (#8705)
* Allow secrets in a Keystore (#8710)
* [Catalog] Secrets suppliers/managers API (#8706)
* JDBC: remove options 'catalog' and 'schema' and revisit schema creation (#8723)
* Helm chart: add support for OIDC client secret (#8750)
* Site: Clarify "Authorization" and remove confusing paragraph (#8749)
* Helm chart: fix a few property names (#8743)
* Add explicit authorization-type = CEL (#8746)
* Add `isAnonymous()` to `AccessContext` (#8745)
* Add a JSON view to `SmileSerialization` (#8744)
* Helm chart: reload pods when configmap changes (#8725)
* Replace `:latest` with versions in docker-compose files (#8724)
* Nit: fix docker-compose paths (#8722)
* Avoid invalid test names due to non-printable characters (#8721)
* Helm chart / Cassandra: add support for pulling credentials from secrets (#8716)
* Helm chart: fix wrong env var + nits (#8718)
* Helm chart: mention removal of legacy store types in 0.75.0 (#8719)
* Fix API examples for merge conflict responses (#8712)
* Prevent Quarkus warning about unindexed dependency (#8711)
* Helm chart: create ConfigMap for application.properties file (#8709)
* [Catalog] S3CredentialsResolver: remove one second from session end (#8703)
* Update URLs in ConfigChecks (#8704)
* Do not pin digests for Dockerfiles and docker-compose files (#8689)
* Strip leading `/` (only for S3) (#8681)
* [Catalog] Implement paging for Iceberg REST (#8633)
* Adjust all `logback*.xml` files to new syntax (#8678)
* Testing: less console log during BigTable tests (#8677)
* Helm chart: add support for JDBC catalog and schema (#8672)
* GC: fix incorrect paths when listing prefixes with ADLS (#8661)
* [Catalog] return metadata-location in object store (at least for now) (#8669)
* [Catalog] Handle `CompletionStage` from snapshot store (#8667)
* [Catalog] Remove unused parameters/code (#8666)
* Deprecate support for Java 8 (#8650)
* [Catalog] Simplify tokens-uri resolution (#8649)
* [Catalog] update CHANGELOG (#8651)
* README: remove link to demos repository (#8652)
* Un-ignore SetLocation directives (#8646)
* [Catalog] remove Google projectId for S3 (#8635)
* Ninja: fix renovate config
* Renovate: more automatic merges (#8640)
* Ninja: rename snapshot-publish workflow
* [FEATURE] Nessie integration of Iceberg REST (#7043)
* Authelia tests: remove nip.io (#8632)
* Split Release workflow job into multiple, retryable jobs (#8596)
* Custom keycloak container: add function to create a bearer token (#8622)
* OAuth2 client: add integration tests with Authelia (#8618)
* Docker compose: add example with Authelia (#8619)
* Fix jaegertracing image spec (#8615)
* Fix downloads on site + publish CLI uber jar (#8595)
* Fix incremental generation of site/docs (#8591)
* Separate MariaDB and MySQL datasources (#8585)
* Clean up failed 0.83.0 + 0.83.1 releases (#8590)
* Snapshot-Publishing: job splitting for snapshot releases (#8589)
* Renovate: there's more than just `dockerfile` (#8586)
* Remove deprecated `HttpClientBuilder` (#7803)

## 0.83.2 Release (May 23, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.83.2).

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

### Commits

(Note: the 0.83.1 and 0.83.0 versions failed to fully release all artifacts for technical reasons,
this list of commits contains all commits for 0.83.0, 0.83.1 and 0.83.2.)

* Ninja: fix GH release-create
* New CLI: Post-release site updates (#8468)
* Blog post: support for MariaDB and MySQL backends (#8577)
* Post-release: Rename nessie-quarkus-cli to nessie-server-admin-tool in docs (#8484)
* Nessie Server Admin Tool: add support for MariaDB and MySQL backends (#8548)
* Helm chart: add support for MariaDB and MySQL backends (#8554)
* Persistence: properly handle timeout-ish exceptions (#8533)
* Renovate: merge "digest" updates automatically (#8576)
* Nessie GC: add support for MariaDB and MySQL backends (#8545)
* Nessie server: add support for MariaDB and MySQL backends (#8544)
* UDF type: additional changes (#8560)
* Tests/Scylla: Cap SMP to 1/3 of num-CPUs (#8559)
* Testing: Centralize image resolution (#8546)
* Always close BigTable clients (#8549)
* Cassandra: explicitly specify statement idempotence (#8557)
* Refactor BackendTestFactory (#8553)
* JDBC persist: properly handle SUCCESS_NO_INFO and EXECUTE_FAILED (#8551)
* Persistence layer: add support for MariaDB and MySQL backends (#8483)
* Renovate: add some recommended extensions (#8534)
* Add congocc license to `NOTICE` (#8540)
* Hide `namespace-validation` setting in docs (#8535)
* Nit: fix Util.isHexChar() (#8528)
* Experimental ability to cache `Reference`s (#8111)
* Changelog / Server-Admin-Tool (#8515)
* Nessie client: optionally disable certificate verifications (#8506)
* Blog: Nessie Catalog announcement
* Publish docker images of the server admin tool (#8507)
* Verify BSD+MIT+Go+UPL+ISC license mentions in `NOTICE` file + expose in Nessie server, GC-tool, Admin-Tool and CLI/REPL (#8498)
* Use `ubi9/openjdk-21-runtime` instead of `ubi9/openjdk-21` as the base image (#8503)
* Rename nessie-quarkus-cli to nessie-server-admin-tool (#8482)
* Add license reports and checks (#8497)
* Bump slf4j to 1.7.36/2.0.12 + logback to 1.3.14/1.5.6 (#6536)
* Add GC tool help to site and enhance GC tool help (#8447)
* Update `UDF` + `IcebergView`content types (#8478)
* Let renovate merge all google-cloud-cli Docker tag bumps (#8472)
* New Java based Nessie CLI tool + REPL (#8348)
* GC: more verbose error messages (#8467)
* build-push-images.sh: remove unused "artifacts" parameter (#8464)

## 0.82.0 Release (May 06, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.82.0).

### Breaking changes

- The readiness, liveness, metrics and Swagger-UI endpoints starting with `/q/` have been moved from the
  HTTP port exposing the REST endpoints to a different HTTP port (9000),
  see [Quarkus management interface reference docs](https://quarkus.io/guides/management-interface-reference).
  Any path starting with `/q/` can be safely removed from a possibly customized configuration
  `nessie.server.authentication.anonymous-paths`.
- The move of the above endpoints to the management port requires using the Nessie Helm chart for this
  release or a newer release. Also, the Helm Chart for this release will not work with older Nessie
  releases.

### Commits
* Doc-gen: trim-right before style changes (#8459)
* Site: Fix release-workflow version bump mistakes (#8454)
* Site: split release notes pages (#8453)
* Move management endpoints to separate HTTP port (#8207)
* Site: let site use wider horizontal space (#8452)
* OAuthClient: fix AbstractFlow.isAboutToExpire (#8423)

## 0.81.1 Release (May 03, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.81.1).

### Fixes

- GC: Fix handling of quoted column names in Iceberg

### Commits
* Ninja: changelog
* Use custom class for handling storage URIs (#8420)
* Site doc generator: respect Javadoc on type for smallrye-config (#8445)
* Doc-gen: traverse really all supertypes (#8446)
* Build: fix a bunch of deprecation warnings and no non-consumed apt arg (#8436)
* Build: tackle Gradle deprecations (#8440)
* Handle deprecation of Maven `DefaultServiceLocator` (#8431)
* Nit: Eliminate deprecation warnings (#8430)
* Build: Disable warnings about javac command line options for Java 21+ (#8432)
* Build: Prevent log4j2 to register its MBean (#8433)
* Build: Prevent unnecessary scala compile warning (#8437)
* Build/Scala: replace deprecated `JavaConverters` with `CollectionConverters` (#8439)
* Build: Remove no longer necessary Java toolchain "constraint" (#8438)

## 0.81.0 Release (May 01, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.81.0).

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

### Commits
* GC: Fix NPE when fetching/parsing table-metadata JSON (#8428)
* Docs: Trino Nessie configurations (#8385)
* OAuth2Client: refactor ResourceOwnerEmulator (#8425)
* OAuthClient: don't call Instant.now() in serde layer (#8419)
* ITOAuth2Client: reduce device code poll interval (#8424)
* Update GH workflows / Gradle run steps (#8386)
* OAuthClient: consider zero as null for refresh_expires_in field (#8415)
* OAuthClient: minor tweaks to the token exchange flow (#8414)
* ResourceOwnerEmulator: properly shutdown executor (#8407)
* Simpler workaround for #8390 (#8397)
* Fix Quarkus uber-jar (#8394)
* Helm charts: add support for AWS profiles (#8382)
* Let nessie-quarkus use resteasy-reactive (#8383)
* Resolve some minor build warnings (#8380)
* Add Amazon STS to dependencies of Nessie Core server (#8377)
* Nessie client: fix javadoc and compiler warnings (#8376)
* OAuth2Client: fully support public clients (#8372)
* Site: exclude generated .md files from search index (#8374)
* Site: exlude old releases from search index, fix released-version title and page-dir (#8368)
* Nit: replace non-ASCII double-quote (#8369)

## 0.80.0 Release (April 21, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.80.0).

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

### Commits
* Ninja: reorg steps in release-create GH workflow
* Ninja: update CHANGELOG
* Make bulk read timeout configurable in BigTablePersist (#8361)
* Fix generates smallrye-config site docs, always add map-key (#8356)
* Ability to cancel interactive client authentication (#8347)
* OAuth2Client: defer initial token fetch when user interaction is required (#8354)
* Generate server and client docs from source (#8334)
* Update `ResponseCheckFilter` to return more human friendly error message (#8321)
* Fix site docs generation on macOS (#8350)
* OAuth2Client: refactor HTTP exchanges into separate flows (#8338)
* Add nessie version placeholder as docker image tags (#8339)
* Docs: align titles with metadata (#8341)
* OAuth2Client: improve sleep/close detection (#8337)
* Cleanup ResourceOwnerEmulator and AuthorizationCodeFlow (#8333)
* Add Nessie GC as Docker Image section to downloads page (#8332)
* Site: Fix link in site footer (#8327)
* Make `nessie.authentication.oauth2.client-secret` optional (#8323)
* Site rearrangement (#8318)
* OAuth2 client config: "bark" with all check-errors (#8322)
* Cleanup: Remove the outdated WORKFLOWS.md file (#8320)
* OAuth2/client: doc-nit: add note about running a client in a conainer (#8324)
* Renovate: Update package pattern (#8311)
* Testing: add test method timeout + more verbose reporting (#8310)
* Remove left-over micrometer-pattern (#8307)
* CI: Mitigate still unfixed Gradle bug leading to a `ConcurrentModificationException` (#8297)
* Testing/obj-storage-mock: add dummy assume-role endpoint (#8288)
* Use new plugin ID `com.gradle.develocity` instead of `com.gradle.enterprise` (#8293)
* Commit after schema creation for GC tool (#8290)
*  Content Generator: reorganize options using argument groups (#8284)
* Update README.md to point to correct url (#8282)
* Add --recursive to BulkCommittingCommand (#8271)
* Configurable scopes in KeycloakTestResourceLifecycleManager (#8275)
* Enforce S3 path-style access for ObjectStorageMock (#8272)
* SQL extensions: fix whitespace not appearing in antlr exceptions (#8273)
* Testing: Move `Bucket.createHeapStorageBucket()` to separate class, allow clearing it (#8268)
* [Docs]: Fix iceberg-links on docs with latest url (#8260)
* Object-store test containers improvements (#8258)
* Testing/object-storage-mock: support different ADLS client upload behavior (#8256)
* Add support for ADLS Gen2 and GCS to Object Storage Mock (#8244)
* Remove effectively unnecessary `:nessie-jaxrs` module (#8247)
* Respect comments in SQL + move code to extensions-base (#8248)
* S3-Mock: add local heap storage (#8236)
* Fix incorrect OpenTelemetry variable names (#8237)
* Add Apache HTTP client, choose HTTP client by name (#8224)
* HTTP-Client: capture beginning of unparseable error response (#8226)
* Allow concurrent instances of GCSContainer (#8235)
* Add GCS testcontainer + IT for Nessie-GC (#8233)
* Bump `com.google.cloud.bigdataoss:gcs-connector` from `hadoop3-2.2.18` to `.21` (#8232)
* CI: Remove duplicate run of `:nessie-client:test` (#8229)
* Minor enhancements to MinioContainer (#8225)
* Prevent duplicate ResponseCheckFilter (#8223)
* CI: Unify Docker + Helm jobs, validate helm chart against local image (#8215)
* Ninja: Fix renovate config
* Renovate/versioning schema for Minio (#8210)
* Add `NessieClientConfigSource` backed by Iceberg REST catalog properties (#8206)
* Fix nessie-gc executable (#8205)
* CI: Add timeouts to all jobs (#8203)
* Bump Minio testing container (#8194)
* Build: Allow "big" shadow-jars (#8192)

## 0.79.0 Release (March 12, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.79.0).

### New Features

- Iceberg bumped to 1.5.0

### Changes

- SQL extensions for Spark 3.2 have been removed
- Experimental iceberg-views module has been removed

### Commits
* Remove SQL Extensions for Spark 3.2 for Iceberg 1.5+ (#7940)
* Remove `iceberg-views` module for Iceberg 1.5+ (#8058)
* Nit: minor javadoc updates in `MergeKeyBehavior` (#8173)
* Simplify ConfigChecks.configCheck (#8177)

## 0.78.0 Release (March 07, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.78.0).

### New Features

- GC Tool: ability to skip creating existing tables (IF NOT EXISTS)
- Make `Authorizer` pluggable
- Helm chart: add option to set sessionAffinity on Service

### Fixes

- Handle re-added keys when creating squash commits
- JDBC backend: infer catalog and schema if not specified

### Commits
* Ninja; update CHANGELOG.md
* Add convenience `Namespace.getParentOrEmpty()` (#8170)
* Enforce covariant return types in NessieClientBuilder hierarchy (#8164)
* Revert meaning of `@WithInitializedRepository` (#8162)
* Handle re-added keys when creating squash commits (#8160)
* HTTP client: add request and response filters programmatically (#8154)
* Reduce flakiness in CustomKeycloakContainer startup (#8153)
* Docker image tool: implement local image builds (#8141)
* JDBC backend: infer catalog and schema if not specified (#8131)
* Bump Scala to 2.12.19 + 2.13.13 (#8125)
* Bump Spark 3.5 to 3.5.1 (#8124)
* APIv2: Properly throw an exception when using `GetEntriesBuilder.namespaceDepth` (#7563)
* Update Iceberg version in mkdocs.yml (#8122)
* Testing: Allow test instance `@NessiePersist.configMethod` (#8120)
* Update Iceberg version in README.md (#8121)
* Keycloak containers: close admin clients (#8118)
* Remove unnecessary `ServerAccessContext` (#8114)
* Add set of roles to `AccessContext`, inject AC instead of `Principal` (#8091)
* Add test case for re-create table and merge (#8113)
* Add S3 user guide section for Minio (#8102)
* GC Tool: ability to skip creating existing tables (IF NOT EXISTS) (#8081)
* Persist-cache: add config object (#8112)
* Nit/noop: Remove use of anonymous `StoreConfig` impl (#8110)
* Nit: correct outdated javadoc (#8107)
* Make `Authorizer` pluggable (#8090)
* Helm chart: add option to set sessionAffinity on Service (#8095)
* Fix logging class in `ConfigChecks` (#8089)
* Remove unused function in CachingPersistImpl (#8086)

## 0.77.1 Release (February 14, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.77.1).

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

### Commits
* Fix release-publishing (#8069)

## 0.77.0 Release (February 14, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.77.0).

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

### Commits
* CI: Don't fail CI job when list of changed files is empty (#8066)
* No longer publish to docker.io, only ghcr.io and sync to quay.io (#8065)
* Ninja: update CHANGELOG
* Version catalog: split versions for Quarkus/Platform and Quakus Gradle Plugin (#8062)
* Mark ContentResponse.getEffectiveReference as not null (#8043)
* Simplify ClientSideGetMultipleNamespaces (#8044)
* Separate production from test code in storage modules (#8046)
* Events: don't log startup messages if there aren't any subscribers (#8061)
* OpenTelemetry: only log startup messages once (#8059)
* Publish Docker images for the GC tool (#8055)
* Renovate: don't add any labels (#8053)
* Tweak CassandraClientProducer for tests (#8052)
* Fix `invalid @Startup method` again (#8047)
* Remove redundant logback-test.xml in classpath (#8048)
* Use Google Cloud CLI image that supports linux/arm (#8045)
* Add some configuration checks to highlight probably production issues (#8027)
* Fix VersionStore panels of the Grafana dashboard (#8033)
* Custom object types: Use gz as default compression (#8041)
* Fix typo (#8029)
* Update Jackson version test matrix in `:nessie-client` (#8030)
* Re-add `@Startup` annotation to eagerly setup the version store backend (#8028)
* Gradle - simpler assignments (#8016)
* CI/Testing: Let Renovate manage Docker image references (#8015)
* Testing: set dynamic Quarkus test-http port everywhere (#8006)
* Disable default OIDC tenant when authentication is disabled (#8000)
* Improve documentation for Nessie on Kubernetes (#7997)
* Disable OpenTelemetry SDK when no endpoint is defined (#8004)

## 0.76.6 Release (January 26, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.76.6).

### Changes

- Nessie Docker images now contain Java 21
- Helm: Make ingressClassName configurable
- Helm: Use auto scaling
- Improve error message when JDBC/C* columns are missing

### Commits
* Bump Spark 3.3 from 3.3.3 to 3.3.4 (#7990)
* Ninja: update CHANGELOG
* Helm chart: use autoscaling/v2 (#7982)
* Make ingressClassName configurable (#7979)
* Coordinated Tasks: make service-impl CDI-friendly (#7974)
* Use Java 21 in published Docker images (#7973)
* Coordinated Tasks (#7947)
* Build: add some more javac linting (#7965)
* nessie-events-quarkus: remove deprecation warnings (#7964)
* Bump Spark 3.4 from 3.4.1 to 3.4.2 (#7963)
* Improve error message when JDBC/C* columns are missing (#7961)
* Bump logback-classic from 1.2.12 to 1.2.13 (#7962)

## 0.76.5 Release (January 26, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.76.5).

### Changes

- Nessie Docker images now contain Java 21
- Helm: Make ingressClassName configurable
- Helm: Use auto scaling
- Improve error message when JDBC/C* columns are missing

### Commits

## 0.76.4 Release (January 26, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.76.4).

### Changes

- Nessie Docker images now contain Java 21
- Helm: Make ingressClassName configurable
- Helm: Use auto scaling
- Improve error message when JDBC/C* columns are missing

### Commits
* Bump Spark 3.3 from 3.3.3 to 3.3.4 (#7990)
* Ninja: update CHANGELOG
* Helm chart: use autoscaling/v2 (#7982)
* Make ingressClassName configurable (#7979)
* Coordinated Tasks: make service-impl CDI-friendly (#7974)
* Use Java 21 in published Docker images (#7973)
* Coordinated Tasks (#7947)
* Build: add some more javac linting (#7965)
* nessie-events-quarkus: remove deprecation warnings (#7964)
* Bump Spark 3.4 from 3.4.1 to 3.4.2 (#7963)
* Improve error message when JDBC/C* columns are missing (#7961)
* Bump logback-classic from 1.2.12 to 1.2.13 (#7962)

## 0.76.3 Release (January 16, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.76.3).

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

### Commits
* Fix URL endpoint for injected `NessieApi` from `OldNessieServer` (#7958)
* Improve test coverage in ProtoSerialization (#7943)
* Use fluent `ObjIdHasher` (#7957)
* Mention JAVA_TOOL_OPTIONS in the docs (#7952)
* Fix "unlimited" behavior flexible cache (#7949)
* remove unused jersey-test-framework-provider dependencies (#7946)
* Persist: Conditional deletes + updates (#7932)
* Fix RockDB config properties (#7942)
* Shrink heap footprint of `ObjIdGeneric` (#7934)
* Caching behavior per object type (#7931)
* HttpClient: ability to define per-request authentication (#7928)

## 0.76.2 Release (January 11, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.76.2).

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

### Commits
* fix free-disk-space action (#7937)
* Nit: let OAuth2Client use switch for enum (#7933)
* Adapt OAuth2Client to per-request base URIs (#7925)

## 0.76.1 Release (January 09, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.76.1).

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

### Commits
* Allow per-request base URIs in HttpClient (#7922)
* Compile services and SPI for java 8 (#7924)
* Fix Device Code flow tests with Java 21 (#7920)
* Custom obj types should not persist class name (#7915)
* Site: Fix TOC in `client_config.md` (#7906)
* Nit: remove mention of "Hive" on project web site (#7905)

## 0.76.0 Release (January 02, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.76.0).

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

### Commits
* Revert "Add `detach-history` command to the ContentGenerator tool (#7867)" (#7907)
* Add Quarkus tests for the OAuth2 Device Code flow (#7900)
* OAuth2 client: support for Device Code Flow (#7899)
* OAuth2 client: programmatic creation of OAuth2Authenticator (#7894)
* OAuth2 client: support endpoint discovery (#7884)
* HttpClient: properly close resources (#7898)
* Persist: simplify JsonObj (#7866)
* Add documentation page for repository migration (#7895)
* Quarkus CLI: minor cleanup after #6890 (#7896)
* Fix Helm CI tests (#7893)
* Nessie Client: support for Authorization Code grant (#7872)
* Fix NPE when fetching commit log with access checks enabled. (#7886)
* Add `detach-history` command to the ContentGenerator tool (#7867)

## 0.75.0 Release (December 15, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.75.0).

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

### Commits
* Quarkus ITs: Remove LoggerFinder error log message (#7862)
* Quarkus-tests: do not log OIDC connection + tracing warnings (#7860)
* Let OAuth2 errors not lot stack traces (#7859)
* stop testing spark-extensions 3.2 on iceberg main (#7863)
* Fix Quarkus warning `@Inject` on private field (#7857)
* Use `/` as the resteasy base path in Quarkus (#7854)
* Extract reusable REST related functionality (#7838)
* Move authN/Z code to separate module (#7851)
* Make `DiffParams` work with resteasy-reactive (#7846)
* Make server-side components use only Jakarta EE (#7837)
* Remove dependency-resolution workaround for guava/listenablefuture (#7841)
* renovate: reduce awssdk update frequency (#7840)
* Helm chart: remove mentions of legacy storage types (#7830)
* Persist: simplify serialization of custom objects (#7832)
* Ignore Obj.type() when using Smile serialization (#7828)
* Fix "older Jackson versions" tests in `:nessie-client` (#7820)
* Expose `HttpClient` from `NessieApi` when available (#7808)
* Expose request-URI in `HttpResponse` (#7807)
* Build: remove no longer needed reflection-config-plugin (#7800)
* Nit: Remove unsed Quarkus config options (#7799)
* Remove unused `@RegisterForReflection` annotations (#7797)
* Remove invalid `@Startup` annotation (#7796)
* Ensure that content IDs are unique in a Nessie repository (#7757)
* Remove database adapter code (#6890)
* Persist/custom objects: allow compression (#7795)
* CassandraPersist: minor code cleanup (#7793)
* Extensible object types (#7771)
* Require Java 17 for the build, prepare for Quarkus 3.7 (#7783)
* Update issue templates (#7787)
* CI: Add `concurrency` to CI-Mac/Win + newer-Java workflows (#7785)
* GH WF: Remove no-longer existing images to remove (#7784)
* Move ObjIdSerializer.java to test scope (#7782)
* Add `LABEL`s to `Dockerfile-jvm` (#7775)
* ClientSideGetMultipleNamespaces: push some predicates down to the server (#7758)

## 0.74.0 Release (November 21, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.74.0).

### New Features

- Nessie-GC: Support Google Cloud Storage (GCS) (experimental)
- Nessie-GC: Support Azure Blob Storage (experimental)

### Fixes

- Add namespace validation for rename operation.
- Namespace validation now correctly reports only one conflict when deleting a namespace that has
  children, whereas previously it reported one conflict for each child.

### Commits
* Fix wrong cache capacity unit in `NessiePersistCache` (#7746)
* Persist: enforce that null array elements are legal (#7741)
* BigTable: set QUALIFIER_OBJ_TYPE cell when upserting (#7731)
* Fix Nesqueit after #7715 (#7722)
* Renovate: only add one label (#7720)
* Nessie-GC: Support Azure Blob Storage (experimental) (#7715)
* Minor doc enhancements around authentication providers (#7710)
* Nessie-GC: Support Google Cloud Storage (GCS) (experimental) (#7709)
* Report a single conflict when deleting a namespace with many children (#7704)
* Add nessie-client-testextension to nessie-bom (#7703)
* Fix Maven-Central search URL in release notes (#7696)
* Align opentelemetry-alpha version (#7694)
* Fix snapshot publishing (#7692)
* Add namespace validation for rename operation (#7650)
* fix JUnit Jupiter references (#7690)
* DynamoDB: fix wrong error message when key schema invalid (#7685)
* OAuth2Client: improve stack traces and reduce log frequency (#7678)
* ITOAuth2Client: improve shutdown sequence (#7676)
* ITOAuth2Client: use less aggressive token lifespans (#7675)

## 0.73.0 Release (October 27, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.73.0).

### Highlights

- Nessie API spec was upgraded to 2.1.3. The only change is that when a commit attempts to create a content
  inside a non-existing namespace, the server will not only return a `NAMESPACE_ABSENT` conflict for the
  non-existing namespace itself, but will also return additional `NAMESPACE_ABSENT` conflicts for all the
  non-existing ancestor namespaces.

### New Features

- Nessie client: the OAuth2 authentication provider is now able to recover from transient failures when
  refreshing the access token.

### Commits
* Update CHANGELOG.md (#7674)
* Expose full set of Bigtable retry settings in BigTableClientsConfig (#7672)
* Report all missing namespaces at once (#7671)
* OAuth2Client: implement fault tolerance (#7669)
* Make Conflict.conflictType non nullable (#7667)
* OAuth2Client: forbid recursive calls to renewTokens() (#7663)

## 0.72.4 Release (October 24, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.72.4).

### Fixes

- Docker images again honor environment variables such as `JAVA_OPTS_APPEND` that are used to pass
  additional JVM options to the Nessie server. See the 
  [ubi8/openjdk-17](https://catalog.redhat.com/software/containers/ubi8/openjdk-17/618bdbf34ae3739687568813)
  base image documentation for the list of all supported environment variables.

### Commits
* Docker: re-enable JAVA_OPTS_APPEND env var (#7659)
* Relax CacheSizing validation rules. (#7660)
* OAuth2Client: propagate scheduling failures when calling authenticate() (#7656)
* Remove commit-meta property containing the merge-parent commit-ID (#7646)
* Enhance released jars (#7651)

## 0.72.2 Release (October 19, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.72.2).

### New Features

- Exposes object cache metrics, meters use the tags `application="Nessie",cache="nessie-objects"`

### Fixes

- Fix incorrectly calculated object cache maximum weight.

### Commits
* Expose object cache metrics (#7643)
* Cleanup: remove dependency on immutables-value-fixtures (#7644)

## 0.72.1 Release (October 18, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.72.1).

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

### Commits
*  Set OAuth2Client idle when Nessie client is idle (#7626)
* BigTable: simplify configuration of clients (#7639)
* Allow Bigtable RPC retries (#7636)
* Update undertow to 2.2.28.Final (#7616)
* Site: some maintenance (#7634)
* SQL+Cassandra: add new `refs` columns to schema check (#7631)
* CI/Publish: Mitigate Gradle's annoying CME bug and GH's HTTP/502 (#7625)

## 0.72.0 Release (October 13, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.72.0).

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

### Commits
* Record previous HEADs of named references (#7607)
* Ninja: Changelog note wrt CVE-2023-44487, fixed via Quarkus 3.4.3
* Remove OTel workaround for tests (#7623)
* Nit: replace TODO with comment (#7624)
* CI/NesQuEIT: Switch Iceberg master->main (#7618)
* CI: Use Java 21 in "newer java versions" workflow (#7525)
* Workaround to run events-service tests with Java 21 (#7617)
* Tag all Nessie multi-environment tests (#7577)
* BigTable: reimplement scanAllObjects with server-side filtering (#7610)
* Spark 3.1 left-overs (#7612)
* Ninja: Changelog updates
* IntelliJ: Generate required classes (#7611)
* Update SQL extensions code to use API V2 (#7573)
* Bump Iceberg to 1.4.0, support Spark 3.5 (#7539)
* BigTable backend: ability to disable telemetry (#7597)
* Cassandra backend: remove ALLOW FILTERING when fetching objects (#7604)
* Ability to export repositories in V1 format (#7596)
* Add option for table prefix to DynamoDB (#7598)
* SQL extensions: add support for DROP IF EXISTS (#7570)
* Fix docs on website (#7559)
* Allow relative hashes in table references (#7565)
* CI: Remove Spark 3.1 from NesQuEIT job (removed in Iceberg 1.4) (#7564)
* Cleanup: explicitly annotate `ignoreUnknown` for types that allow this (#7561)
* BigTable: custom channel-pool settings, make retry-timeout configurable (#7552)
* Add client-based test for request parameter validation (#7557)
* Fix Nessie java client groupId in docs (#7554)
* Docs: Update Metadata authorization (#7553)
* Remove unused type parameter in BigTablePersist.doBulkFetch (#7551)
* Store-cache: allow cache sizing based on max-heap-size (#7540)
* Configure BigTable client to use MAX_BULK_READS (#7548)
* BigTable: Use parallel reads for some objects to fetch (#7547)
* Combined Nessie server+client for testing (#7527)
* GH actions - allow fork of Nessie in `projectnessie` (#7529)
* Bump Spark to 3.3.3/3.4.1 (#7538)
* Bump Scala 2.13 to 2.13.12 (#7537)
* CI/NesQuEIT: Change temp branch back to "normal" (#7534)
* Build: allow Spark/Java configuration for `JavaExec` as well (#7531)

## 0.71.1 Release (September 21, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.71.1).

### Changes

- Configuration of the `NessieClientBuilder` now uses system properties, environment, `~/.config/nessie/nessie-client.properties` and `~/.env`

### Deprecations

- `HttpClientBuilder` class has been deprecated for removal, use `NessieClientBuilder` instead.

### Commits
* Ninja: Fix release-create WF

## 0.71.0 Release (September 21, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.71.0).

### Changes

- Configuration of the `NessieClientBuilder` now uses system properties, environment, `~/.config/nessie/nessie-client.properties` and `~/.env`

### Deprecations

- `HttpClientBuilder` class has been deprecated for removal, use `NessieClientBuilder` instead.

### Commits
* Ninja: changelog
* Cleanup nessie-client (#7516)

## 0.70.3 Release (September 20, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.70.3).

### Fixes

- Fix wrong `New value for key 'some-key' must not have a content ID` when swapping tables.

### Commits
* Avoid re-running previously validated access checks on commit retries. (#7524)
* Make sure `effectiveReference` is not present in v1 REST API responses (#7512)
* Add OTel span events for commit retry sleeps (#7509)
* IntellJ workaround when using Iceberg snapshot builds (#7504)
* Remove superfluous `dependsOn` (#7505)
* Remove ref-log code (#7500)

## 0.70.2 Release (September 12, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.70.2).

### Fixes

- Fix wrong `New value for key 'some-key' must not have a content ID` when swapping tables.

### Commits
* Fix behavior when swapping tables (#7498)

## 0.70.1 Release (September 12, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.70.1).

### Changes

- Content Generator tool: added new `--limit` parameter to `commits`, `references` and `entries` 
  commands.
- Content Generator tool: tool now prints the total number of elements returned when running the 
  `commits`, `references` and `entries` commands.
- Helm charts: OpenTelemetry SDK is now completely disabled when tracing is disabled.
- Helm charts: when auth is disabled, Quarkus OIDC doesn't print warnings anymore during startup.

### Fixes

- GC: Handle delete manifests and row level delete files

### Commits
* Ninja: update CHANGELOG
* GC: Handle delete manifests and row level delete files (#7481)
* GC: Bump spark version for integration tests (#7482)
* MultiEnvTestEngine: append the environment name to test names (#7478)
* Fix default-OLTP-port typo in docs (#7474)
* Add NessieUnavailableException (HTTP 503) (#7465)
* Helm chart: disable OIDC warnings when authentication is disabled (#7460)
* Helm chart: disable OpenTelemetry SDK when tracing is disabled (#7459)
* Content generator: add `--limit` parameter and print totals (#7457)

## 0.70.0 Release (August 31, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.70.0).

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

### Commits
* Propagate index stripes from parent to child commits (#7452)
* Build: fix removal of superfluous system-property for NesQuEIT (#7455)
* Build: remove superfluous system-property hack (#7451)
* Bulk-fetch contents for get-keys (#7450)
* Print full hashes in `commits` command (#7449)
* Add --hash parameter to content-generator commands (#7448)

## Older releases

See [this page for 0.50.0 to 0.69.2](./releases-0.69.md)

See [this page for 0.49.0 and older](./releases-0.49.md)
