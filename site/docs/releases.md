# Releases

**See [Nessie Server upgrade notes](server-upgrade.md) for supported upgrade paths.**

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

## 0.69.2 Release (August 29, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.69.2).

### Fixes

- Nessie CLI: check-content command was incorrectly reporting deleted keys as missing content, when
  using new storage model.
- GC Tool handles JDBC config via environment correctly

### Commits
* Process outer test instances in clientFactoryForContext (#7444)

## 0.69.1 Release (August 28, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.69.1).

### Fixes
- Nessie CLI: check-content command was incorrectly reporting deleted keys as missing content, when
  using new storage model.
- GC Tool handles JDBC config via environment correctly

### Commits
* Refactor `ContentMapping.fetchContents` (#7434)
* Ninja: fix badged in README

## 0.69.0 Release (August 25, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.69.0).

### Fixes
- Nessie CLI: check-content command was incorrectly reporting deleted keys as missing content, when
  using new storage model.
- GC Tool handles JDBC config via environment correctly

### Commits
* Ninja: changelog
* GC: Finally fix JDBC values specified via environment (#7425)
* Make check-content not report deleted keys (#7427)
* Remove unused org.openapitools:openapi-generator-cli dependency (#7426)
* Move OTel wrappers to the modules defining corresponding interfaces (#7423)
* Pushdown filters in TreeApiImpl.getEntries (#7415)

## 0.68.0 Release (August 24, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.68.0).

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

### Commits
* Add Value.Check to Entry + KeyEntry (#7420)
* BigTable: use Batcher consistently for bulk mutations (#7416)
* CI: Disable tests in Windows + macOS jobs (#7403)
* Support reading arguments from a file in the `delete` CLI command (#7382)
* Make check-content command compatible with new model (#7411)
* Ninja: changelog
* Gradle: Replace deprecated `Project.buildDir` (#7412)
* Build/Gradle/Jandex/Quarkus: Attempt to fix the occasional `ConcurrentModificiationException` (#7413)
* GC: Allow configuring gc-tool JDBC via env only (#7410)
* GC: Fix duplicate-key error w/ PostgreSQL (#7409)
* Enforce expected hash presence when creating and assigning references (#7396)
* GC: Fix log level property interpolation and default to INFO (#7407)
* CI: Free disk space for NesQuEIT (#7402)
* Include Hibernate Validator in nessie-jaxrs-testextension (#7395)
* Mark repository as imported (#7380)
* Bump undertow from 2.2.24 to 2.2.26 (#7388)
* Add runtime dependency to AWS STS in the GC tool (#7385)
* Ability to update the repository description (#7376)
* Fix compilation warning (#7381)
* Don't initialize the repo in Persist Import tests (#7378)
* Fix javadocs of Persist.storeObj (#7377)
* Protect against IOBE in IndexesLogicImpl.completeIndexesInCommitChain (#7371)
* Remove unused API v2 java client classes (#7370)
* Propagate content type from method parameter (#7369)
* Ninja: changelog
* Support assign/delete reference without type in Java client (#7348)
* Remove call to Persist.erase in ImportPersistV1.prepareRepository (#7363)
* Export secondary parents (#7356)
* Retry erase repo with non-admin path if DropRowRange fails (#7352)
* Add Quarkus config option to disable BigTable admin client (#7353)
* Add support for BigTable in Helm chart (#7345)
* Fix volume declaration for ROCKSDB storage (#7344)
* Minor javadoc clarification for ConflictType.UNEXPECTED_HASH (#7333)
* Remove workaround for Quarkus #35104 (#7315)

## 0.67.0 Release (August 02, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.67.0).

### Upgrade notes
- Tracing and metrics have been migrated to Quarkus "native". The options to en/disable metrics and tracing have been removed. Please remove the options `nessie.version.store.trace.enable`, `nessie.version.store.metrics.enable` from your Nessie settings.

### Changes
- Nessie API spec upgraded to 2.1.1
- Support for relative hashes has been standardized and is now allowed in all v2 endpoints
- Migrate to Quarkus metrics and tracing

### Commits
* Ninja: CHANGELOG
* Fix running Spark 3.3+ ITs on Java==17 (#7322)
* Nit: remove unused import (#7321)
* Fix NesQuEIT IntelliJ import (#7320)
* Remove metrics.enable and trace.enable properties in QuarkusEventConfig (#7319)
* Move to Quarkus provided observability (#6954)
* Extend relative hash support to whole API v2 (#7308)
* Add .python-version to .gitignore (#7313)
* Simplify BigTableBackendConfig.tablePrefix declaration (#7309)

## 0.66.0 Release (July 31, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.66.0).

### New Features
- New `entries` command in Content-Generator tool
- New `--all` option to the `content-refresh` Content-Generator tool command
- Helm chart: add `podLabels` for Nessie Pods

### Changes
- Add/fix `info` section in OpenAPI spec, add templates to `servers` section

### Fixes
- Fix handling of not present and wrong reference-type for create/assign/delete-reference API calls

### Commits
* Ninja: update CHANGELOG
* Add new `entries` command to `content-generator` (#7296)
* Make AbstractTestRestApiPersist extend BaseTestNessieRest (#7304)
* Move TestRelativeCommitSpec to right package (#7306)
* Add podLabels for Nessie Pods (#7287)
* Add `--all` option to the `content-refresh` command (#7292)
* Fix examples in openapi.yaml (#7291)
* Nit: update changelog (#7290)
* Correct handling of reference-type query parameter (#7282)
* Fix Quarkus Swagger UI (#7281)
* Move classes in quarkus-common, no change in functionality (#7275)
* Nit: Remove superfluous `allowDependencies` (#7273)
* Add OpenAPI info/description (#7270)
* Backport Keycloak and Nessie testcontainers (#7255)

## 0.65.1 Release (July 19, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.65.1).

### Changes
- Add validation of cutoff-definitions in `GarbageCollectorConfig`
- Fix self-reference in OpenAPI spec
- Add `servers` section to OpenAPI spec

### Commits
* Ninja: fix test failure
* Nit: update changelog
* OpenAPI spec: add `servers` section (#7266)
* Fix openapi self-reference and type ambiguity (#7264)
* Validate default-cut-off-policy with gc config/repository APIs (#7265)
* Allow Java 17 for Spark 3.3+3.4 tests (#7262)
* Introduce `CHANGELOG.md`, include in release information (#7243)
* Introduce `StringLogic` for persisted strings (#7238)
* Bump Keycloak to 22.0.0 (#7254)
* Revert bot-changes in `server-upgrade.md` (#7244)

## 0.65.0 Release (July 14, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.65.0).

* Revert Gradle 8.2.1 (#7239)
* Add Nessie as a Source announcement blog from Dremio website (#7236)
* Add `--author` option to `content-generator` commands (#7232)
* Add repository configuration objects (#7233)
* Fix retrieval of default branch (#7227)
* Allow re-adds in same commit (#7225)
* Allow snapshot versions in dependencies (#7224)
* IDE: Cleanup Idea excludes (#7223)
* Spark-tests: disable UI (#7222)
* Compatibility tests: move to new storage model (#6910)
* Use testcontainers-bom (#7216)
* Reference keycloak-admin-client-jakarta (#7215)
* Post Quarkus 3: Remove no longer needed dependency exclusion (#7214)
* Bump to Quarkus 3.2.0.Final (#6146)
* CI: Add some missing `--scan` Gradle flags (#7210)
* Update main README after UI sources moved (#7207)
* Forbid relative hashes in commits, merges and transplants (#7193)
* Remove misplaced license header (#7203)
* More diff-tests (#7192)
* removed extra tab (#7189)
* Tests: Make `ITCassandraBackendFactory` less flaky (#7186)
* IntelliJ: Exclude some more directories from indexing (#7181)

## 0.64.0 Release (July 03, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.64.0).

* Ninja: really fix create-release WF
* Fix create-release workflow (#7177)
* Make gc-base, gc-iceberg classes Java 8 compatible (#7174)
* Nessie-Java-API: Add builder methods for extended merge behaviors (#7175)
* Spark-SQL-tests: Nessie client configuration + ignore committer (#7166)
* improve configure-on-demand (#7173)
* Fix Gradle task dependencies (#7172)
* Let Immutables discover the right Guava version (#7165)
* Unify Gradle daemon heap size (#7164)
* Revert Gradle daemon heap settings (#7160)
* Nessie Catalog related changes for Spark SQL extensions (#7159)
* Minor Gradle build scripts addition (noop for `main`) (#7158)
* Fully lazy `StoreIndexElement` deserializion (#7132)
* CI: attempt to fix snapshot publising (#7151)
* Revert "Use WorkerThread if parallelism is one." (#7152)
* Use WorkerThread if parallelism is one. (#7150)
* Move `python/` to separate repo (#7147)
* Move `ui/` to separate repo (#7146)
* Simplify test profiles in AbstractOAuth2Authentication and AbstractBearerAuthentication (#7143)
* Remove another Deltalake left-over (#7145)

## 0.63.0 Release (June 27, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.63.0).

* Fix inability to delete some references (#7141)
* Do not add Jandex index to shadow-jars (#7138)
* Allow table names prefixes for BigTable (#7140)
* Lazily deserialize `StoreIndexElement`s `content` (#7130)
* StoreIndexImpl: wrong estimated serialized size for empty index (#7128)
* Events/Quarkus: Do not cache huge artifacts in Gradle cache (#7118)

## 0.62.1 Release (June 23, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.62.1).

* Emergency-fix of #7088 (#7117)
* Remove another occurence of Delta Lake (#7113)

## 0.62.0 Release (June 23, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.62.0).

* Identify merge-base instead of common-ancestor (#7035)
* Remove code for and mentions of Delta Lake (#7108)
* Fix reference-recovery-logic (#7100)
* Fix Windows CI (#7107)
* CI: Fix newer-java CI (#7106)
* Enhance references with created-timestamp + extended-info-obj-pointer (#7088)
* BigTable follow-up (#7102)
* Refactor KeycloakTestResourceLifecycleManager (#7101)
* Tests: Only pass the surrounding test class to `AbstractReferenceLogicTests` (#7099)
* Ref-indexes: supply `int/refs` HEAD commit ID to check in reference-r… (#7095)
* Rocks: simplify `checkReference` (#7096)
* Inmemory: slightly change the update-ref function (#7094)
* Mongo: unify condition handling (#7093)
* Dynamo: simplify condition handling (#7092)
* Tests: Add logging to a bunch of modules (#7086)
* Add some convenience functionality to `Reference` (#7089)
* Nit: `ImportPersistV1/2` just a static import (#7091)
* Just test refactorings, no functional change (#7090)
* Site: Add database status section (#7085)
* Store indexes: some new tests, some test/bench optimizations (#7066)
* Testing: Remove unnecessary `ITCockroachDBCachingPersist` (#7087)
* Fix Newer-Java + Mac/Win CI (#7058)
* Micro-optimization of `LazyIndexImpl.get()` (#7059)
* Bump Scala 2.12/2.13 patch versions (#7060)
* CommitLogic: add `fetchCommits` for 2 IDs (#7055)
* Enable token exchange flow in authn docker-compose example (#7057)
* Make number of access checks configurable (#7056)
* Fix nit in `TypeMapping` (#7054)
* UI: Properly "wire" `compileAll` + `checkstyle` helper tasks (#7042)
* Add BigTable as a version-store type (#6846)
* `nessie-rest-services` back to 8 (#7049)
* Fetch names references: replace `fetchReference`-per-ref w/ bulk-fetch (#7046)
* Extract `CommitMetaUpdater` class, fix "set authors on merge" (#7039)
* New storage/references logic: prevent one database read (#7045)
* More enhancements to docker-compose files (#7047)
* CI: Do not trigger on push to `feature/**` branches (#7044)
* CI: Split "Code Checks" job's main step (#7041)
* Minor fixes to Keycloak docs and docker-compose file (#7036)
* `BatchAccessChecker`: distinguish `UPDATE` and `CREATE` for commited values (#7028)
* Build/nit: simplify version catalog usage (#7032)
* CI: Give "Newer Java" runs more memory (#7031)
* CI: Support `feature/**` branches (#7030)

## 0.61.0 Release (June 13, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.61.0).

* Fix Backends.createPersist() (#7015)
* Make Docker image + tag for Minio configurable (#7023)
* Nit: Make `ITSparkIcebergNessieS3` safe against empty buckets (#7025)
* Ninja: make CI pass again, fix `ITSparkIcebergNessieS3`
* Add four new Nessie animations for chats (#7008)
* Nit: remove unused method (#7007)
* Micro-opt: collections with expected size (#7006)
* Nit: remove unnecessary type arguments (#7005)
* Micro-opt `TypeMapping` (#7004)
* Fix `:nessie-services-bench` (#7003)
* Move `StreamingUtil` to its only call site (#6999)
* Remove AWS lambda mentions from README and docs (#6997)
* Database adapters - back to Java 8 (#6995)
* Docs: Update Trino version (#6991)
* DynamoDB: let Quarkus use the Apache HTTP client (#6994)
* Export zip tempfile (#6983)
* Build/nit: missing annotation dependency (#6992)
* Build: allow javac 'release' option, server modules built for Java 11 (#6730)
* Build/nit: properly prevent missing compiler annotation warnings (#6988)
* Do not initialize new model repository for CLI tool (#6985)
* Faster export w/ new data model (#6984)
* Make the TLS guide compatible with macOS (#6959)
* Fix soft-merge conflict of #6952 and #6975 (#6987)
* Add some version-store microbenchmarks (#6952)
* Serialize amount of entries of store-index (#6976)
* Micro-optimizations in ContentKey + TypeMapping (#6975)
* Emit access-checks for merge and transplant (#6949)
* Add `VersionStore.KeyRestrictions` parameter bag (#6951)
* Export: fix NPE when only the ZIP file name is supplied (#6982)
* Remove `ITScyllaDBBackendFactory` (#6969)
* Fix Spark 3.4 dependency nit (#6972)
* Throw when v2 requested but v1 provided (#6958)
* Quarkus tests OOM (again) (#6955)
* Let new-storage DynamoDb use Apache Http Client (#6950)
* Quarkus events tests - proper commits (#6953)
* Events notification system for Nessie - Quarkus (#6870)
* Identify relative commit + commit by timestamp (#6932)
* Enhancements to the Events API (#6945)
* Use parameter objects for `VersionStore.merge()` + `.transplant()` (#6944)
* Add SQL extension for Spark 3.4 (#6822)
* Add convenience `Content.withId(String)` (#6937)
* Add user guide for TLS ingress (#6861)
* Doc for `NessieConfiguration.specVersion` (#6938)
* Bump Spark to 3.2.4 + 3.3.2 (#6916)
* Fix possible IntelliJ dependency issue (#6918)
* Remove Gatling runs in CI (#6912)
* Build-tools-tests: switch tests to new storage model (#6913)
* Build: prevent duplicate checkstyle task runs (#6915)
* Content generator: test against new storage model (#6907)
* Iceberg-views: tests against new storage model (#6908)
* Events-SPI: doc update for new storage model config option (#6906)
* GC: Test against new storage model (#6905)
* jaxrs-testextension: test against new storage model (#6904)
* Nit: Quarkus-tests: use new storage model code (#6903)
* Quarkus-tests: Update test resources to use new storage test code (#6902)
* Mongo/nit: simplify `MongoClientProducer` (#6901)
* Remove Jackson support in Events API (#6899)
* Configure MeterRegistry and MeterFilter globally (#6900)

## 0.60.1 Release (May 25, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.60.1).

* Release-publish: fix after removed native images (#6898)

## 0.60.0 Release (May 25, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.60.0).

* Pass content-IDs of all `ContentKey` elements to `Check`s (#6859)
* Nit: Spec version updates (#6897)
* Fix `MergeKeyBehavior.DROP` in new storage model for merge (#6894)
* Defer API compatibility check until first request (#6893)
* Remove mentions of spark2 in Docs (#6762)
* Revert "Bump keykloak container image from 21.0.2 to 21.1 (#6887)" (#6895)
* Table renames: allow delete-op after put-op (#6892)
* Bump keykloak container image from 21.0.2 to 21.1 (#6887)
* Update dependency ch.qos.logback:logback-classic to v1.2.12 (#6889)
* Bugfix: include ROCKSDB in selector to create PVC in both cases. (#6881)
* Build: cleanup `libs.versions.toml` (#6886)
* Don't use filters when checking API compatibility (#6877)
* Make nip.io usage resilient against lookup failures (#6885)
* CI/Nesqueit: Switch back to `iceberg-nesqueit` branch (#6883)
* Ability to disable the API compatibility check via system properties (#6875)
* Skip API compatibility check if /config endpoint fails (#6878)
* Fix snapshot publising after #6847 (#6882)
* Build: minor `baselibs.versions.toml` update (#6884)
* Improve NessieError message (#6874)
* Disable Scylla tests on macOS (#6871)
* Enable compatibility tests on macOS (#6857)
* Use nip.io domain in MinioExtension (#6856)
* Docker compose template for Nessie + OpenTelemetry (#6860)
* Release: Remove relocation-poms (#6810)
* Remove native image (#6847)
* Use API V2 by default in GC Tool (#6858)
* Enables the extended information in `NessieConfiguration` (#6640)
* Add missing jakarta annotations (#6850)
* Nit: Remove unneeded dev-profile hints (#6851)
* Quarkus ITs: Restrict Keycloak to tests using Keycloak (#6852)

## 0.59.0 Release (May 18, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.59.0).

* Add an API compatibility check to Nessie clients (#6818)
* Remove Content type hierarchy from Events API (#6840)
* Mark REST API v2 as GA (#6749)
* REST V2: Key ranges for entries + diff (#6743)
* Add ObjId.objIdFromByteBuffer() (#6845)
* Wikis / REST API v2 changes (#6801)
* Allow exporting only the latest contents from a branch. (#6823)
* Events notification system for Nessie - Service module (#6760)
* Remove the lambda module (#6819)
* increase severity of multiple errorprone checks (#6800)
* Tests: Add utility check for new storage model (#6814)
* Nit: let export/new-model not use a deprecated function (#6813)
* Release: no relocation-pom for events (#6809)
* SQL Extensions: Fix handling of quoted reference names (#6804)
* Update site with new storage config (#6795)
* UI: Handle reference names with `/` (#6806)
* set ClassCanBeStatic severity to ERROR (#6797)
* Some errorprone mitigations (no functional change) (#6796)
* Make EventSubscriber.onSubscribe not default (#6777)
* Make Reference.getFullName return Optional<String> (#6776)
* Add new content type UDF (#6761)
* Add missing test for #6758 (#6768)
* Events: align field names to model (#6771)
* Blog: Fix rendering of namespace creation option (#6778)
* Blog: Update about namespace enforcement (#6753)
* Add Alex to dev list (#6775)
* New storage: allow deletion of multiple repositories (#6758)
* Fix left-over TODO from content-metadata PR (#6759)
* Events notification system for Nessie - VersionStore changes (#6647)
* Nessie error details: Add `ContentKey` to related errors (#6721)
* Minor optimization when retrieving named referencs (#6745)
* Cleanup V2 MergeResponse (#6747)
* GC: Trim long failure messages in JDBC repo (#6746)
* Nit: remove no longer valid TODO (#6744)
* Add extraEnv to helm chart (#6698)
* Events notification system for Nessie - SPI module (#6733)
* CI/main: Run CI on main sequentially (#6741)
* CI: Run "forgotten" java 8 tests (#6738)
* Bump json5 to 2.2.3 (#6739)
* Revert "fix(deps): update mockito monorepo to v5 (major) (#6731)" (#6737)
* Reserve usage of BatchingPersist to dry-run mode only (#6736)
* UI: Update a bunch of JS dependencies (#6734)
* Nit: Remove unnecessary `JdkSpecifics` (#6729)
* Events notification system for Nessie - API module (#6646)
* Build/CI: pass `test.log.level` via `CommandLineArgumentProvider` and populate Quarkus console log level (#6725)
* Remove top-level condition from the `CI Website` job (#6728)
* CI: add `build/quarkus.log` to failed quarkus jobs artifacts (#6727)
* Do not store intermediate commits during merge/transplant (#6677)
* build: `buildSrc` using Java toolchain (#6726)
* Cassandra: add timeouts for DDL + DML (#6716)
* Keycloak requires container-network (#6719)
* CI: update helm-chart-testing (#6720)
* Nit: remove unused version definition (#6718)
* CI: capture test reports (#6717)
* Let `:nessie-versioned-spi` use OpenTelemetry (#6687)
* Minor delta test fix (#6699)
* Add Nessie spec definition for 2.0.0-beta.1 (#6679)
* Change default message for (squash) merges (#6642)
* Add "fallback cases" for relevant enums in `:nessie-model` (#6634)
* Make dry run merge / transplant throw exceptions (#6685)
* Rename `ConflictResolution.IGNORE` -> `ADD` (#6686)
* Add more commit-attributes to `Merge` (#6641)
* Implement "external" conflict resolution for merges (#6631)
* Ability to pass advanced config as nested YAML (#6684)
* Remove unnecessary annotations from `ITImplicitNamespaces` (#6678)
* Ensure custom content-types work (#6618)
* Nessie: Generic information for operations and content results (#6616)
* Events design doc - minor evolutions (#6672)
* bugfix: namespaceDepth filter loses entry content (#6648)
* Expose Nessie repository information (preparation) (#6635)
* Allow all commit attributes for namespace operations (#6643)
* Mark `namedRef` as `@Nullable` in `ContentService` (#6638)
* Allow Nessie clients to deserialize unknown `Content.Type`s (#6633)
* Prepare REST API for content-aware merges (#6619)

## 0.58.1 Release (April 19, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.58.1).

* Null out references to java.net.http.HttpClient (#6630)
* Validate that the generated `nessie-gc` executable works (#6625)
* Cleanup content-type code (#6617)
* Add a test for getContent() on the default branch (API v1) (#6623)
* Tests for the OAuth2 authentication provider in nessie-quarkus (#6597)
* Add test for specVersion in API v2 (#6599)
* Disable IT-Auth on WIn/Mac CI (#6615)
* Docs for the new OAuth2 authentication provider (#6595)

## 0.58.0 Release (April 15, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.58.0).

* Renovate: let more dependency trigger Quarkus/native CI (#6596)
* OAuth2 authentication provider for Nessie client (#6527)
* Design document for Nessie events notification system (#6482)
* JAX-RS: Properly "pass through" `WebApplicationException` (#6584)
* CI: Make Quarkus/native job not a matrix job (#6589)
* Quarkus: Fix HTTP compression parameters (#6581)
* Remove direct use of hamcrest (#6583)
* CI: Replace auto-labeler with changed-files checks (#6577)
* CI: Use Java 20 in "newer-java" (#6582)
* Update Helm template for new storage implementation. (#6580)
* Gradle: eliminate some more `doFirst`/`doLast` script references (#6516)
* CI: Remove `CI Success` job (#6572)
* Align Quarkus dependencies (#6565)
* CI: Skip Helm CI, if version is not available (fix) (#6559)
* Revert "Update smartbear/swaggerhub-cli action to v0.7.1 (#6519)" (#6556)
* Release-WF: Disable the Gradle cache (#6555)

## 0.57.0 Release (April 11, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.57.0).

* CI: adjust Gradle cache storage parameters for non-`ci.yml` workflows (#6521)
* Build: use Smallrye-OpenAPI-Plugin from Gradle plugins (#6525)
* Perftest: Fix commit-to-branch simulation (#6552)
* Support `@NessieServerProperty` annotations in nested tests (#6548)
* Remove unused type parameter in Backends.createPersist (#6554)
* CI: Skip Helm CI, if version is not available (#6546)
* Fix wrong jackson-core coordinates (#6553)
* CI: Un-bump latest Java from 20 to 19 (#359) (#6551)
* CI-Workflow updates (#6481)
* CI: finally the correct artifacts (#6520)
* README: Fix deep-links into CI jobs (#6522)
* Bump nodejs to 18.15.0 LTS + npm to 9.5.0 LTS (#6542)
* Merge: Verify that there is at least one source commit (#6514)
* Gradle: minor non-changes for configuration-cache (#6513)
* CI: Cache everything that's needed (#6515)
* Fix `nessie-gc` publication (#6511)
* Fix specVersion to comply with semver syntax (#6506)

## 0.56.0 Release (April 05, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.56.0).

* CI: Save 15m in CI NesQuEIT (#6505)
* Populate reference conflict details (#6503)
* Enhance `NessieError` with error details (#6492)
* Revert slf4j2 bump (#6504)
* Revert "Update smartbear/swaggerhub-cli action to v0.7.0 (#6478)" (#6500)
* Test code cleanup / no functional change (#6502)
* Add `@JsonView` to HttpConfigApi v2 (#6498)
* Fix CI badge on README.md (#6499)

## 0.55.0 Release (April 04, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.55.0).

* Add min-version + spec-version-bag to `NessieConfiguration` (#6480)
* IntelliJ: Exclude build directories in build-tools-integration-tests (#6489)
* Support running compatibility tests with the new data model. (#6484)
* Test code: Refactor `Json` to `JsonNode` in tests (#6487)
* Bump jaxb-impl from hadoop-common (#6485)
* Let errorprone not check code generated by APTs (#6486)
* Bring back logback logging (#6469)
* Remove concurrent test case execution per class (#6479)
* WF: No more need to schedule contaner-registry sync (#6454)
* Refactor CI workflow - bring back reasonable CI runtime (#6461)
* Do not let Weld register a shutdown hook (#6470)
* Skip empty commits during merge/transplant (#6468)
* Simplify BaseCommitHelper.mergeSquashFastForward() (#6467)
* Nit: remove unused version decl (#6466)
* Fix validation error messages in CommitImpl (#6450)
* Add a convenience `compileAll` Gradle task (#6456)
* Fix nessie-perftest-simulations standalone (#6453)
* IntelliJ: Let IntelliJ test-runner default to CHOOSE_PER_TEST again (#6452)

## 0.54.0 Release (March 30, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.54.0).

* Scylla: 8->4G (#6446)
* Remove the need to pass `Put.expectedContent()` (#6438)
* Tests: Faster ScyllaDB container startup (#6444)
* IntelliJ: Always use Gradle to run tests (#6445)
* Testing: Faster C* container startup (#6443)
* Re-add Cassandra version store type to Nessie-Quarkus (#6440)
* Allow CI on Nessie forks (#6442)
* Cleanup/build: Remove no longer used Gradle configuration (#6437)
* Add missing "project only" code-coverage-report tasks (#6436)
* Fix OOM when running tests `:nessie-cli` (#6435)
* Some minor Gradle build optimizations (#6428)
* Testing: add `-XX:+HeapDumpOnOutOfMemoryError` (#6433)
* Nit: fix IntelliJ import warning for nessie-perftest-simulations (#6431)
* Disable testcontainers startup checks (#6423)
* Remove old content-attachments approach (#6422)
* Expand iceberg-views version-ID to 64 bit (#6421)
* Update Nessie client version as per Iceberg 1.2.0 release (#6412)
* Site: Update Iceberg, Flink, Presto versions (#6419)
* Gradle CI tweaks (#6416)
* Bump maven to 3.9.1 + maven-resolver to 1.9.7 (#6413)
* Nit: suppress "unclosed resource" warning in some tests (#6408)
* UI-Build: Use openapi-generator jar directly (#6411)
* Nit: remove unnecessary `.collect()` (#6414)
* Nit: Suppress a bunch of unchecked and deprecated warnings (#6410)
* Fix nullability for fields of ContentValueObj (#6409)
* Remove deprecated and unused `@NessieUri` annotation (#6407)
* Tests: Update Quarkus' TestNessieError (#6399)
* Remove exception mapping for `java.security.AccessControlException` (#6405)
* Nit: remove unused code from #6384 (#6406)

## 0.53.1 Release (March 24, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.53.1).

* New storage: add "batch write" facade (#6385)
* Persist: replace `updateObj()` with `upsertObj()` (#6384)
* Import: print total duration (#6383)
* IntelliJ: exclude more directories from indexing (#6374)
* Import: Eager prefetching of commits during finalization (#6378)
* New storage: `Persist.updateObj()` must respect index-size limits (#6377)
* Import: use updated commit as new parent (#6376)
* Nessie Import: Respect commit import batch size (new model) (#6375)
* Nessie Import: Print durations, double row size printed to console (#6370)

## 0.53.0 Release (March 23, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.53.0).

* Import: paint dots also during the finalize phase (#6366)
* Use relocated protobuf runtime (#6355)
* Build/publishing: Proper Gradle module metadata for shadow-jars (#6361)
* Intellij: exclude `/.mvn` + `/ui/node_modules` (#6364)
* Support annotation-based configuration in "old server" tests (#6356)
* Mongo/storage: use `replaceOne` instead of `findAndReplaceOne` (#6363)
* Compatibility-test: allow adapter-configuration via system properties (#6353)
* Compatibility-tests: require Nessie >= 0.42.0, cleanup code (#6352)
* CI: Bump newer-java-version WF from 19 to 20 (#6348)
* Properly return `ParamConverterProvider` errors as HTTP/400 (#6346)
* Remove InstantParamConverterProvider (#6341)
* Update release notes template, remove "Docker Hub" (#6339)
* Build: Fix Jackson-annotations configuration for build-plugins int-test (#6337)
* Nit: Remove unnecessary null-check (#6344)

## 0.52.3 Release (March 20, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.52.3).

* Ninja: fix release WF
* Ninja: fix release WF
* Fix release WF (#6334)
* New storage: fix writetamp collision in C* integration tests (#6332)
* Use positive repo erase confirmation codes. (#6317)
* Forbid special ASCII chars in content keys (#6313)
* GH WF: Remove `concurrency` from PR jobs (#6314)
* Replace Docker Hub with ghcr.io (#6305)
* Fix param examples for getSeveralContents() (#6302)
* Fix labeler config (#6303)
* Workaround UI issues in ghcr.io + quay.io (#6299)
* Fix docker-sync WF typo (#6296)
* GH WF to sync container repositories (#6295)
* Content-tool: Add command to create missing namespaces (#6280)
* Ensure parent namespace(s) for commited content objects exist (#6246)
* Core reorg: use `api/` and `integrations/` directories (#6288)
* Add ACKNOWLEDGEMENTS.md (#6289)
* Allow some integration tests to be executed on macOS (#6266)
* Build/CI: Allow renovate to update the Maven wrapper (#6286)
* Update the Docker image section of main README.md (#6267)
* Ignore jEnv .java-version file (#6268)
* Use JvmTestSuite + JacocoReport (#6231)
* Content-generator tool changes for 'Ensure parent namespace(s) for commited content objects exist' PR (#6265)
* Test changes for 'Ensure parent namespace(s) for commited content objects exist' PR (#6263)
* CLI tool cheanges for 'Ensure parent namespace(s) for commited content objects exist' PR (#6264)
* compatiblity-tests: Extracted test changes for namespace-validation (#6261)
* Fix maven group id in docs (#6259)
* Add helper functions to `Namespace` and `ContentKey()` (#6256)
* Let ITs against Nessie/Quarkus send back stack traces (#6257)
* Replace `Key` with `ContentKey` (#6242)
* New DynamoDB storage - do not pull in the Apache HTTP client (#6243)
* Add missing Gradle build scan for last check (#6254)
* Fix `AbstractDatabaseAdapter.removeKeyCollisionsForNamespaces` (#6253)
* Fix wrong parameter validation override (#6241)
* Nit: remove mentions of dependabot (#6239)
* New Nessie storage model (#5641)

## 0.52.2 Release (March 20, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.52.2).

* Ninja: fix release WF
* Fix release WF (#6334)
* New storage: fix writetamp collision in C* integration tests (#6332)
* Use positive repo erase confirmation codes. (#6317)
* Forbid special ASCII chars in content keys (#6313)
* GH WF: Remove `concurrency` from PR jobs (#6314)
* Replace Docker Hub with ghcr.io (#6305)
* Fix param examples for getSeveralContents() (#6302)
* Fix labeler config (#6303)
* Workaround UI issues in ghcr.io + quay.io (#6299)
* Fix docker-sync WF typo (#6296)
* GH WF to sync container repositories (#6295)
* Content-tool: Add command to create missing namespaces (#6280)
* Ensure parent namespace(s) for commited content objects exist (#6246)
* Core reorg: use `api/` and `integrations/` directories (#6288)
* Add ACKNOWLEDGEMENTS.md (#6289)
* Allow some integration tests to be executed on macOS (#6266)
* Build/CI: Allow renovate to update the Maven wrapper (#6286)
* Update the Docker image section of main README.md (#6267)
* Ignore jEnv .java-version file (#6268)
* Use JvmTestSuite + JacocoReport (#6231)
* Content-generator tool changes for 'Ensure parent namespace(s) for commited content objects exist' PR (#6265)
* Test changes for 'Ensure parent namespace(s) for commited content objects exist' PR (#6263)
* CLI tool cheanges for 'Ensure parent namespace(s) for commited content objects exist' PR (#6264)
* compatiblity-tests: Extracted test changes for namespace-validation (#6261)
* Fix maven group id in docs (#6259)
* Add helper functions to `Namespace` and `ContentKey()` (#6256)
* Let ITs against Nessie/Quarkus send back stack traces (#6257)
* Replace `Key` with `ContentKey` (#6242)
* New DynamoDB storage - do not pull in the Apache HTTP client (#6243)
* Add missing Gradle build scan for last check (#6254)
* Fix `AbstractDatabaseAdapter.removeKeyCollisionsForNamespaces` (#6253)
* Fix wrong parameter validation override (#6241)
* Nit: remove mentions of dependabot (#6239)
* New Nessie storage model (#5641)

## 0.52.1 Release (March 20, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.52.1).

* Fix release WF (#6334)
* New storage: fix writetamp collision in C* integration tests (#6332)
* Use positive repo erase confirmation codes. (#6317)
* Forbid special ASCII chars in content keys (#6313)
* GH WF: Remove `concurrency` from PR jobs (#6314)
* Replace Docker Hub with ghcr.io (#6305)
* Fix param examples for getSeveralContents() (#6302)
* Fix labeler config (#6303)
* Workaround UI issues in ghcr.io + quay.io (#6299)
* Fix docker-sync WF typo (#6296)
* GH WF to sync container repositories (#6295)
* Content-tool: Add command to create missing namespaces (#6280)
* Ensure parent namespace(s) for commited content objects exist (#6246)
* Core reorg: use `api/` and `integrations/` directories (#6288)
* Add ACKNOWLEDGEMENTS.md (#6289)
* Allow some integration tests to be executed on macOS (#6266)
* Build/CI: Allow renovate to update the Maven wrapper (#6286)
* Update the Docker image section of main README.md (#6267)
* Ignore jEnv .java-version file (#6268)
* Use JvmTestSuite + JacocoReport (#6231)
* Content-generator tool changes for 'Ensure parent namespace(s) for commited content objects exist' PR (#6265)
* Test changes for 'Ensure parent namespace(s) for commited content objects exist' PR (#6263)
* CLI tool cheanges for 'Ensure parent namespace(s) for commited content objects exist' PR (#6264)
* compatiblity-tests: Extracted test changes for namespace-validation (#6261)
* Fix maven group id in docs (#6259)
* Add helper functions to `Namespace` and `ContentKey()` (#6256)
* Let ITs against Nessie/Quarkus send back stack traces (#6257)
* Replace `Key` with `ContentKey` (#6242)
* New DynamoDB storage - do not pull in the Apache HTTP client (#6243)
* Add missing Gradle build scan for last check (#6254)
* Fix `AbstractDatabaseAdapter.removeKeyCollisionsForNamespaces` (#6253)
* Fix wrong parameter validation override (#6241)
* Nit: remove mentions of dependabot (#6239)
* New Nessie storage model (#5641)

## 0.52.0 Release (March 20, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.52.0).

* New storage: fix writetamp collision in C* integration tests (#6332)
* Use positive repo erase confirmation codes. (#6317)
* Forbid special ASCII chars in content keys (#6313)
* GH WF: Remove `concurrency` from PR jobs (#6314)
* Replace Docker Hub with ghcr.io (#6305)
* Fix param examples for getSeveralContents() (#6302)
* Fix labeler config (#6303)
* Workaround UI issues in ghcr.io + quay.io (#6299)
* Fix docker-sync WF typo (#6296)
* GH WF to sync container repositories (#6295)
* Content-tool: Add command to create missing namespaces (#6280)
* Ensure parent namespace(s) for commited content objects exist (#6246)
* Core reorg: use `api/` and `integrations/` directories (#6288)
* Add ACKNOWLEDGEMENTS.md (#6289)
* Allow some integration tests to be executed on macOS (#6266)
* Build/CI: Allow renovate to update the Maven wrapper (#6286)
* Update the Docker image section of main README.md (#6267)
* Ignore jEnv .java-version file (#6268)
* Use JvmTestSuite + JacocoReport (#6231)
* Content-generator tool changes for 'Ensure parent namespace(s) for commited content objects exist' PR (#6265)
* Test changes for 'Ensure parent namespace(s) for commited content objects exist' PR (#6263)
* CLI tool cheanges for 'Ensure parent namespace(s) for commited content objects exist' PR (#6264)
* compatiblity-tests: Extracted test changes for namespace-validation (#6261)
* Fix maven group id in docs (#6259)
* Add helper functions to `Namespace` and `ContentKey()` (#6256)
* Let ITs against Nessie/Quarkus send back stack traces (#6257)
* Replace `Key` with `ContentKey` (#6242)
* New DynamoDB storage - do not pull in the Apache HTTP client (#6243)
* Add missing Gradle build scan for last check (#6254)
* Fix `AbstractDatabaseAdapter.removeKeyCollisionsForNamespaces` (#6253)
* Fix wrong parameter validation override (#6241)
* Nit: remove mentions of dependabot (#6239)
* New Nessie storage model (#5641)

## 0.51.1 Release (March 07, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.51.1).

* DOCS PR for Maven group-ID refactoring (#6207)
* Properly sign pom-relocations (#6236)
* Allow tests to customize Nessie REST API URI resolution. (#6234)
* build/nit: Remove superfluous settings (#6232)
* Add SwaggerHub badge to README (#6233)
* Make V2 ref path-param parsing independent of `Reference` (#6224)
* Add tests to validate that API v1 responses do not have v2 attributes. (#6222)
* Fix codecov-n-builds (#6230)
* V2 REST declares wrong response types in OpenAPI spec (#6211)
* Fix native images after recent rocksdb version bump (#6216)
* Symlink gradle-wrapper.jar, unignore gradle-wrapper.jar (#6219)
* upgrade GH WFs to ubuntu-22.04 (#6206)
* Maven group-ID refactoring (#6197)
* Fix: `Content.Type` is a string (#6202)
* Git ignore `__pycache__` (#6201)
* Compatibility tests code cleanup (#6198)
* Site: Update community page with updated chat information (#6192)
* Move Nessie speecific build code into this build (#6196)
* Nit: javadoc (#6193)
* Gradle 8 adoption for NesQuEIT (#6166)
* mac-CI: Workaround for brew-upgrade issue (#6184)

## 0.51.0 Release (March 06, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.51.0).

* Allow tests to customize Nessie REST API URI resolution. (#6234)
* build/nit: Remove superfluous settings (#6232)
* Add SwaggerHub badge to README (#6233)
* Make V2 ref path-param parsing independent of `Reference` (#6224)
* Add tests to validate that API v1 responses do not have v2 attributes. (#6222)
* Fix codecov-n-builds (#6230)
* V2 REST declares wrong response types in OpenAPI spec (#6211)
* Fix native images after recent rocksdb version bump (#6216)
* Symlink gradle-wrapper.jar, unignore gradle-wrapper.jar (#6219)
* upgrade GH WFs to ubuntu-22.04 (#6206)
* Maven group-ID refactoring (#6197)
* Fix: `Content.Type` is a string (#6202)
* Git ignore `__pycache__` (#6201)
* Compatibility tests code cleanup (#6198)
* Site: Update community page with updated chat information (#6192)
* Move Nessie speecific build code into this build (#6196)
* Nit: javadoc (#6193)
* Gradle 8 adoption for NesQuEIT (#6166)
* mac-CI: Workaround for brew-upgrade issue (#6184)

## 0.50.0 Release (February 24, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.50.0).

* Strip jakarta.* annotations for Java 8 (#6172)
* Fix Nessie GH URL in docs (#6167)
* Refactor advanced configuration (#6159)
* Site: Update community links CI and README.md CI (#6165)
* Redesign telemetry support in Helm chart (#6153)
* Disable versioned-transfer ITs in macOS CI (#6164)
* Remove rocksdb.dbPath from Helm chart values (#6149)
* Fix console log level setting in Helm charts (#6158)
* Minor fixes to the build-push-images.sh script (#6155)
* mac-os/win CI - add retry for steps that regularly timeout/hang (#6152)
* Add missing jakarta.* annotations (#6150)
* Disable intTest using containers/podman for macOS CI (#6148)
* Improve nessie-ui build a little bit (#6147)
* Bump npm from 7.24.2 to 9.4.2 and nodejs from 16.14.2 to 18.4.1 (#6145)
* Rename jobs for mac+win GH workflows (#6143)
* Allow all GH WFs to become "required checks" (#6131)
* Label `model` changes with `pr-integrations` (#6132)
* GH workflow to smoke test Docker images (#6127)
* Fix typo in release notes (#6129)
* PR Auto labeler (#6128)
* Fix renovate configuration (#6124)

## 0.49.0 Release (February 17, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.49.0).

* Update GH WFs to publish Java images for many platforms (#6110)
* GH uploaded artifacts retention (#6115)
* Test: Update the validation to unblock query-engine-integration-tests (#6117)
* GH workflows: prevent running a bunch of stuff on forks (#6116)
* Revert changes to Chart.yaml maintainers section (#6113)
* Fail test discovery in MultiEnvTestEngine if no environments are defined (#6108)
* Disable :nessie-compatibility-common tests on macOS (#6106)
* Disable NPM tests on Windows (#6107)
* Simplify Quarkus build configuration (#6103)
* Automatically generate Helm chart documentation (#6095)
* Add Jakarta's ws-rs to `javax.ws.rs.*` (#6067)
* Add Jakarta's validation constraints to `javax.validation.constraints.*` (#6096)
* Add Jakarta's `@Inject` to `javax.Inject` (#6093)
* Add Jakarta's `@RequestScoped` to `javax.enterprise.context.RequestScoped` (#6092)
* Add Jakarta's `@Nullable` to `javax.annotation.Nullable` (#6088)
* Add Jakarta's `@Nonnull` to `javax.annotation.Nonnull` (#6066)
* Ability to configure PVC selector labels (#6074)
* Gatling simulation for a mixed workload (#6068)
* Split ws-rs-api implementors into javax and jakarta (#6065)
* Add API v2 test for commit parents (#6073)
* Nit: replace Jetbrains not-null annotation import (#6072)
* CI: Fix PR integrations workflow (#6071)
* Make Nessie build with `javax.*` and `jakarta.*` dependencies (#6064)
* Remove `@ThreadSafe` annotation, no `jakarta.*` counterpart (#6063)
* Remove `@CheckForNull` annotation, no `jakarta.*` counterpart (#6062)
* Tests: let tests in nessie-compatibility-common use soft-assertions (#6061)

## 0.48.2 Release (February 08, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.48.2).

* Fix dependencies in :nessie-compatibility-common (#6055)
* fix website link in README (#6056)
* Nit: rename build utility function to `buildForJava11` (#6053)
* Reenable JVM monitoring in native images (#6052)
* Cleanup InmemoryStore after tests (#6048)
* Cleanup README.md badges (#6051)
* CI: Prevent OOM/GCLocker-issue in tests / disable Micrometer JVM-Metrics for tests (#6050)
* Clear in-memory database-adapter between REST-tests in Quarkus (#6049)

## 0.48.1 Release (February 06, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.48.1).

* Release-WF: Fix openapi yaml attachment (#6040)

## 0.48.0 Release (February 06, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.48.0).

* Produce NessieBadRequestException when paging is not available (#6020)
* Quarkus builds: modernize & simplify (#6019)
* Expose definitive Nessie API version to tests (#6021)
* CI: Gradle cache read-only for snapshot publication workflow (#6018)
* REST v2: return parent commit IDs in `CommitMeta.getParentCommitHashes` (#6001)
* REST API v2: get-multiple-namespaces w/ option for direct children (#6010)
* Quarkus-nit: build script cleanup (#6016)
* Refactor API v2 reference resolution logic into a utility class (#6012)
* Quarkus: make `QuarkusBuild` not caching (#6015)
* Add missing explicit rename-table test (#6008)
* Do not memoize CommitMeta.properties, it is derived (#6006)
* CI: Do not cache Python stuff (#6004)
* Add support for API v2 to the compatibility test framework (#6000)
* Add java client-based test for Entry.getContentId() (#5996)
* UI: Sort properties in commit details view (#5987)
* Allow retrieval of `Content` with `get-entries` for API v2 (#5985)
* CI: fix helm-testing action refs (#6005)
* CI: Helm testing (#6003)
* Revert "Make `QuarkusBuild` cacheable" (#5997)
* Nessie client doc improvements (#5992)
* Add effective-reference to get-entries, diff and modify-namespace responses (#5984)
* Build: Fix task dependency warnings for code coverage (#5990)
* Let content-generator use REST API v2 (#5948)
* REST API v2 : Let 'get-content(s)' return the reference (#5947)
* Minor convenience methods `getElementsArray()` to `ContentKey` + `Namespace` (#5983)
* Nit: Prevent java compilation warning (#5978)
* Re-make Gatling sims debuggable after the move to Gradle (#5972)
* Gatling Simulations - error out Nessie actions by default (#5971)
* Helm: Add storage class for RockDB PVC (#5981)
* Fix `MathAbsoluteNegative` in `NonTransactionalDatabaseAdapter` (#5979)
* Make `QuarkusBuild` cacheable (#5974)
* Add `@JsonView` really everywhere (#5965)
* Add PVC for RocksDB in Nessie Helm chart (#5952)
* Let Gatling simulations use Nessie API v2 (#5970)
* Build: Make `QuarkusGenerateCode` tasks cacheable (#5973)
* Nit: `var` -> `val` in quarkus builds (#5975)
* Remove usage of deprecated `StreamingUtil` (#5954)
* Nit: typo (#5951)
* Add `@JsonView` everywhere (#5953)
* REST v2: Correct namespace key evaluation for delete + get-multiple namespaces (#5946)
* Minor: immutables contstructors for `Branch` + `Tag` + `Detached` (#5944)
* Replace use of unpaged `get()` when listing references from SQL extensions (#5950)
* Add utility method `CommitResponse.toAddedContentsMap()` (#5945)
* Minor Namespace.isEmpty() optimization (#5943)
* Use the Smallrye OpenAPI Gradle plugin (#5936)
* Add `.scalafmt.conf` for IntelliJ (#5942)
* Update merge "no hashes" error message (#5938)
* Separate version catalog for `buildSrc/` (#5935)
* Re-add batched access checks & replace `j.s.AccessControlException` (#5930)
* Move Jersey test dependencies to where they belong to (#5933)
* Refactor Service/REST/API testing (#5924)
* Unify `*ApiImplWithAuthorization` and "base" `*ApiImpl` classes (#5927)
* REST v2 - fetch HEAD if hashOnRef is not specified (#5921)
* Prepare protocol independent pagination support for diff, get entries, commit log, get all refs (#5901)
* Just rename test classes (#5920)
* DatabaseAdapterExtension: configure database-adapter via a nested test class' parent context (#5919)
* Bump Spark to 3.1.3/3.2.3/3.3.1 (#5909)
* CI/Win: exclude UI tests (#5908)

## 0.47.1 Release (January 20, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.47.1).

* Fix Jacoco reporting in Nessie Quarkus projects (#5902)
* Return latest state of deleted reference (#5905)
* Disable JVM monitoring in native images (#5903)

## 0.47.0 Release (January 18, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.47.0).

* Use custom CDI extension for injecting Principals (#5883)
* Update V2 commit-response to include generated content-IDs (#5880)
* Add content-ID to REST API v2 entries endpoint (#5879)
* Gatling: properly prefer `sim.duration.seconds` over `sim.commits` (#5847)
* Spark extensions tests - no CID for new content & common-ancestor (#5877)
* Content generator: no CID for new content, properly use expectedContent+CID for existing (#5876)
* Deltalake: no CID for new content, properly use expectedContent+CID for existing (#5875)
* Quarkus & JAX-RS tests - no CID for new content & common-ancestor (#5874)
* Do not expose junit-jupiter-engine via test artifacts (#5870)
* Gatling: allow running the Gatling simulations against an external Nessie (#5848)
* Testing: unify container start retry mechanism (#5844)
* Testing/benchmarking: fix configuration for persist-bench via system properties (#5843)
* Add macOS + Windows build check badges to README.md (#5849)
* Use REST API v2 in Gatling simulations (#5846)
* Testing: use full Quarkus listen-URL instead of just the port (#5845)
* GH WF: simplify some steps (#5869)

## 0.46.7 Release (January 12, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.46.7).

* Expose Nessie API version to custom client builders in tests (#5839)

## 0.46.5 Release (January 12, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.46.5).

* CI: Fix jobs.if condition for mac+win workflows (#5840)
* nessie-quarkus-cli/jacoco: use a fresh jacoco data file (#5833)
* CI: cross-check macOS + Windows (#5705)
* Validate runtime parameters in service implementations. (#5828)
* Testcontainers: retry container launch (#5831)
* Test/CI: Introduce test parallelism constraints for test tasks (#5824)
* Actually assign `main` in AbstractContentGeneratorTest (#5830)
* Exclude compatibility tests from code-coverage (#5826)
* Disable Deltalog integration tests on Windows (#5825)
* Disable Deltalog integration tests on macOS (#5822)
* Disable tests on projects using testcontainers on Windows (#5817)
* Disable quarkus-jacoco on `:nessie-quarkus-cli` (#5820)
* Fix newline when testing under Windows (#5814)
* model/test: Platform independent line separator (#5815)
* Testing: add logging for projects using testcontainers (#5813)
* Testing: allow `test.log.level` in compatibility tests (#5812)
* Nit: compatibility-tests: remove wrong comment (#5816)
* Disable tests on Windows that have strict time requirements (#5807)
* Disable compatibility tests on Mac (#5805)
* GC/Tests: Ignore schema-less base-uri-test on Windows (#5804)
* Prevent using the Minio Extension on non-Linux OS (#5803)
* Proper Path handling in GC tool CLI (#5799)
* Make LocalMongoResource a bit more resilient (#5802)
* Testcontainers: increase number of start attempts from 1 to 5 (#5801)
* Disable Quarkus dev services for tests (#5800)
* Proper Path handling in IcebergContentToFiles (#5798)
* Proper Path handling in TestNessieIcebergViews (#5797)
* Fix ZIP importer handle leak (#5796)
* CI: Fix results-upload for PRs (#5795)
* CI: Remove deprecated `::set-output` (#5794)
* Nit: Suppress ClassCanBeStatic warnings (#5793)

## 0.46.3 Release (January 06, 2023)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.46.3).

* Remove grep -v from the HISTORY.rst generation script (#5778)
* Fix MultiEnvTestFilter to pass inner test classes (#5775)
* Make OlderNessieServersExtension inject Nessie URIs (#5770)
* Remove duplicate startHash call in `HttpGetCommitLog` (#5756)

## 0.46.2 Release (December 28, 2022)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.46.2).

* Add validation annotations to backend service interfaces (#5740)
* Docs: Update README.md for 0.46.0 release (#5734)

## 0.46.0 Release (December 20, 2022)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.46.0).

* Make generated OpenAPI type names more readable (#5732)
* Add serialized form tests for `CommitMeta` v2 (#5731)
* Add v2 attributes to `CommitMeta` (#5706)
* CI: No duplicate checkstyle task runs (#5703)
* Allow publishing Gradle build scans from CI (#5701)
* Remove legacy Spark extensions (#5704)
* Fix flaky TestKeyGenerator (#5702)
* Support merge/transplant message overrides (#5686)
* Add common ancestor commit in merge tests (#5696)
* Add global test timeout for versioned/persist/ (#5690)
* Add DeleteContent command (#4773)
* Expose updated reference data in java client APIs (#5670)
* Fix OpenAPI docs for the required "expected hash" parameters (#5665)
* Expose MergeBehavior to java clients (#5682)
* Expose `CommitResponse` in java client API (#5673)
* Migrate `FetchOption` to the `model` package (#5667)
* Add README.md to model (#5668)
* Use API v2 in GC CLI (#5660)
* Add pagination params to the Diff API v2 (#5452)
* Replace deprecated native build parameters (#5662)
* Move Graal/native registration to nessie-quarkus-common (#5661)
*   Increase gradle memory in integration tests (#5654)
* Remove unused Jaeger properties (#5647)
* Validate ID for explicitly created Namespaces (#5644)
* Gradle: no longer run test classes concurrently (#5653)
* Nit: remove TODO (#5652)
* Fix GetReferenceParams description for API v2 (#5639)
* Import: move repo-setup part to nessie-versioned-transfer (#5640)
* Export using reference/commit-log scanning (#5635)
* Switch nessie-client to use OpenTelemetry (#5607)
* Version-store tests: use proper Put operations (#5614)
* DatabaseAdapter/identify-heads-and-forks: Allow commit-log scanning (#5638)
* Version-store tests: Adjust test to currently undetected transplant conflict (#5613)
* DatabaseAdapterExtension: Allow adapter-configurations on fields (#5637)
* Export: store exporting Nessie version and show during import (#5636)
* Adopt REST tests to stricter validations (#5612)
* PersistVersionStore: expose missing additional-parents (#5633)
* Export/import: explicit tests for exporter/importer implementations (#5631)
* DatabaseAdapter/identifyHeadsAndForks: Don't return `NO_ANCESTOR` as fork point (#5632)
* Export/import: remove unused functionality from BatchWriter (#5630)
* Rely on new `NessieVersion` in CLI tools (#5629)
* Allow `-` as a reference name in v2 REST paths (#5618)
* Export/import: move file related code to separate package (#5628)
* Use soft-assertions in jaxrs-tests (#5610)
* Adopt versioned-tests to not set cid for "new" `Content`; minor refactoring for merge/transplant (#5611)
* Deprecate commit lists in merge-results (#5599)
* Add Nessie version into nessie-model.jar (#5609)
* Minor verstion-store commit-log test refactoring (#5604)
* Minor merge test enhancements (#5603)
* Add "diff test" with a key "in between" (#5602)
* Add a some test cases for "reference not found" scenarios (#5601)
* Factory methods for EntriesResponse.Entry + DiffResponse.Entry + MergeResult.KeyDetails (#5600)
* Add expected content to v2 REST update namespace (#5598)
* Switch to OpenTelemetry in Nessie Quarkus server (#5605)
* Minor code clean-up in client-side Namespace operations (#5594)
* Fix v2 URL path mapping for getReferenceByName (#5596)
* Add REST API v2 (#5004)
* Fix `@Parameter` annotations in `DiffParams` (#5589)
* Build UI against Nessie API v1 from 0.45.0 (#5587)

## 0.45.0 Release (November 29, 2022)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.45.0).

* Fix OpenAPI spec for DiffApi (#5584)
* Content generator: allow functional key patterns and more (#5575)
* CI: auto-destruct spawned Nessie Quarkus runner JVM (#5583)
* Scala/Gradle: keep Scala compiler alive, bump Scala 2.13 to 2.13.10 (#5576)
* Fix BaseExceptionMapper's NPE when an Exception's message is null (#5563)
* Add RefreshContent command (#5551)
* Fix erasing repo descriptions (#5557)
* Perftest: allow passing system properties to launched Nessie (#5556)
* Transfer: completely abstract `DatbaseAdapter` from core export/import (#5555)
* Remove dependency to database-adapter type from `ImportResult` (#5553)
* Deprecate `VersionStore.getRefLog()` (#5554)
* Nessie export/import: update abstraction for file-typed stuff (#5545)
* Nessie CLI: rename `call()` to `callWithDatabaseAdapter()` (#5540)
* Nessie import: check export version (#5543)
* Nessie CLI: Update wording for in-memory warning (#5542)
* Nessie/Quarkus: use `Instance<DatabaseAdapter>` (#5541)
* Update verification code for Nessie CLI erase-erpository (#5539)
* Add content-info Quarkus CLI command (#5501)
* Reduce memory pressure during Quarkus CLI integration tests. (#5530)
* Bump+change Quarkus builder image to Mandrel + 22.3 (#5526)
* renovate: put python back on weekly schedule (#5519)
* Docs: Update Spark Python/Java API docs (#5517)
* Fix latest Nessie and Iceberg versions in side docs (#5499)
* Refactor Nessie-Jax-RS extension a little bit (#5486)
* renovate: fix fileMatch regexp (#5489)
* fix renovate python requirements file pattern (#5481)
* Proper put-update-operation for namespace-update (#5484)
* Move tests from o.p.jaxrs to o.p.jaxrs.tests (#5483)
* Fix a few checkstyle warnings (#5482)
* Update protobuf plugin to 0.9.1 (#5457)
* Documentation accommodating new version format of Iceberg and Nessie artifacts (#5371)
* Pull `ProtobufHelperPlugin` from `gradle-build-plugins` (#5456)
* Refactor some test code to soft-assertions (#5446)
* Move Merge/Transplant classes to the api.v1.params package (#5435)
* Move old API classes into the api.v1 package (#5419)
* Move some tests from db-adapter test code to verstion-store test code (#5445)
* Support method-level NessieApiVersions annotations (#5436)
* Introduce multi-version API test framework (#5420)
* Extract service-side interfaces for RefLog and Namespace services (#5418)
* Extract common java client builder code (#5411)
* Extract service-side interfaces for client-facing services (#5412)
* Remove OpenAPI spec properties from build script. (#5413)
* Build UI against Nessie API v1 from 0.44.0 (#5408)
* Refactor TestAuthorizationRules (#5399)
* Java 11 HttpClient (#5280)
* Testing/nit: Logging for :nessie-s3minio ITs (#5398)
* Add non-trivial tests for assign branch/tag operations (#5395)
* Fix version in the Nessie Helm Chart (#5392)
* Extract a multi-env test engine into a module. (#5339)
* Move Nessie Client construction into a JUnit5 extension (#5370)
* Record next development version (#5387)
* GH workflows: Add missing `cache-read-only: true` (#5385)
* GH create release WF: next version not properly recorded (#5386)

## 0.44.0 Release (October 18, 2022)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.44.0).

* Ninja: fix create-release WF
* Ninja: fix GH env reference
* Update README mentioning Iceberg 1.0 (#5384)
* Add Dan + Vladimir to devs list (#5381)
* GH release WF: default to "minor" version bump (#5380)
* GH release WF: Fix wrong task name (#5379)
* Automatically update release text files (#5377)
* GH release WF - fix log exclusion + log filter for rel-notes (#5378)
* Nessie GC: Docs (#5209)
* Nessie GC: Command line tool (#5227)
* Nessie GC: Iceberg functionality (#5207)
* Avoid direct dependency on iceberg-bundled-guava (#5366)
* quarkus-server tests use dynamic port from env (#5352)
* Fix missing placeholder for Preconditons.checkState (#5360)
* fix GH workflows still mentioning maven (#5353)
* Remove httpClient param from AbstractRest.init (#5354)
* Update pretty-ms to 8.x (#5341)
* DynamoDB related test changes (#5338)
* Split unsquashed merge tests into dedicated test methods (#5328)
* Update testing-library-react to 13.x (#5334)
* Bump actions/checkout from v3 to v3.1.0 (#5335)
* Update material-ui to mui 5.x.x (#5326)
* nessie-client/test: compress responses for all relevant HTTP methods (#5323)
* Fix micrometer path replacement patterns (#5321)
* Renovate: labels for java/javascript/python (#5318)
* Revert protobuf to 3.21.6 (#5317)
* Renovate: limit to 2 PRs per hour (#5319)
* Isolate http-level test from java client-level tests (#5314)
* Skip newer-java workflow on forks (#5294)
* Nessie client tests: Replace JDK's HTTP server w/ Jetty (#5285)
* Move internal classes of the Nessie HTTP client (#5286)
* GH/WF: Use `temurin` instead of 'zulu` (#5289)
* Nessie-client tests: do not compile w/ older Jackson versions (#5287)
* Test Nessie client with Java 8 (#5284)
* Integrate Jackson-version tests into Gradle build (#5279)
* Migrate to Gradle version catalogs (#5167)
* Unify Postgres container version declaration for tests
* Migrate from dependabot to renovate (#5166)
* Testing pre-requisites for Nessie GC: Two S3 testing projects (#5142)
* Fix iceberg verison on web site (#5222)
* Java 19 testing (#5221)
* Automatic patch releases (#5214)
* Unsupport 0.30.x versions (#5212)
* WF: Remove Maven part (#5213)
* Nessie GC: JDBC Live-Set-Repository (#5208)
* Allow Hadoop Spark config in tests (#5206)
* Nessie GC: mark & sweep (`gc-base` module only) (#5144)
* Slight build scripts change for Java11+ target compat (#5203)
* Fix test failure caused by #5147 (#5204)
* No longer write ref-log entries for commit/merge/transplant (#5147)
* Schedule dependabot for npm + pip less frequently (#5201)

## 0.43.0 Release (September 15, 2022)

**Rolling upgrades from versions before 0.40.0 are not supported!**

* Prepare for Nessie GC
* Nessie export/import functionality
* Use Graal 22.2-java17 for native images
* Several test and build improvements

## 0.42.0 Release (August 26, 2022)

**Rolling upgrades from versions before 0.40.0 are not supported!**

* Key list creation fixes
* Pluggable content types

## 0.41.0 Release (August 5, 2022)

**Rolling upgrades from versions before 0.40.0 are not supported!**

* Do not persist and expose attached content metadata
* Fix issue when looking up key in an open-addressing-key-list

## 0.40.3 Release (August 1, 2022)

**Rolling upgrades from versions before 0.40.0 are not supported!**

* Remove Quarkus-BOM dependency from non-Quarkus projects

## 0.40.2 Release (August 1, 2022)

**Rolling upgrades from versions before 0.40.0 are not supported!**

* Dependency issues fixed

## 0.40.1 Release (July 25, 2022)

**Rolling upgrades from versions before 0.40.0 are not supported!**

* Fix key-lists issue resulting in server-errors (`ArrayIndexOutOfBoundsException`)

## 0.40.0 Release (July 22, 2022)

**Rolling upgrades from earlier versions are not supported!**

* Support Spark 3.1 (Scala 2.12), Spark 3.2 (Scala 2.12 + 2.13), Spark 3.3 (Scala 2.12 + 2.13)
* Support Iceberg 0.14.0
* Nessie Spark SQL extensions: handle timestamps w/ time-zones
* Nessie Spark SQL extensions: fix handling of `USE`d references for `CREATE/ASSIGN BRANCH/TAG`
* Detailed merge/transplant result to allow inspection of conflicts
* Merge/transplant optionally allow "force-keep" & "force-merge" of conflicting content-keys
* Iceberg table metadata stored in Nessie
* Improvements to REST error handling
* Performance improvements when there are many content-keys
* Hard limit on content-key length (max 20 elements, total 500 characters)
* Prevent (accidental) deletion of default branch
* Improved usage of automatic paging via `NessieApi`
* Improvements to Nessie server health checks
* Add rolling-upgrade test suite in regular CI
* Daily testing against Java 17 + newer
* Switched to Java 17 in native images
* Build switched from Maven to Gradle

## 0.30.0 Release (May 13, 2022)

* Add commit-ID to KeyListEntry when writing new key-lists
* Do not process old key-lists when retrieving values
* Helm: Fix k8s version detection in ingress template
* Database-adapter: commit optimizations
* Remove the configurable default for the configurable values for getDefaultMaxKeyListSize
* Dynamo/Mongo/TX: use bulk/batch requests where possible

## 0.29.0 Release (May 5, 2022)

* Spark SQL: Configure ref.hash for NessieCatalog only when explicitly requested
* Escape all column names in SQL DML+DDL statements
* Use hashOnRef when fetching Namespaces
* Helm: Add ingress support for Kubernetes >=1.22
* Fix CockroachDB transaction-retry behavior

## 0.28.0 Release (April 26, 2022)

* Generate unique content IDs for explicitly created namespaces
* Fix patterns for metrics
* Various test improvements (CI + build)
* Various minor code fixes (fixes for errorprone warnings)

## 0.27.0 Release (April 14, 2022)

* Support for Namespace properties
* Make NessieContentGenerator extensible

## 0.26.0 Release (April 12, 2022)

**Rolling** upgrades from an older Nessie release to 0.26.0 or newer are not supported.

* Remove global state for Iceberg tables + views
* Internal optimizations in database adapters, version store and API endpoints
* Change 'marker' character to indicate `.` in namespace/table identifiers from ASCII 0 to `\u001D`
* Opt-in to force-merge or not merge specific content keys (also for transplant)
* Squash merged and transplanted commits by default (with opt-out)

## 0.25.0 Release (April 6, 2022)

* Nessie Quarkus Server can use Postgres as its backend database
* Explicitly define behavior of multiple commit-operations in a commit
* Load correct view metadata for a given ref

## 0.24.0 Release (March 31, 2022)

* Prevent explicit creation of empty namespaces
* Add content-id to `BatchAccessChecker.canReadContentKey()`

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

* Update [Nessie CLI](nessie-latest/cli.md) commands to better match `git` syntax
* Update [REST Apis](develop/rest.md) to be more consistent and better
* Add support for merge & cherry-pick in DynamoDB storage backend
* Add [WebUI](guides/ui.md)
* Introduce new DynamoDB optimizations to support faster log and entry retrieval
* Update to Quarkus 1.9.1
* Expose the new [Store interface](https://github.com/projectnessie/nessie/blob/main/versioned/dynamodb/src/main/java/org/projectnessie/versioned/store/Store.java) for low level storage implementations
* Introduce Quarkus Gradle runner plugin for easier third-party testing (e.g. Iceberg)
* Enable [swagger-ui](guides/ui.md) by default in Nessie service

## 0.1.0 Release (October 1, 2020)

* Initial release
