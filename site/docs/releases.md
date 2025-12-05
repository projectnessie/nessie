# Release Notes

**See [Nessie Server upgrade notes](server-upgrade.md) for supported upgrade paths.**

## 0.106.0 Release (December 05, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.106.0).

### Fixes

- Catalog/S3: Add a per-bucket configuration option `chunked-encoding-enabled` (defaults to `true`)
  so deployments targeting Oracle Cloud Infrastructure (OCI) or other S3-compatible stores that
  reject chunked payload signatures can disable AWS SDK chunked encoding without downgrading
  Nessie. Fixes [#11441](https://github.com/projectnessie/nessie/issues/11441).

### Commits
* Ninja... one more
* ninja-fix previous PR
* Fix create-release workflow (#11691)
* Support configuration of S3 signing URL validity via chart (#11690)
* feat(helm): add priority class support (#11689)
* Add OCI friendly S3 chunked encoding toggle (#11676)
* Get back snapshot publications - take 2 (#11653)
* Attempt to get snapshot publishing back (#11629)
* Ninja (last one!): fix new Quarkus-CR workflow
* Ninja: fix run of Quarkus CR workflow
* Ninja: remove conditional
* Ninja: fix dangling whitespace in Quarkus CR workflow + align names
* CI: Add a workflow to verify that Quarkus CR versions pass Nessie CI (#11605)
* Fix a test issue in macOS CI (#11575)

## 0.105.7 Release (November 06, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.105.7).

### Fixes

- Fixes an issue when defining a nested schema field as an identifier field of an Iceberg schema.

### Commits
* Ninja: changelog
* Fix: nested schema fields used as identifier field causes NPE (#11569)
* CI: Fix build scans (#11565)
* Build: improve caching (#11563)

## 0.105.6 Release (October 24, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.105.6).

### Fixes

- Fixes an issue where Iceberg batch deletions did not work with Nessie remote S3 signing.

### Commits
* Build: fix some Java deprecation warnings (#11516)
* Gradle 9 prep - create iceberg/bom directory (#11515)
* Bugfix: S3 remote signing for batch deletions does not work (#11493)
* Prepare for Gradle 9 (#11491)
* Quarkus: Tackle some more deprecated/changed configs (#11492)
* CI/Test: Tackle deprecated otel config property (#11490)

## 0.105.5 Release (October 16, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.105.5).

### Fixes

- Iceberg REST: adapt the deprecation of `lastColumnId` in `AddSchema` table metadata update, fiel field is optional now

### Commits
* Fix javadocs for checkstyle, add `--continue` to CI jobs, remove retries (#11464)
* Object-storage-mock: Fix off-by-1 for HTTP range requests (#11465)
* Support parsing compressed metadata files in registerTable (#11435)

## 0.105.4 Release (October 08, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.105.4).

### Fixes

- Iceberg REST: adapt the deprecation of `lastColumnId` in `AddSchema` table metadata update, fiel field is optional now

### Commits
* Add KMS to GC tool (#11433)

## 0.105.3 Release (September 24, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.105.3).

### Fixes

- Iceberg REST: adapt the deprecation of `lastColumnId` in `AddSchema` table metadata update, fiel field is optional now

### Commits
* IRC: Make AddSchema.lastColumnId optional (#11352)
* Add `nessie-object-storage-mock` to `nessie-bom` (#11343)
* Fix maxOS CI (#11329)

## 0.105.2 Release (September 19, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.105.2).

### Highlights

- Nessie UI: CSS + fonts are now fetched from Nessie instead of external sources (CDNs).

### Commits
* Updates for Nessie-UI merge (#11323)
* CI/Caching: Fix Gradle cache retention (#11318)

## 0.105.1 Release (September 16, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.105.1).

### Highlights

- Bump Iceberg from version 1.9.2 to 1.10.0

### Commits
* Ninja: changelog
* Iceberg 1.10: Adopt Nessie CLI for Iceberg 1.10 S3 remote signing reconfiguration (#11305)
* Site-build: do not exclude 1.10 (#11302)
* Iceberg 1.10: S3 remote signing can no longer be "reconfigured" (#11303)
* Add AWSSDK kms dependency, required w/ Iceberg 1.10 (#11300)
* Iceberg 1.10: Adopt to oauth changes (#11304)
* Adopt Iceberg 1.10 GenericManifestFile restrictions (#11299)
* build: Nessie 0.10x is less than 0.50.* (#11301)
* Nit: fix deprecation warning in NessieServerAdminTestExtension (#11293)
* Nit: tackle deprecation warnings (#11294)
* Build/Shadow: handle duplicate files (no warnings) (#11292)
* Build/local-testing: Allow using custom Quarkus versions and not enforce Quarkus platform versions (#11276)
* Migrate from VLSI Jandex plugin to Kordamp Jandex plugin (#11268)
* Ninja: bump site-build timeout

## 0.105.0 Release (September 03, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.105.0).

### Fixes

- A bug has been resolved in the OAuth2 Authorization Code grant type. This fix addresses an issue
  where the `extra-params` configuration option was not being properly included in the authorization
  URI. Consequently, users, particularly Auth0 users passing the `audience` parameter, were receiving
  opaque tokens instead of JWTs.

### Commits
* OAuth2 (Authorization Code): Include extra parameters in authorization URI (#11239)

## 0.104.10 Release (August 27, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.104.10).

### Changes

- JDBC: Previously, the JDBC backends and Nessie GC left the fetch-size to its default, which is to fetch
  all rows when a `SELECT` statement is executed. This is changed to default to a fetch-size of 100 rows.
  To revert to the old behavior, set the corresponding configuration option to `0` (not recommended).
  A corresponding, new command line option for Nessie GC has been introduced as well.

### Commits
* fix: typo in server-iam.assume-role (#11219)

## 0.104.9 Release (August 25, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.104.9).

### Changes

- JDBC: Previously, the JDBC backends and Nessie GC left the fetch-size to its default, which is to fetch
  all rows when a `SELECT` statement is executed. This is changed to default to a fetch-size of 100 rows.
  To revert to the old behavior, set the corresponding configuration option to `0` (not recommended).
  A corresponding, new command line option for Nessie GC has been introduced as well.

### Commits
* Fix STDOUT redirect in create-gh-release-notes.sh (#11209)

## 0.104.7 Release (August 25, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.104.7).

### Changes

- JDBC: Previously, the JDBC backends and Nessie GC left the fetch-size to its default, which is to fetch
  all rows when a `SELECT` statement is executed. This is changed to default to a fetch-size of 100 rows.
  To revert to the old behavior, set the corresponding configuration option to `0` (not recommended).
  A corresponding, new command line option for Nessie GC has been introduced as well.

### Commits
* Fix release workflow in case no non-renovate commits happened (#11198)

## 0.104.5 Release (August 21, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.104.5).

### Changes

- JDBC: Previously, the JDBC backends and Nessie GC left the fetch-size to its default, which is to fetch
  all rows when a `SELECT` statement is executed. This is changed to default to a fetch-size of 100 rows.
  To revert to the old behavior, set the corresponding configuration option to `0` (not recommended).
  A corresponding, new command line option for Nessie GC has been introduced as well.

### Commits

## 0.104.4 Release (August 18, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.104.4).

### Changes

- JDBC: Previously, the JDBC backends and Nessie GC left the fetch-size to its default, which is to fetch
  all rows when a `SELECT` statement is executed. This is changed to default to a fetch-size of 100 rows.
  To revert to the old behavior, set the corresponding configuration option to `0` (not recommended).
  A corresponding, new command line option for Nessie GC has been introduced as well.

### Commits
* Add missing s3.path-style-access to GC Tool help (#11169)
* Helm chart: fix nodeport support (#11137)

## 0.104.3 Release (July 11, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.104.3).

### Changes

- JDBC: Previously, the JDBC backends and Nessie GC left the fetch-size to its default, which is to fetch
  all rows when a `SELECT` statement is executed. This is changed to default to a fetch-size of 100 rows.
  To revert to the old behavior, set the corresponding configuration option to `0` (not recommended).
  A corresponding, new command line option for Nessie GC has been introduced as well.

### Commits
* Switch Keycloak container to standard token exchange (#10963)
* Respect `idea.active` in addition to `idea.sync.active` (#10980)

## 0.104.2 Release (June 12, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.104.2).

### Changes

- JDBC: Previously, the JDBC backends and Nessie GC left the fetch-size to its default, which is to fetch
  all rows when a `SELECT` statement is executed. This is changed to default to a fetch-size of 100 rows.
  To revert to the old behavior, set the corresponding configuration option to `0` (not recommended).
  A corresponding, new command line option for Nessie GC has been introduced as well.

### Commits
* JDBC: Let JDBC fetch-size default to 100 (#10933)
* Testing/object-storage-mock: fix content-length for HTTP range requests (#10932)
* Fix error messages in `AssertRefSnapshotId` (#10860)
* feat(build): make archive builds reproducible (#10858)
* Java11-client: don't share the FJP, shutdown (if possible) (#10835)
* Add `public` workaround to some tests (#10836)
* Handle Iceberg NestedField.of() deprecation (#10829)
* Object-storage-mock: add start/stop log messages (#10832)
* Move secrets-manager `QuarkusTest`s to `src/test/` (#10839)
* Testing: give Quarkus 4g (#10837)
* Update `AddressResolver`, prepare for Vertx 5 (#10838)
* QUarkus: `RestAssured` may sometimes have the wrong port (#10840)
* Multi-env-test-engine cosmetics (#10828)
* Disable `ITOAuth2ClientAuthelia.testOAuth2AuthorizationCode()` (#10830)

## 0.104.1 Release (May 07, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.104.1).

### Changes

- OpenAPI specs are no longer published to swaggerhub.

### Commits
* Migrate to a different Maven publishing plugin (#10784)

## 0.104.0 Release (May 06, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.104.0).

### Changes

- OpenAPI specs are no longer published to swaggerhub.

### Commits
* Update Sonatype publishing URLs (#10758)
* Bump iceberg to 1.9 (#10773)

## 0.103.6 Release (May 01, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.103.6).

### Changes

- OpenAPI specs are no longer published to swaggerhub.

### Commits
* Remove links to swaggerhub (#10757)

## 0.103.5 Release (April 26, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.103.5).

### New Features

- Add PDB support to helm chart

### Commits
* Ninja: changelog
* Release: Fix Helm chart publication (#10730)

## 0.103.4 Release (April 24, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.103.4).

### Changes

- Change default of `nessie.version.store.persist.cache-enable-soft-references` to `false`

### Commits
* Add PDB support to helm chart (#10709)
* Cache-config: fix default for "soft references" to `false` (#10668)

## 0.103.3 Release (April 08, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.103.3).

### New Features

- Introduces a hard objects-cache capacity limit to ensure that the cache does never consume more than
  the configured cache-capacity plus a configurable "overshoot" (defaults to 10%). New cache entries are
  admitted as long as the current cache size is less than the "cache-capacity + overshoot".

### Changes

- Nessie's REST API endpoints now accept "truncated timestamps" in relative-commit-specs, aka without the
  second-fraction.

### Commits
* Introduce a hard capacity limit for the objects cache (#10629)
* Maven publication: Produce correct `<scm><tag>` in `pom.xml` (#10656)
* Relax Nessie REST API relative-commit-spec timestamp rules (#10623)
* Simplify Lowkey Vault configuration (#10591)

## 0.103.2 Release (March 21, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.103.2).

### New Features

- Catalog/S3/request-signing: Add a per-S3-bucket config option `url-signing-expire` to override the default
  3-hour lifetime of S3-URL-signing URLs.

### Commits
* Make S3 signing URL validity configurable (#10582)
* Adopt to next guava version (#10581)
* Disable NTP check for Authelia ITs (#10570)
* Adopt to JUnit deprecation (#10569)

## 0.103.1 Release (March 18, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.103.1).

### Highlights

- Configuration option `nessie.version.store.persist.cache-enable-soft-references` defaults to 
  `false` now. Some feedback suggests that using soft references in the Nessie cache may not be
  optimal with respect to GC overhead in some environments, so defaulting to `false` is safer.

### New Features

- Helm: Allow annotations on the configmap

### Fixes

- Catalog: Return consistent metadata-location for Iceberg REST APIs

### Commits
* Ninja: changelog
* Helm: Allow annotations on the configmap.  (#10510)
* Catalog: Return consistent metadata-location for Iceberg REST APIs (#10508)
* Make `cache-enable-soft-references` default to `false` (#10526)
* Add `copy` command to the ContentGenerator tool (#10443)
* Renovate: Quarkus Group (#10435)
* CI/NesQuEIT: Remove Spark 3.3 + revert workaround #10184 (#10436)

## 0.103.0 Release (February 18, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.103.0).

### Highlights

- If you are using Iceberg/Java 1.8.0 it is STRONGLY RECOMMENDED to upgrade to this or a newer Nessie release!

### Upgrade notes

- This Nessie version is compatible with Iceberg/Java version 1.8.0 via Iceberg REST.
  Iceberg 1.8.0 contains changes that breaks compatibility with previous Nessie versions!
- Iceberg table spec v3 is not supported in Nessie, because it is still under active development.

### Changes

- Dependencies that are only licensed using GPL+CE are no longer included in Nessie CLI, server and admin tool.

### Commits
* Ban dependencies licensed only as GPL+CE (#10413)
* Update changelog (#10412)
* Iceberg 1.8: Adopt remaining changes in CatalogTests + ViewCatalogTests (#10411)

## 0.102.5 Release (February 05, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.102.5).

### Note

- This release has no code changes.
- NOTICE and LICENSE files clarifications, included in jars published to Maven Central.

### Commits
* Include NOTICE+LICENSE in every jar (#10331)

## 0.102.4 Release (January 31, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.102.4).

### Note

- This release has no code changes except NOTICE file(s) clarifications.

### Commits
* Ninja: CHANGELOG
* Add separate NOTICE-BINARY-DIST file (#10315)
* Site: Improve the `Nessie Spark SQL Extensions` page (#10304)

## 0.102.3 Release (January 30, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.102.3).

### New Features

- Catalog: Iceberg table configurations overrides are now available in storage configuration settings.
  Example: `nessie.catalog.service.s3.default-options.table-config-overrides.py-io-impl=pyiceberg.io.pyarrow.PyArrowFileIO`

### Commits
* Ninja: add AL2 license for smallrye-certs
* Renovate: group quarkus-platform + quarkus-plugin together (#10300)
* Fix build scripts to distribute the right files (#10297)
* Catalog: Add table config overrides to bucket configuration (#10296)
* Fix a new failure in NesQuEIT, dependency issue w/ scala-collection-compat (#10298)
* Fix NesQuEIT for recent Iceberg changes (#10281)
* Adopt renovate config for #10275 (#10280)
* Update some Maven coordinates (#10275)

## 0.102.2 Release (January 23, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.102.2).

### Fixes

- Nessie re-assigns IDs for new schemas/partition-specs/sort-orders. The check that the provided ID for
  those must be valid (>= 0) is therefore superfluous, it can actually unnecessarily lead to problems. This
  change also fixes an issue that the last-added schema/spec/sort ID is set to -1, if the schema/spec/sort
  already existed. This lets the set-current-schema/set-default-partition-spec/set-default-sort-order
  updates with `-1` for the last-added one fail, but it should return the ID of the schema/spec/sort ID that
  already existed.

### Commits
* Catalog: Allow passing -1 for new schema/partition-spec/sort-order (#10264)

## 0.102.1 Release (January 22, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.102.1).

### Fixes

- Catalog/ADLS: Fix an issue that prevented the usage of retry-options for ADLS.

### Commits
* Catalog/ADLS: Only use `RequestRetryOptions` (#10255)

## 0.102.0 Release (January 21, 2025)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.102.0).

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

### Commits
* Fix using nested fields in partition-spec and sort-order (#10237)
* Fix backwards compatibility issues with Obj.referenced (#10218)
* Iceberg-update / set-statistics / snapshot-id deprecation (#10234)
* Cache: add option to disable soft references (#10217)
* Adopt Object Storage Mock to respect S3 chunked input trailing headers (#10231)
* Fix NesQuEIT to pass against recent Iceberg changes (#10184)
* Migrate to maintained shadow plugin (#10183)
* OAuth client: add support for custom request parameters (#10154)
* Remove duplicate entry in auth docs (#10157)
* Remove use of ScyllaDB in Nessie (#10144)
* Enable Azure Key Vault IT (#10142)
* Support metrics relabelings in service monitor (#10095)

## 0.101.3 Release (December 18, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.101.3).

### New Features

- Add the `cut-history` command to the Nessie server admin tool. This command allows advanced users to 
  detach certain commits from their predecessors (direct and merge parents).

### Commits
* Add admin tool command to cut commit log at a certain point. (#10048)
* Revert "Workaround for CI failures because of missing `vectorized/redpanda` images (#10074)" (#10087)

## 0.101.2 Release (December 12, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.101.2).

### Fixes

- Fix large index processing in the `cleanup-repository` admin command.

### Commits
* Fix clean-up of reference index objects (#10083)
* Workaround for CI failures because of missing `vectorized/redpanda` images (#10074)

## 0.101.1 Release (December 09, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.101.1).

### Fixes

- Fix handling of Iceberg update-requirement "no current snapshot"

### Commits
* Fix update-requirement check to handle "no current snapshot" requirement properly (#10064)

## 0.101.0 Release (December 06, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.101.0).

### New Features

- Helm: Add clusterIP and traffic policy to helm service config
- Add functionality to the Nessie server admin tool, the `cleanup-repository` command, to delete
  unneeded objects from a Nessie repository (backend database).

### Commits
* Server admin tool: add command to purge unreferenced `Obj`s (#9753)
* Persistence: purge unreferenced `Obj`s (#9688)
* Disable tests using containers on macOS in CI (#10038)
* Docs: update environment variables table and add section on Kubernetes memory settings (#10035)
* Ninja: changelog
* Add clusterIP and traffic policy to helm service config (#10011)
* Port some `CatalogTests` updates from Iceberg (#10036)
* Propagate CDI scopes to health checks (#10026)

## 0.100.3 Release (December 02, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.100.3).

### New Features

- Add `deploymentStrategy` to Helm deployment configs

### Fixes

- Allow multiple `SetProperties` updates via Iceberg REST.

### Commits
* Ninja: changelog
* Add deployment strategy to helm deployment configs (#10012)
* Catalog: Allow multiple `SetProperties` updates (#10024)
* Build: Allow testing GC with Java 23 (#10023)
* Renovate: Update config for trino-client (#10004)
* Quarkus/Agoral: fix deprecation (#10000)
* Migrate to `org.apache.cassandra:java-driver-bom` and bump to latest version 4.18.1 (#9992)
* Support v2 iceberg views with gzip (#9982)
* Snapshot publishing: no build scan (#9976)
* Nit: Remove antlr-removal left-over (#9977)
* Spark integrations: enhance tests to use `merge-on-read` for compaction and rewrite-manifests (#9974)
* Build/release: Fix Git info after #9965 (#9975)
* Remove "AWS Athena" from the index page (#9972)
* Nit: fix an "unchecked/unsafe case" warning (#9966)
* CI: Use a non-rate-limiting docker.io mirror (#9911)
* Build: tackle Gradle deprecations marked as for-removal in Gradle 9 (#9965)
* CI: Explicitly configure Scala version (#9970)

## 0.100.2 Release (November 20, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.100.2).

### Highlights

- Export: ZIP file exports were broken in all Nessie versions from 0.92.1 until 0.100.0.
  If you are using any of these versions, you must not use ZIP export mode, but use the
  file (directory) based exporter (`--output-format=DIRECTORY`)!

### Fixes

- Export: ZIP file exports are fixed with this Nessie version 0.100.1.

### Commits
* Force dnsjava downgrade to 3.5.3 (#9951)

## 0.100.1 Release (November 19, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.100.1).

### Highlights

- Export: ZIP file exports were broken in all Nessie versions from 0.92.1 until 0.100.0.
  If you are using any of these versions, you must not use ZIP export mode, but use the
  file (directory) based exporter (`--output-format=DIRECTORY`)!

### Fixes

- Export: ZIP file exports are fixed with this Nessie version 0.100.1.

### Commits
* Export: fix ZIP output after #9034 (#9945)
* Helm chart: fix incorrect documentation (#9923)
* Testing: use age-based pull policy for postgres images (#9910)

## 0.100.0 Release (November 12, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.100.0).

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

### Commits
* Catalog/Iceberg: support new `remove-partition-specs` metadata-update (#9906)
* Testing/Docker: use exact version for C* images (#9908)
* Catalog: Fix "load credendials" model (#9907)
* Catalog: add new model and api (#9905)
* Refactor Nessie's HTTP authentication (Quarkus 3.16 prep) (#9863)
* Catalog/S3,GCS: Adopt IAM policies to new object-storage layout (Iceberg 1.7.0) (#9897)
* Catalog/config: add endpoints to config response (Iceberg 1.7.0) (#9895)
* Testing: disable looking up GCP credentials (#9900)
* JDBC: commit after DDL setup + more info (#9901)
* GC: consider statistics files (#9898)
* Catalog/S3: Adopt S3 signing to new object-storage layout (Iceberg 1.7.0) (#9896)
* Revert "Prevent tracing initialization race (Quarkus 3.16 prep) (#9866)" (#9899)
* [Catalog] Do not return `*FileIO` options from the Iceberg REST config endpoint (#9642)
* Send s3-signer only when signing is enabled (#9869)
* Prevent tracing initialization race (Quarkus 3.16 prep) (#9866)
* Remove `@Nested` from a Quarkus test (Quarkus 3.16 prep) (#9865)
* Adopt `AmazonSecretsManagerBuilder` (Quarkus 3.16 prep) (#9864)
* Convenience for pyiceberg (#9868)
* Build only: Prefer Maven Local if enabled (#9861)
* Build/internal/NesQuEIT: enforce no colon `:` for `nessieProject()` (#9842)
* Docs: update troubleshooting guide with recent UID/GID changes (#9783)
* Helm chart: add `extraInitContainers` value (#9773)
* fix/keycloak-v26-deprecated-vars (#9778)
* Helm chart: redesign logging options (#9775)
* Fix some IDE warnings, remove unused code (#9772)
* server-admin-tool intTest: Re-add `forkEvery` (#9762)
* Remove validatation annotations from static functions (#9761)
* Do not access Apache snapshots repository by default (#9754)
* Transfer/related: make `CoreTransferRelatedObjects` generally accessible (#9752)
* Persist: introduce `deleteWithReferenced(Obj)` (#9731)
* ReferenceLogic: parameterized purge of the commit log of a `Reference` (#9735)
* Add convenience functionality to get all storage locations defined in `LakehouseConfig` (#9742)
* More verbose "Unauthorized signing request" warnings (#9743)
* Move catalog-config types to separate module (#9741)
* Site: fix formatting in `Time travel with Iceberg REST` chapter (#9732)
* Docker compose: enhance all-in-one example with Spark SQL and Nessie CLI (#9719)
* Helm chart: explicitly include namespace in created resources (#9711)
* Let `Persist.scanAllObjects()` accept an empty set to return all object types (#9687)
* Make the composite `TransferRelatedObjects` accessible to other projects (#9689)
* Events SPI: load implementations via CDI (#9696)
* Events RI: use Quarkus Messaging extension (#9686)
* Fix `ObjId.longAt()` for non-256-bit object IDs (#9685)
* Fix deprecation of `o.t.containers.CassandraContainer` + `KafkaContainer` (#9680)
* Replace deprecated `Aws4Signer` with `AwsV4HttpSigner` (#9681)
* Cache: keep (deserialized) object around (#9648)
* Patch version bumps of Scala + Spark 3.5 (#9667)
* Fix running `nessie-quarkus` instructions (#9668)
* Update Docker Compose instructions in Getting Started guide (#9662)
* Helm chart: remove bogus default value for oidcAuthServerUrl (#9654)
* Events API: test JSON serde with views (#9645)
* Events RI: add example with JSON serialization (#9639)

## 0.99.0 Release (September 26, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.99.0).

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

### Commits
* CLI: Pull in essential `*FileIO` dependencies for Iceberg REST (#9640)
* Events API: add support for direct JSON serialization (#9637)
* Remove unused `sourceHashes` from `TransplantResult` (#9628)
* Events API: use Nessie model API directly and remove DTOs (#9588)
* remove rocksdb dependency from nessie-compatibility-common (#9632)
* Helm chart: more flexible services configuration (#9625)
* Also initialize Iceberg-View `location` (#9629)
* [Catalog] More flexible named buckets (#9617)
* Nit: remove unintentional output (#9626)
* LakehouseConfigObj as transfer-related for export/import (#9623)
* Persistable `LakehouseConfig` (#9614)
* Derive `location` of new tables from parent namespaces, add some validations (#9612)
* HTTP client: Update Apache HTTP client impl to avoid deprecated classes (#9610)
* Richer access checks (#9553)
* Version Store Result API enhancements (#9592)

## 0.98.0 Release (September 23, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.98.0).

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

### Commits
* Helm chart: allow setting config options when the value is a zero-value (#9587)
* Helm chart: fix Azure SAS token settings (#9585)
* Nit: move constant used in tests (#9579)
* Construct `*ApiImpl` instead of injecting the V1 rest instances (#9577)
* Simplify IcebergMetadataUpdate/trusted-location (#9576)

## 0.97.1 Release (September 19, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.97.1).

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

### Commits
* MySQL/MariaDB: change from `BLOB` type to `LONGBLOB` (#9564)

## 0.97.0 Release (September 18, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.97.0).

### New Features

- Helm chart: support has been added for the `DYNAMODB2`, `MONGODB2`, `CASSANDRA2`, and `JDBC2`
  version store types, introduced in Nessie 0.96.0. Also, support for legacy version store types
  based on the old "database adapter" code, which were removed in Nessie 0.75.0, has also been
  removed from the Helm chart.

### Fixes

- Helm chart: fixed a regression where a datasource secret would result in a failure to deploy the
  chart.

### Commits
* Downgrade Jandex from 3.2.2 to 3.1.8 (#9561)
* Site: Add information about warehouse + object storages (#9560)
* Replace versioned-spi operation types with model operation types (#9551)
* Use `Tree`/`ContentService` in Catalog (#9547)
* Nit: fix JSON alias typo in IcebergMetadataUpdate interface (#9550)
* Quarkus: fail on unknown config properties (#9542)
* Add secrets-validation option to docs (#9541)
* Docs: add missing link in TOC of index.md (#9529)
* Events notification system for Nessie - Reference Implementation (#6943)

## 0.96.1 Release (September 12, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.96.1).

### New Features

- Helm chart: support has been added for the `DYNAMODB2`, `MONGODB2`, `CASSANDRA2`, and `JDBC2`
  version store types, introduced in Nessie 0.96.0. Also, support for legacy version store types
  based on the old "database adapter" code, which were removed in Nessie 0.75.0, has also been
  removed from the Helm chart.

### Fixes

- Helm chart: fixed a regression where a datasource secret would result in a failure to deploy the
  chart.

### Commits
* Helm chart: add support for DYNAMODB2, MONGODB2, CASSANDRA2, JDBC2 (#9520)
* Postgres tests: use alpine-based Docker images (#9521)
* Helm chart: redesign CI fixtures + minor fixes to docs (#9519)
* [Bug]: Helm Chart does not render in version 0.96 (#9517)

## 0.96.0 Release (September 11, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.96.0).

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

### Commits
* Secrets validation (#9509)
* Introduce functional `LakehouseConfig` (#9353)
* External secrets managers follow-up (#9497)
* Wire `SecretsProvider`s up to Quarkus (#8708)
* Add `SecretsProvider` implementations for AWS, GCP, Vault (#8707)
* Move object-store configuration types to `:nessie-catalog-files-api` (#9350)
* Reference secrets by name, do not inject (#9345)
* "Thread per test class" via `MultiEnvTestEngine` (#9453)
* public test methods in `BaseTestNessieApi` (#9472)
* Introduce `MONGODB2` version store type, deprecate `MONGODB` version store type (#9367)
* Helm chart: strengthen default security context (#9448)
* Add `referenced` attribute to persisted `Obj`s (#9401)
* Helm chart: add license headers and LICENSE file (#9466)
* Switch Nessie Docker images to use UID 10000 (#9456)
* Add ability to change pathType of ingress (#9462)
* CLI: make tool runnable without history (#9449)
* Use non-blocking random (#9445)
* Switch to new s3-sign endpoint using an opaque path parameter (#9447)
* Docs: fix broken link (#9446)
* Choose the Nessie client by name, case-insensitive (#9439)
* Cleanup resources held by Iceberg that accumulate JVM resources (#9440)
* Introduce `DYNAMODB2` version store type, deprecate `DYNAMODB` version store type (#9418)
* Adopt tests for C*2 (#9426)
* Introduce `CASSANDRA2` version store type, deprecate `CASSANDRA` version store type (#9368)
* Minor cleanups in JDBC(2) (#9422)
* Introduce `JDBC2` version store type, deprecate `JDBC` version store type (#9366)
* Allow Nessie Web UI to authenticate (#9398)
* [DocTool] Allow nested config sections (#9370)
* Catalog: use `prefixKey` in "list" operations (#9383)
* OAuth client: avoid recomputing HTTP headers for static secrets (#9411)
* Remove no longer used "global state" code (#9365)
* GC: always look into all keys of the last commit (#9400)
* Add license element to every pom (#9407)
* Minor cleanup in `:nessie-versioned-storage-store` + commit-logic-impl (#9399)
* Ninja: format comment
* Mitigate OOM during `:nessie-server-admin-tool:intTest` (#9371)
* Build: remove unneeded references to jacoco (#9384)
* Drop support for Java 8 (#9253)
* Require Java 21 for Nessie build (#9382)
* Remove a couple unused dependency declaration (#9376)
* Nit: remove unused field (#9359)
* Fix use of deprecated API (#9357)
* Add notes about running `ct lint` and `helm lint` locally (#9354)
* Ninja: changelog
* Eliminate more Quarkus test warnings (#9341)
* Use `@RepositoryId` qualifier instead of named bean (#9348)
* Allow tweaking `CommitMeta` via HTTP headers (#9335)
* Move object-storage specific code from `IcebergConfigurer` into `ObjectIO` implementations (#9327)
* Nit: suppress warning in `S3*Iam` types (#9349)
* Adopt Nessie REST API + client (#9289)
* Disable Swagger-UI + eliminate "duplicate operation ID" warnings in Quarkus tests (#9340)
* Explicitly prevent empty content keys (#9338)
* Fix typos in docs (#9339)
* Catalog: Implementation for down-scoped GCP/GCS access tokens (#9302)
* Add java client-based test for `getEntries` with a UDF (#9336)
* Add some "paranoid" tests for new URL encoding algorithm + forbid 0x7f (#9333)
* Fix javadoc after #9282 (#9332)
* Catalog: Implementation for short-lived user-delegation SAS tokens (#9301)
* BasicAuthenticationProvider: ability to dynamically provide a password (#9319)
* Prepare for Jakarta Servlet Spec 6 (#9282)
* OAuth2 client: introduce secret suppliers (#9315)
* Prepare downscoped credentials for ADLS + GCS (#9299)
* Doc generator: fix incorrect prefix when property has custom section (#9303)
* Site: `migration.md` overhaul + add server-admin-tool command references (#9291)
* Catalog: more producers (#9300)
* Release: Fix openapi publishing idempotency (#9294)

## 0.95.0 Release (August 07, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.95.0).

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

### Commits
* Quarkus 3.13.1 (#9236)
* Helm Chart: mention imagePullSecrets in values.yaml (#9292)
* Trino config-helper-endpoint & web site updates (#9270)
* Catalog: split S3SessionsManager into several components (#9279)
* Catalog: Enable time-travel and branch/tag selection for Iceberg REST in all cases (#9219)
* Replace antlr w/ congocc grammar in SQL Extensions (#9256)
* Build / IntelliJ: include the project root dir in the IDE window name (#9281)
* Remove Windows CI (#9260)
* Fix illegal access (#9280)
* Catalog: expose `location` for namespaces, iam-policy per location (#9170)
* Nit: remove references to unused `pr-native` label (#9272)
* Release: make `publish-openapi` job idempotent (#9264)
* Add signer-keys service (#9239)
* Scala Compiler and how it disrespects things (#9261)
* Adopt to Iceberg dropping support for Java 8 (#9259)
* Blog: Polaris (#9257)
* Fix Gradle/Kotlin deprecation warning (#9255)
* Fix a Gradle deprecation (#9254)
* Fix `NOTICE` vs `LICENSE` confusion (#9250)
* Declare the `contentType` variable for CEL AuthZ rules. (#9251)
* Add ability to generate IAM policies (#9244)
* Protect Iceberg REST config endpoint (#9247)
* Nit: make some test profiles non-`final` (#9243)
* Rename `S3Clients.awsCredentialsProvider()` to make its usage clearer (#9242)
* Add `StorageUri.pathWithoutLeadingTrailingSlash()` (#9241)
* Add utility method to escape strings in IAMs (#9240)

## 0.94.4 Release (August 01, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.94.4).

### New Features

- Helm chart: liveness and readiness probes are now configurable via the `livenessProbe` and 
  `readinessProbe` Helm values.

### Commits
* Replace deprecated quarkus properies (#9235)
* Fix NPE, will NPE w/ Quarkus 3.13.0 (#9233)
* Replace `quarkus-resteasy-reactive` with `quarkus-rest` (#9234)
* Helm chart: better document the purpose of the logLevel option (#9223)
* Gradle wrapper - download and verify (#9221)
* Fix legit scale=0 (#9214)

## 0.94.3 Release (July 29, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.94.3).

### New Features

- Helm chart: liveness and readiness probes are now configurable via the `livenessProbe` and 
  `readinessProbe` Helm values.

### Commits
* Helm chart: configurable liveness and readiness probes (#9203)
* Dependency cleanup, remove dependency on `nessie-quarkus-common` (#9206)
* Ninja: fix release notes

## 0.94.2 Release (July 26, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.94.2).

### Highlights

* Helm chart: it is now possible to use Helm templating in all values; any [built-in
  object](https://helm.sh/docs/chart_template_guide/builtin_objects/) can be specified. This is
  particularly useful for dynamically passing the namespace to the Helm chart, but cross-referencing
  values from different sections is also possible, e.g.:

  ```yaml
  mongodb:
    name: nessie
    connectionString: mongodb+srv://mongodb.{{ double_curly }} .Release.Namespace }}.svc.cluster.local:27017/{{ double_curly }} .Values.mongodb.name }}
  ```

  The above would result in the following properties when deploying to namespace `nessie-ns`:

  ```properties
  quarkus.mongodb.database=nessie
  quarkus.mongodb.connection-string=mongodb://mongodb.nessie-ns.svc.cluster.local:27017/nessie
  ```

### Commits
* Renovate: let Trino pass (#9199)
* Helm chart: allow templating in chart values (#9202)
* Helm chart: configurable mount point for configmap (#9201)
* Docs: add warning about printing configuration (#9193)

## 0.94.1 Release (July 25, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.94.1).

### Upgrade notes

- Helm chart: the `logLevel` configuration option now only sets the log level for the console and
  file appenders, _but does not change the `io.quarkus` logger level anymore_. To actually modify a
  logger level, use the `advancedConfig` section and set the
  `quarkus.log.category."<category>".level` configuration option, e.g.
  `quarkus.log.category."io.quarkus".level=DEBUG` would set the log level for the `io.quarkus`
  logger to `DEBUG`, effectively achieving the same as setting `logLevel` to `DEBUG` in previous
  versions.

### Commits
* Quarkus: split out code from `:nessie-quarkus` into separate modules (#9189)
* Include LoggingConfigSourceInterceptor to help diagnose configuration issues (#9190)
* Helm chart: don't set quarkus.log.level (#9191)

## 0.94.0 Release (July 25, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.94.0).

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

### Commits
* Helm chart: ability to change or add container ports (#9183)
* Ninja: changelog
* Add java client-based test for committing a UDF (#9182)
* Initial Trino integration testing (#9160)
* CLI: Add s3/adls/gcs file-io impls to CLI (#9177)
* Nit: simplify `normalizeBuckets()` (#9179)
* Catalog/data-access: propagate `X-Iceberg-Access-Delegation` header (#9178)
* Add utility to create an OAuth2 token (#9176)
* Minio test resource: way to expose S3 endpoint (#9175)
* Fix missing OAuth token for GCS IT (#9174)
* Load reference info before processing commits during export (#9168)
* Remove OIDC from dependencies of nessie-catalog-service-rest (#9171)
* Bump timeout in release workflows (#9172)
* Catalog: Don't log every client error using `INFO` log level (#9162)
* Doc-gen-tool: support HTML entities in Javadoc (#9169)
* Remove `@WithDefault` annotations (#9165)
* Limit recursion when creating tables in `JdbcBackend` (#9163)
* ninja: changelog
* GC: Support Iceberg Views (#9074)
* Helm chart: add support for ADLS auth type (#9158)
* Ninja: changelog
* Add `APPLICATION_DEFAULT` authorization type for ADLS Gen2 (#9086)
* Site: libraries note typo (#9152)
* Remove superfluous badge from README (#9150)
* Site-gen: remove backup files generated by `sed` (#9151)

## 0.93.1 Release (July 19, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.93.1).

### Breaking changes

- The `throttled-retry-after` advanced configuration property was renamed from
  `nessie.catalog.service.s3.throttled-retry-after` to
  `nessie.catalog.error-handling.throttled-retry-after`. The old property name is ignored.
- Helm chart: a few ADLS-specific options under `catalog.storage.adls` were incorrectly placed and
  therefore effectively ignored by Nessie; if you are using ADLS, please re-check your configuration
  and adjust it accordingly.

### New Features

- CLI: New `REVERT CONTENT` command to update one or more tables or views to a previous state.

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

### Commits
* Revert "[release] release nessie-0.93.0", but keep version
* Build: fix annotation types warnings (#9148)
* CI: Use Java 21 for CI (#9140)
* Release workflow: Let helm chart publishing depend on images publishing (#9142)
* Bump undertow from 2.2.28 to 2.2.33 (#9143)
* Bump Spark 3.4 from 3.4.2 to 3.4.3 (#9144)
* Bump Scala 2.13 from 2.13.13 to 2.13.14 (#9145)
* Revert "[release] release nessie-0.93.0"
* Add an Operating System check (#9139)
* Cache invalidations: move code to `:nessie-quarkus` (#9137)
* Catalog: decouple bucket name from bucket config key (#9116)
* Catalog/ADLS: change 'ping' endpoint (#9134)
* Add exception mappers to convert storage failures to Iceberg REST client exceptions (#8558)
* CLI: Nicer syntax rendering (#9119)
* CLI: Add `REVERT CONTENT` command (#9120)
* Catalog: update `IcebergManifestFileReader` to handle broken manifest files (#9132)
* Reference caching: update default for negative, update comments/docs (#9126)
* Catalog/ADLS: More information if manadory ADLS endpoint is missing (#9128)
* ninja: changelog
* GC: Manifest file reading with `specById` (#9131)
* Site: notes on Nessie server sizing + tips (#9127)
* Catalog / GCS: minor enhancements (#9107)
* Site: dynamo db note (#9113)
* GC: Record expiry exception in repository + record stack trace as well (#9114)
* Catalog: Don't expose ADLS + GCS credentials (#9100)
* Renovate: automerge action updates (#9106)
* Catalog: Accept object-store locations w/o trailing `/` (#9098)
* Catalog/ADLS: Don't let endpoint default to warehouse/object-store URI (#9102)
* Add `message` argument to `Objects.requireNonNull()` (#9099)
* GC: Fix behavior of cutoff policy "num commits", 'off by one' (#9096)
* Site: Fix links to nessie-bom (#9088)

## 0.92.1 Release (July 13, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.92.1).

### Fixes

- Catalog: fix field-ID reassignment and last-column-id calculation

### Commits
* Catalog: Fix assignment of nested fields in a table's initial schema (#9085)
* Update docs for CEL Authorization rules (#9078)
* HTTP client: consistently close streams and responses (#9082)
* HTTP client: remove hard-coded behavior for status >= 400 (#9071)
* Gradle 8.9 post updates (#9075)
* Use released Nessie image in docker-compose examples (#9070)

## 0.92.0 Release (July 11, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.92.0).

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

### Commits
* Fix Iceberg REST "getting started" examples (#9065)
* HTTP client: refactor BaseHttpRequest (#9055)
* Persist: make GenericObj implement Obj (#9059)
* Remove superfluous `\` in CLI welcome message (#9066)
* Make `o.p.client.http.Status` safe against unknown HTTP status codes (#9062)
* Export related objects (#9034)
* Catalog S3: use DefaultCredentialsProvider if no access key configured (#8987)
* nit: fix typo in changelog (#9051)
* Compatibility filter: bypass filters when contacting the config endpoint (#9050)
* Add ability to (de)serialize generic custom `ObjType`s (#9032)
* Refactor Nessie HTTP compatibility filter (#9047)
* Fix Docker Compose ports (#9046)
* Fix behavior of metadata-update/set-statistics + set-partition-statistics (#9045)
* SQL Extension SHOW LOG with AT (#8294)
* Committing operations: cleanup metadata after failures (#8889)
* Ability to identify related objects for Nessie export (#9033)
* Docs: Update REST catalog config for Trino (#9016)
* ninja: changelog
* Fix concurrency issue in flaky test `TestIceberg*Files.iceberg()` (#9039)
* Allow Smallrye config sections to have no properties (#9023)
* Catalog: Improve warehouse health/readiness checks (#9036)
* Add at least some reason to `Optional.orElseThrow()` (#9037)
* Nit: move EntityObj ID calculation from impl class (#9030)
* Unify obj-type namespaces (#9031)
* Fix older-client/server dependency resolution errors (#9029)
* Implement early access-checks for Iceberg REST operations (#8768)
* Export: remove ability to export a new repo using the legacy export version (#9017)
* GC/ADLS: Handle BlobNotFound (#9015)
* ninja: changelog
* Catalog/S3: Allow custom trust and key stores (#9012)
* Ninja: changelog
* Ninja: changelog updates
* Catalog/Bug: Fix double-write for S3 (#9008)
* Support using the default application credentials for google client (#9004)
* Docs: improve tabbed display of code blocks (#9006)
* Helm chart: fix advanced config examples (#9005)
* Refactor `MultiTableUpdate` (#9001)
* Exclude DNS test on macOS (#9003)
* Add `for-write` query parameter to Nessie API v2 (#8993)
* Move `AddressResolve` to a separate module, so it's reusable (#8992)
* Add initial unit tests for `CatalogServiceImpl` (#8998)
* Make address resolution more resilient (#8984)
* Add `returnNotFound` for `VersionStore.getValue(s)` (#8983)
* Adopt to change from Gradle 8.0 to 8.1 (#8985)
* Helm chart: don't call template if feature is disabled (#8982)
* Add `ObjectIO.deleteObjects()` (#8981)
* Site: add docs on Kubernetes troubleshooting (#8966)
* Fix potential class-loader deadlock via `Namespace.EMPTY` (#8963)
* Clean site directories and built-tools-IT directories during `clean` (#8969)
* Fix more IJ inspections (#8962)

## 0.91.3 Release (June 28, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.91.3).

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

### Commits
* Fix a bunch of IntelliJ inspections, rather nits + cosmetic (#8955)
* nit: fix typo in NessieConfigConstants (#8959)
* Site: display download options for each artifact in tabs (#8946)
* Just add comments when adding new images. (#8958)
* Nessie CLI: publish Docker images (#8935)
* Fix STDOUT in `ConnectCommand` (#8953)
* Remove unused `Cloud` enum (#8951)
* Fix flaky `TestCacheInvalidationSender.regularServiceNameLookups` (#8950)
* Helm chart: remove build-time property quarkus.log.min-level (#8948)
* Helm chart: fix catalogStorageEnv template (#8944)
* Clarify OpenAPI doc for updateRepositoryConfig (#8945)
* Migrate default bucket options to `default-options` composite property (#8933)
* Site: new placeholders for nightly version, tag and unstable suffix (#8939)
* Replace usage of `Project.extra` for `PublishingHelperPlugin` + explicitly disable Gradle config cache (#8942)
* create-gh-release-notes.sh: don't include h1 title in release notes (#8938)
* Site: nicer word-break for config references (#8936)
* Site / downloads fixes + reorg (#8934)
* Follow-up of stream-to-list refactor (#8931)
* OAuth2: nit: clarify trust relationship required for impersonation (#8928)
* GH WFs: Fix comment + job parameter description (#8924)

## 0.91.2 Release (June 24, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.91.2).

### Breaking changes

- We have improved Nessie client's support for impersonation scenarios using the token exchange
  grant type. A few options starting with `nessie.authentication.oauth2.token-exchange.*` were
  renamed to `nessie.authentication.oauth2.impersonation.*`. Check the [Nessie authentication
  settings] for details. Please note that token exchange and impersonation are both considered in
  beta state. Their APIs and configuration options are subject to change at any time.

### Commits
*  OAuth2Client: differentiate token exchange from impersonation (#8847)
* Migrate off deprecated `AwsS3V4Signer` to `AwsV4HttpSigner` (#8923)
* OAuthClient: improve next token refresh computation (#8922)
* Docker compose: expose Nessie management port (#8888)
* Site: update headers to appear in ToC (#8916)

## 0.91.1 Release (June 22, 2024)

See [Release information on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-0.91.1).

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

### Commits
* Fix flaky tests in `:nessie-gc-iceberg-files` (#8913)
* Fix patch version
* Ninja: bump sonatype timeout + bump snapshot-version for next release
* Revert release 0.91.0
* CI: split Quarkus intTest jobs (#8898)
* Site: exchange "black" Nessie (#8908)
* Fix Helm chart CI (#8900)
* NInja: changelog
* Possibly fix Java stream issuue w/ name resolution (#8899)
* Expose object store(s) availability as a readiness check (#8893)
* ADLS: advice against shared account name/key (#8895)
* Use correct IcebergView.of (#8896)
* Update nessie-IDs in returned metadata properties, simplify some code (#8879)
* Catalog/ADLS: Expose the endpoint and sas-token using the correct Iceberg properties (#8894)
* Add admin command: delete-catalog-tasks (#8869)
* TestOAuth2Authentication: fix flaky test (#8892)
* Metrics: support user-defined tags (#8890)
* Prevent console spam when connecting to a non-Iceberg-REST endpoint (#8885)
* Add the nice Nessie images (#8880)
* Add docs and docker-compose illustrating the use of a reverse proxy (#8864)
* Helm chart: support for REST Catalog (#8831)
* Docker compose: add examples with Prometheus and Grafana (#8883)
* Fix heading in index-release.md (#8881)
* Bump regsync from 0.4.7 to 0.6.1 (#8873)
* Fix test failure in `TestResolvConf` in macOS CI job (#8874)
* Make S3 signing work with Iceberg before 1.5.0 (#8871)
* Relax image registry sync (#8872)
* Disable Iceberg GC by default (#8870)
* AdlsConfig: remove unused option (#8868)
* Support v2 iceberg tables with gzip (#8848)
* OAuth2 client: improve error reporting (#8861)
* Use `search` in `resolv.conf` to resolve service names (#8849)
* REST Catalog: disable metrics by default (#8853)
* Proper `N commits since x.y.z` in release notes (#8823)
* Fix release-publish (#8822)
* Attempt to fix flaky test #8819 (#8821)
* Increase test worker heap size (#8820)

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
