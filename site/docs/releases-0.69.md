# Release Notes 0.50.0 to 0.69.2

**See [Nessie Server upgrade notes](server-upgrade.md) for supported upgrade paths.**

## Recent releases

**See [Main Release Notes page](./releases.md) for the most recent releases.**

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

## Older releases

See [this page for 0.49.0 and older](./releases-0.49.md)
