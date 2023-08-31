# Nessie Changelog
The Nessie changelog is used to give users and contributors more information than just the list of commits.
Entries are grouped in sections like _Highlights_ or _Upgrade notes_, the provided sections can be adjusted
as necessary. Empty sections will not end in the release notes.

## [Unreleased]

### Highlights

### Upgrade notes

### Breaking changes

### New Features
- Content Generator tool: added new `--hash` parameter to `commits`, `content` and `entries` 
  commands.

### Changes
- Content Generator tool: commit hashes are now printed in full when running the `commits` command.
- For a "get-keys" operation that requests the content objects as well, the content objects are now
  fetched using bulk-requests.

### Deprecations

### Fixes
- Fixed potential index corruption when importing repositories into the new storage model that 
  could cause some contents to become inaccessible.

### Commits

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

[Unreleased]: https://github.com/projectnessie/nessie/compare/nessie-0.69.0...HEAD
[0.69.0]: https://github.com/projectnessie/nessie/compare/nessie-0.68.0...nessie-0.69.0
[0.68.0]: https://github.com/projectnessie/nessie/compare/nessie-0.67.0...nessie-0.68.0
[0.67.0]: https://github.com/projectnessie/nessie/compare/nessie-0.66.0...nessie-0.67.0
[0.66.0]: https://github.com/projectnessie/nessie/compare/nessie-0.65.1...nessie-0.66.0
[0.65.1]: https://github.com/projectnessie/nessie/compare/nessie-0.65.0...nessie-0.65.1
[0.65.0]: https://github.com/projectnessie/nessie/compare/nessie-0.64.0...nessie-0.65.0
