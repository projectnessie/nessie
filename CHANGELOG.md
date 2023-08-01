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
- Nessie API spec upgraded to 2.1.1
- Support for relative hashes has been standardized and is now allowed in all v2 endpoints

### Deprecations

### Fixes

### Commits

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

[Unreleased]: https://github.com/projectnessie/nessie/compare/nessie-0.66.0...HEAD
[0.66.0]: https://github.com/projectnessie/nessie/compare/nessie-0.65.1...nessie-0.66.0
[0.65.1]: https://github.com/projectnessie/nessie/compare/nessie-0.65.0...nessie-0.65.1
[0.65.0]: https://github.com/projectnessie/nessie/compare/nessie-0.64.0...nessie-0.65.0
