# Nessie export/import

Functionality to export a Nessie repository and later import into another, empty Nessie repository.

## Building blocks

* Export functionality, based
  on [AbstractNessieExporter](./src/main/java/org/projectnessie/versioned/transfer/AbstractNessieExporter.java)
  to dump commits, named references, heads+fork points.
* Import functionality, based
  on [AbstractNessieImporter](./src/main/java/org/projectnessie/versioned/transfer/AbstractNessieImporter.java)
  to load the exported data.
* Commit log optimization to
  * populate the list of parent-commits in all commits, according to the target Nessie repository's
    configuration
  * populate the key-lists in the commits, according to the target Nessie repository's configuration

## Code examples

```java
class CodeExamples {
  void exportExample() {
    Path exportZipFile;

    DatabaseAdapter databaseAdapter;

    ZipArchiveExporter.builder()
      .outputFile(exportZipFile)
      .databaseAdapter(databaseAdapter)
      .build()
      .exportNessieRepository();
  }

  void importExample() {
    Path exportZipFile;

    DatabaseAdapter databaseAdapter;

    ImportResult importResult =
      ZipArchiveImporter.builder()
        .sourceZipFile(exportZipFile)
        .databaseAdapter(importDatabaseAdapter)
        .build()
        .importNessieRepository();

    CommitLogOptimization.builder()
      .headsAndForks(importResult.headsAndForkPoints())
      .databaseAdapter(databaseAdapter)
      .build()
      .optimize();
  }
}
```

`ZipArchiveImporter` can be replaced with `FileImporter`.

## Export contents

Each export contains this information:

* All commits (no specific order)
* All named references including their heads
* Heads + fork-points (used to feed commit-log optimization ran after a repository import)
* Summary and inventory

## `Content`, `CommitMeta`, global state, et al

A Nessie export contains all `Content` information without any database internal information. This
means that there is no information contained whether the source repository stored `Content` using
e.g. global state. All `Content`s and `CommitMeta` are exported in their public JSON representation.

As a side effect, an export from a Nessie repository with commits that were persisted using global
state will be imported using on-reference-state. However, for content that was persisted using
global state, there will multiple on-reference-states referring to the same Iceberg table-metadata.

## Technical commit information

Exported commits to _not_ contain key-lists or commit-parents or the like. But exported commits
have information about the commit-sequence-number and the technical create-at-timestamp.

## Export contents consistency

Any Nessie export guarantees that the commits referenced by the _named_ references and all their
parent commits are contained.

A Nessie export _may_ contain unreferenced commits, for example commits that have been created
while the export is running or commits that are otherwise unreferenced.

The HEADs of the _named_ references and the heads in the `HeadsAndForks` structure may not be
consistent, for example when commits have been created while the export is running.

## Export formats

Exports can be generated into either an empty directory or as a compressed zip file.

Users can optionally zip the contents of an export to a directory and pass that to the zip-file
based importer.
