# Nessie export/import

Functionality to export a Nessie repository and import into another Nessie repository, allowing to
migration from one backend database to another.

## Usage

Nessie repository export + import requires direct access to the database used by Nessie. The
necessary executable is the `nessie-quarkus-cli-x.y.z-runner.jar` can be downloaded from
the [release page on GitHub](https://github.com/projectnessie/nessie/releases) and is available
for Nessie 0.43.0 or newer.

The Nessie Quarkus CLI tool `nessie-quarkus-cli-x.y.z-runner.jar` should use the same configuration
settings as the Nessie Quarkus server.

### Exporting

The following command (replace `x.y.z` with the version you're using) exports your Nessie repository
to a single ZIP file called `my-export-file.zip`,

```bash
java -jar nessie-quarkus-cli-x.y.z-runner.jar export --path my-export-file.zip
```

A ZIP file export contains all necessary repository information in a single, compressed file.
Note that the export will only automatically generate a ZIP file, if the output path ends with
`.zip`, otherwise it will export to a directory. You can force either option using
the `--output-format` option.

!!! note
    Please use the following command for advanced options.
    ```bash
    java -jar nessie-quarkus-cli-x.y.z-runner.jar help export
    ```

### Importing

The following command (replace `x.y.z` with the version you're using) imports your Nessie repository
from a single ZIP file called `my-export-file.zip`,

```bash
java -jar nessie-quarkus-cli-x.y.z-runner.jar import --path my-export-file.zip
```

The import will fail, if the target Nessie repository exists and is not empty. If you intentionally
want to overwrite an existing Nessie repository, then use the `--erase-before-import` option.

!!! note
    Please use the following command for advanced options.
    ```bash
    java -jar nessie-quarkus-cli-x.y.z-runner.jar help import
    ```

## Building blocks

* Export functionality, based
  on [AbstractNessieExporter](https://github.com/projectnessie/nessie/blob/main/versioned/transfer/src/main/java/org/projectnessie/versioned/transfer/AbstractNessieExporter.java)
  to dump commits, named references, heads+fork points.
* Import functionality, based
  on [AbstractNessieImporter](https://github.com/projectnessie/nessie/blob/main/versioned/transfer/src/main/java/org/projectnessie/versioned/transfer/AbstractNessieImporter.java)
  to load the exported data.
* Commit log optimization to:
  * populate the list of parent-commits in all commits, according to the target Nessie repository's
    configuration
  * populate the key-lists in the commits, according to the target Nessie repository's configuration

## Code examples

```java
class CodeExamples {

  void exportExample(Persist persist, Path exportZipFile) {

    ZipArchiveExporter.builder()
      .outputFile(exportZipFile)
      .persist(persist)
      .build()
      .exportNessieRepository();
  }

  void importExample(Persist persist, Path importZipFile) {

    ImportResult importResult =
      ZipArchiveImporter.builder()
        .sourceZipFile(importZipFile)
        .persist(persist)
        .build()
        .importNessieRepository();
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

Exported commits do _not_ contain key-lists or commit-parents or the like, because that is
rather internal, implementation specific information and, additionally, the configuration of the
target repository that controls the aggregated key-lists and commit-parent-lists might be different
from the source repository.

However, exported commits do contain information about the commit-sequence-number and the technical
created-at-timestamp.

!!! note
    The `nessie-quarkus-cli` tool's `import` command performs a commit-log optimization after all
    commits and named references have been created. This optimization populates missing
    aggregated key-lists and commit-parents. Running commit-log optimization is necessary for good
    performance to access contents and commit logs, but not strictly necessary.
    Commit-log optimization can be disabled.

## Export contents consistency

Any Nessie export guarantees that the commits referenced by the _named_ references and all their
parent commits are contained in the exported data.

A Nessie export _may_ contain unreferenced commits, for example commits that have been created
while the export is running or commits that are otherwise unreferenced.

The `HEAD`s of the _named_ references and the heads in the `HeadsAndForks` structure may not be
consistent, for example when commits have been created while the export is running.

## Export formats

Exported data can be written either into an empty directory or as a compressed zip file.

Users can optionally zip the contents of an export to a directory and pass that to the zip-file
based importer.
