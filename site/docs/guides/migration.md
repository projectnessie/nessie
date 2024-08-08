# Migration & Backup/Recovery

The [Nessie Server Admin tool] can be used to migrate a Nessie repository from one version store type 
to another, for example from MongoDB to Postgres. It can also be used to migrate from a legacy 
version store type (before Nessie 0.75) to a new version store type. The export/import functionality
can also be used for backup & recovery use cases.

!!! note
    The Nessie Server Admin tool is an executable jar, also available as a Docker image, that can be
    used to interact with a Nessie database directly. It should not be confused with the
    [Nessie CLI tool], which is a tool that is used to interact with Nessie servers.

    In Nessie versions before 0.83.2, the _Nessie Server Admin tool_ was called _Nessie Quarkus CLI_.

[Nessie Server Admin tool]: ../nessie-latest/export_import.md
[Nessie CLI tool]: ../nessie-latest/cli.md

## Use cases

There are two major use cases for "export"/"import":

### Backup / recovery use case

To export a Nessie repository for the backup/recovery use case, it is not necessary to stop the
Nessie servers, if you are using a Nessie Server admin tool version since 0.94.0.

With Nessie Server admin tool versions before 0.94.0, you **must** stop the Nessie servers
before starting the export, otherwise the exported data will be inconsistent.

For recovery ("import") it is mandatory that the Nessie servers are not running.

### Migration use case

The migration process naturally **requires downtime of the Nessie server**. Nessie Servers must
be stopped before starting the export to ensure that all information from the source repository
is available in the target repository.

The process for the migration use case in short:

1. Stop all Nessie servers
2. Export from the source repository
3. Upgrade Nessie to the latest version
4. Import into the target repository
5. Configure Nessie servers to use the target repository
6. Start all Nessie servers

!!! warn
    Because support for _legacy_ version store types was removed in Nessie 0.75, the migration
    process from a Nessie version < 0.75 to a Nessie version >= 0.75 requires a full repository
    migration **prior to upgrading the Nessie server**.

    Make sure to use the correct _legacy_ version store types for the export and the correct
    version store type for the import.

    If you want to migrate _from_ a Nessie version older than 0.75, you **must** use the
    `nessie-quarkus-cli` from that version. Download links are available on our [GitHub releases
    page](https://github.com/projectnessie/nessie/releases).

## Step-by-step guide

Except for the migration use case from Nessie before version 0.75, it is safe to use the latest
version of Nessie Server admin tool. You can find the downloads for a specific version on the
[Docs](/nessie-latest/) site.

### Stop the Nessie server(s).

When using a Nessie Server admin tool version before 0.94.0, you **must always** stop the Nessie
servers before starting the export, otherwise the exported data will be inconsistent.

If you want to export a repository for export for backup/recovery _and_ you are using Nessie
Server admin tool version since 0.94.0, the Nessie servers do not need to be stopped. 

### Export

Export the source Nessie repository to a ZIP file. In this example we assume the MongoDB 
database is called `nessie-source` and is hosted on a MongoDB instance available at 
`nessie.example.com:27017`:

See [Nessie Server Admin tool reference docs](/nessie-latest/export_import) for details.

=== "Standalone Jar"
    Running the Nessie Server Admin tool standalone jar requires Java 17 or newer.

    ```bash
    curl -L -o nessie-server-admin-tool-{{ versions.nessie }}-runner.jar \
      https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-server-admin-tool-{{ versions.nessie }}-runner.jar
    ```

    ```bash
    java \
      -Dnessie.version.store.type=MONGODB \
      -Dquarkus.mongodb.database=nessie-source \
      -Dquarkus.mongodb.connection-string=mongodb://<user>:<password>@nessie.example.com:27017 \
      -jar  nessie-server-admin-tool-{{ versions.nessie }}-runner.jar \
      export \
      --path "/tmp/export-$(date +'%Y-%d-%m').zip" \
      --commit-batch-size 1000
    ```
=== "Docker Image"
    The Docker image is available for Nessie Server admin tool since version 0.83.1.

    With the below settings, the export file(s) are going to be placed in a directory
    named `export-data`, which is mapped as `/data` in the Docker container.

    ```bash
    mkdir -p export-data

    docker run --rm -ti \
      --volume ./export-data:/data \
      -e nessie.version.store.type=MONGODB \
      -e quarkus.mongodb.database=nessie-source \
      -e quarkus.mongodb.connection-string=mongodb://<user>:<password>@nessie.example.com:27017 \
      ghcr.io/projectnessie/nessie-server-admin:{{ versions.nessie }} \
      export \
      --path "/data/export-$(date +'%Y-%d-%m').zip" \
      --commit-batch-size 1000
    ```

The `--commit-batch-size` option generally improves performance, but is not required.

The export process will take some time, depending on the size of the Nessie repository. You should 
see something like this:

```text
Exporting from a MONGO version store...
Exporting commits...
..........
100 commits exported.

Exporting named references...
1 named references exported.

Exported Nessie repository, 100 commits into 1 files, 1 named references into 1 files.
```

Once the
export process has completed, the ZIP file will be available at `/tmp/export-<date>.zip`.

### Import

Download the `nessie-server-admin-tool-x.y.z-runner.jar` corresponding to the **new** Nessie version, here
we assume version {{ versions.nessie }} (note: older versions have this tool under the name of `nessie-quarkus-cli`):

Create/setup the database to host the new target Nessie repository. In this example we assume that
a MongoDB database is called `nessie` and is hosted on the same MongoDB instance as the source Nessie
repository.

Refer to the [Server Configuration](/nessie-latest/configuration/) reference docs for the respective
Nessie version.

Import the ZIP file into the new Nessie repository:

See [Nessie Server Admin tool reference docs](/nessie-latest/export_import) for details.

=== "Standalone Jar"
    Running the Nessie Server Admin tool standalone jar requires Java 17 or newer.

    ```bash
    curl -L -o nessie-server-admin-tool-{{ versions.nessie }}-runner.jar \
      https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-server-admin-tool-{{ versions.nessie }}-runner.jar
    ```

    ```bash
    java \
      -Dnessie.version.store.type=MONGODB \
      -Dquarkus.mongodb.database=nessie \
      -Dquarkus.mongodb.connection-string=mongodb://nessie.example.com:27017 \
      -jar nessie-server-admin-tool-{{ versions.nessie }}-runner.jar \
      import \
      --path "/tmp/export-$(date +'%Y-%d-%m').zip" \
      --commit-batch-size 1000
    ```
=== "Docker Image"
    The Docker image is available for Nessie Server admin tool since version 0.83.1.

    With the below settings, the export file(s) are expected to be in a directory named
    `export-data`, which is mapped as `/data` in the Docker container.
    See the [Export](#export) section above.

    ```bash
    docker run --rm -ti \
      --volume ./export-data:/data \
      -e nessie.version.store.type=MONGODB \
      -e quarkus.mongodb.database=nessie-source \
      -e quarkus.mongodb.connection-string=mongodb://<user>:<password>@nessie.example.com:27017 \
      ghcr.io/projectnessie/nessie-server-admin:{{ versions.nessie }} \
      import \
      --path "/data/export-$(date +'%Y-%d-%m').zip" \
      --commit-batch-size 1000
    ```

The `--commit-batch-size` option generally improves performance, but is not required.

The import process will take some time, depending on the size of the Nessie repository. You should
see something like this:

```text
Importing into a MONGODB version store...
Export was created by Nessie version 0.71.0 on 2023-12-27T11:50:10.547Z, containing 1 named references (in 1 files) and 100 commits (in 1 files).
Preparing repository...
Importing 100 commits...
..........
100 commits imported, total duration: PT0.113285209S.

Importing 1 named references...
1 named references imported, total duration: PT0.018604083S.

Finalizing import...
..........
Import finalization finished, total duration: PT0.217375166S.

Imported Nessie repository, 100 commits, 1 named references.
Total duration: PT0.632381167S.
```

Once the import process has completed, the new Nessie repository is ready to be used. Change the 
Nessie server configuration to use the new version store type and the new MongoDB database by
modifying the `application.properties` file (or equivalent environment variables) to contain:

```properties
nessie.version.store.type=MONGODB
quarkus.mongodb.database=nessie
```

### Start the Nessie server(s).

Once the import completed, you can start your Nessie server(s) against the target version store
(database).
