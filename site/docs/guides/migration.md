# Migration Guide

The [Nessie Quarkus CLI tool] can be used to migrate a Nessie repository from one version store type 
to another, for example from MongoDB to Postgres. It can also be used to migrate from a legacy 
version store type to a new version store type.

!!! note
    The Nessie Quarkus CLI tool is an executable jar that can be used to interact with a Nessie
    database directly. It should not be confused with the [Nessie CLI tool], which is a Python
    tool that is used to interact with Nessie servers.

[Nessie Quarkus CLI tool]: ../nessie-latest/export_import.md
[Nessie CLI tool]: ../nessie-latest/cli.md

## Prerequisites

Because support for legacy version store types was removed in Nessie 0.75, the migration process
from a Nessie version < 0.75 to a Nessie version >= 0.75 requires a full repository migration
**prior to upgrading the Nessie server**.

The migration process **requires downtime of the Nessie server**. The Nessie server must be stopped
before the migration process starts and must not be started until the migration process has
successfully completed.

## Step-by-step guide

In this example we will assume an existing Nessie repository that uses the legacy version store
type `MONGO` and we want to migrate to the new version store type `MONGODB`.

!!! note
    Even though the version store types are similar and backed by the same database (MongoDB in our
    example), the migration process still requires a full export of the Nessie repository and a full 
    import into the new version store type, because the internal data structures are different.

Download the `nessie-quarkus-cli-x.y.z-runner.jar` corresponding to the **old** Nessie version, here 
we assume version 0.71.0:

```bash
curl -L https://github.com/projectnessie/nessie/releases/download/nessie-0.71.0/nessie-quarkus-cli-0.71.0-runner.jar -o nessie-quarkus-cli-0.71.0-runner.jar
```

Stop the Nessie server(s). 

Export the legacy Nessie repository to a ZIP file. In this example we assume that the MongoDB 
database is called `nessie-legacy` and is hosted on a MongoDB instance available at 
`nessie.example.com:27017`:

```bash
java \
  -Dnessie.version.store.type=MONGO \
  -Dquarkus.mongodb.database=nessie-legacy \
  -Dquarkus.mongodb.connection-string=mongodb://<user>:<password>@nessie.example.com:27017 \
  -jar nessie-quarkus-cli-0.71.0-runner.jar \
  export \
  --path "/tmp/export-$(date +'%Y-%d-%m').zip" \
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

Download the `nessie-quarkus-cli-x.y.z-runner.jar` corresponding to the **new** Nessie version, here
we assume version 0.75.0:

```bash
curl -L https://github.com/projectnessie/nessie/releases/download/nessie-0.75.0/nessie-quarkus-cli-0.75.0-runner.jar -o nessie-quarkus-cli-0.75.0-runner.jar
```

Create the MongoDB database to host the new Nessie repository. In this example we assume that the 
MongoDB database is called `nessie` and is hosted on the same MongoDB instance as the legacy Nessie
repository.

Import the ZIP file into the new Nessie repository:

```bash
java \
  -Dnessie.version.store.type=MONGODB \
  -Dquarkus.mongodb.database=nessie \
  -Dquarkus.mongodb.connection-string=mongodb://nessie.example.com:27017 \
  -jar nessie-quarkus-cli-0.75.0-runner.jar import \
  --path "/tmp/export-$(date +'%Y-%d-%m').zip" \
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

Start the Nessie server(s).
