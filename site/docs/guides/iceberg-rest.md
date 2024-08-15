# Configure Nessie with Iceberg REST

!!! warn
    Support for Iceberg REST is currently considered experimental in Nessie!

With Iceberg REST, Nessie manages the metadata of the tables and views. On top, Nessie provides
mechanisms like S3 request signing and S3 session credentials. Using Nessie with Iceberg
therefore requires Nessie to have access to your object store.

The object stores used by tables and views need to be defined in the Nessie configuration, for
example using the following configuration example snippet:

!!! warn
    Nessie requires at least one object store and one warehouse to be configured before you can
    use Nessie's Iceberg REST integration.

```properties
# Default/global S3 configuration settings
nessie.catalog.service.s3.default-options.region=us-west-2
nessie.catalog.service.s3.default-options.access-key.name=awsAccessKeyId
nessie.catalog.service.s3.default-options.access-key.secret=awsSecretAccessKey
# For non-AWS S3 you need to specify the endpoint and possibly enable path-style-access
nessie.catalog.service.s3.default-options.endpoint=http://localhost:9000
nessie.catalog.service.s3.default-options.path-style-access=true

# S3 configuration settings that are different for "bucket1"
nessie.catalog.service.s3.buckets.bucket1.access-key.name=awsAccessKeyId1
nessie.catalog.service.s3.buckets.bucket1.access-key.secret=awsSecretAccessKey1
nessie.catalog.service.s3.buckets.bucket1.region=us-east-1
```

See [Server configuration Reference](../nessie-latest/index.md).

!!! note
    Up to Nessie including version 0.91.2 the above property names had to be specified without the
    `default-options.` part.

!!! tip
    Secrets can be encrypted using
    [config encryption available in Quarkus](https://quarkus.io/guides/config-secrets#encrypt-configuration-values).
    We do plan to support other secrets management systems.

!!! info
    The above S3 credentials (access key ID + secret access key) are never passed exposed to a client.

!!! info
    GCS and ADLS object store can be configured, but are considered "experimental" at the moment.

In addition to an object store, see the S3 example above, Nessie needs at least a default warehouse
to be configured.

```properties
nessie.catalog.default-warehouse=warehouse
nessie.catalog.warehouses.warehouse.location=s3://mybucket/my-lakehouse/
```

## Seamless migration from "Nessie" to "Nessie with Iceberg REST"

You can safely use your current Nessie applications, those that use `type=nessie` when using Iceberg,
concurrently with applications using Nessie via Iceberg REST (`type=rest` with a URI like
`uri=http://127.0.0.1:19120/iceberg`).

## Migrate an Iceberg client configuration

To migrate existing Iceberg clients that use the `NessieCatalog` to use Nessie via Iceberg REST refer
to the following table.

| Iceberg option             | Old value                     | New value                    | Description/notes                                                                                                                                                                                                                                   |
|----------------------------|-------------------------------|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `type`                     | `nessie`                      | `rest`                       | Change the catalog type from "nessie" to "rest".                                                                                                                                                                                                    |
| `catalog-impl`             | `...NessieCatalog`            |                              | Use `type` = `rest`                                                                                                                                                                                                                                 |
| `uri`                      | `http://.../api/v2` (or `v1`) | `http://.../iceberg`         | Replace `api/v1` or `api/v2` with `iceberg`. If you want to connect to Nessie using a different branch, append the branch or tag name to the `uri` parameter, for example: `http://.../iceberg/my_branch`.                                          |
| `ref`                      | Nessie branch name            | n/a                          | Migrate to `prefix` option                                                                                                                                                                                                                          |
| `prefix`                   | n/a                           | (don't set, see description) | Don't set this for Nessie. If you want to connect to Nessie using a different branch, append the branch or tag name to the `uri` parameter, for example: `http://.../iceberg/my_branch`. Setting the `prefix` parameter doesn't work for pyiceberg. |
| `warehouse`                | *                             | n/a                          | Migrate object store configurations to the Nessie server configuration                                                                                                                                                                              |
| `io`                       | *                             | n/a                          | Migrate object store configurations to the Nessie server configuration                                                                                                                                                                              |
| (all S3/GCS/ADLS settings) |                               |                              | Remove all object store settings                                                                                                                                                                                                                    |

!!! warn
    Current Iceberg REST clients do not support the OAuth2 authorization code and device code flows,
    like Nessie does!

    Only Bearer and client-ID/secret work. We recommend bearer tokens over client-ID/client-secret
    configuration to not put those credentials at the risk of being compromised - it is easier to
    revoke a single bearer token than to change a password used by many applications. Keep in mind
    that bearer token are only valid for some given period of time.

    We want to contribute the advanced OAuth2 functionality that already exists for Nessie client
    to Apache Iceberg. 

## Using "object storage" file layout

Nessie respects the `write.object-storage.enabled=true` setting. With Nessie, it is _not_ necessary
to set the `write.data.path` (or `write.object-storage.path` or `write.folder-storage.path`), because
Nessie automatically returns the table property `write.data.path` set to the warehouse location.

Both S3 request signing and credentials vending ("assume role") work with `write.object-storage.enabled`.

## Noteworthy

* The (base) location of tables created via Iceberg REST are mandated by Nessie, which will choose
  the table's location underneath the location of the warehouse.
* Changes to the table base location are ignored.
* Nessie will always return only the Iceberg table snapshot that corresponds to the Nessie commit.
  This solves the mismatch between Nessie commits and Iceberg snapshot history. Similarly Nessie
  returns the Iceberg view version corresponding to the Nessie commit.

## Nessie CLI

The [Nessie CLI](../nessie-latest/cli.md) has been enhanced with basic support for Iceberg REST when
used with Nessie. It will transparently connect with Iceberg REST as well. You can also use the
Iceberg REST base URI instead of the Nessie REST base URI.

!!! tip
    If you use `CONNECT TO http://127.0.0.1:19120/api/v2 USING "nessie.authentication.type" = BEARER`,
    Nessie CLI will prompt you for the bearer token.
    If you use `CONNECT TO http://127.0.0.1:19120/api/v2 USING "token" = "<bearer token>""`,
    Nessie CLI will use bearer authorization for both Nessie and Iceberg REST APIs.

## Time travel with Iceberg REST

Nessie provides catalog level versioning providing a consistent and reproducible state across the
whole catalog. In other words: atomic transactions across many tables and views and namespaces are
natively built into Nessie. This "Git for data" approach allows, for example, merging all changes
to tables and views in a branch into another branch, for example the "main" branch.

To retain Nessie's consistency and cross-branch/tag isolation guarantees, we have deliberately chosen
to only return the state of a table or view as a _single_ snapshot in Iceberg.

You can still do time-travel queries by specifying the Nessie commit ID, or the branch/tag name, or
a timestamp.

Assuming you connect to Nessie using Iceberg REST with `prefix` set to `main` and you have a table
called `my_table`.

### Time travel

The following example `SELECT`s the state/contents of the table `my_namespace.my_table` as of
July 1st, 2024 at midnight UTC.

Generally:
* the table name must be quoted using backticks (`)
* specify a branch or tag name after the at-char (`@`) (for example `table@branch`)
* specify a timestamp after the hash-char (`#`) (for example `table#2024-07-01T00:00:00Z`) or
* specify a Nessie commit ID after the hash-char (`#`) (for example `table#748586fa39e02bd1e359df105c6c08287ad5ed7a53235f71c455afb10fbff14c`) 

```sql
SELECT * FROM nessie.my_namespace.`my_table#2024-07-01T00:00:00Z`;
```

Similarly, but for the time zone at offset -09:00:

```sql
SELECT * FROM nessie.my_namespace.`my_table#2024-07-01T00:00:00-09:00`;
```

Read from a different Nessie branch or tag:

```sql
SELECT * FROM nessie.my_namespace.`my_table@my_other_branch`;
```

Read from a different Nessie branch or tag at a specific timestamp:

```sql
SELECT * FROM nessie.my_namespace.`my_table@my_other_branch#2024-07-01T00:00:00Z`;
```

You can also specify Nessie commit IDs:

```sql
SELECT * FROM nessie.my_namespace.`my_table#748586fa39e02bd1e359df105c6c08287ad5ed7a53235f71c455afb10fbff14c`;
```

`INSERT`ing or `UPDATE`ing data works similarly:

```sql
INSERT INTO nessie.my_namespace.`my_table@my_other_branch` ( id, val ) VALUES ( 123, 'some value' );
```

`CREATE`ing a table on a different branch:

```sql
CREATE TABLE nessie.my_namespace.`my_table@my_other_branch` ( id INT, val VARCHAR );
```

## Customizing Nessie commit author et al

It is possible to specify the author(s) and signed-off-by fields recorded in Nessie commits via
Iceberg REST by using these Nessie specific REST/HTTP headers.

| Header                      | Meaning                                                                                                                                                        |
|-----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Nessie-Commit-Authors`     | Comma separated list of authors to record and show in Nessie commit log. Example: `My Name <name@domain.internal>, Other user <other@company.internal>`.       |
| `Nessie-Commit-SignedOffBy` | Comma separated list of signed-off-by to record and show in Nessie commit log. Example: `My Name <name@domain.internal>, Other user <other@company.internal>`. |
| `Nessie-Commit-Message`     | Custom commit message, overrides all commit messages - use with care, because generated commit messages contain useful information.                            |
