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
# Name of the default warehouse
nessie.catalog.default-warehouse=warehouse

# The base location of the warehouse named "warehouse"
nessie.catalog.warehouses.warehouse.location=s3://my-bucket

# Another warehouse named "sales"
nessie.catalog.warehouses.sales.location=s3://sales-data

# Default/global S3 configuration settings
nessie.catalog.service.s3.default-options.region=us-west-2
nessie.catalog.service.s3.default-options.access-key=urn:nessie-secret:quarkus:my-secrets-default
my-secrets-default.name=awsAccessKeyId
my-secrets-default.secret=awsSecretAccessKey
# For non-AWS S3 you need to specify the endpoint and possibly enable path-style-access
nessie.catalog.service.s3.default-options.endpoint=http://localhost:9000
nessie.catalog.service.s3.default-options.path-style-access=true

# S3 configuration settings that are different for "bucket1"
nessie.catalog.service.s3.buckets.sales.access-key=urn:nessie-secret:quarkus:my-secrets-for-sales
my-secrets-for-sales.name=awsAccessKeyIdForSales
my-secrets-for-sales.secret=awsSecretAccessKeyForSales
nessie.catalog.service.s3.buckets.sales.region=us-east-1
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

## Warehouses & Storage Locations

Iceberg stores data in object stores like S3, GCS or ADLS. Nessie pushes the necessary configuration to
Iceberg clients via Iceberg REST. This information includes the object store type (Iceberg `FileIO`), the
configuration for this and short-lived and down-scoped credentials.

The object storage used for a table is identified by the `location` Iceberg table metadata property,
considering the scheme (`s3`/`s3a`/`s3n`, `gs` or `abfs`/`abfss`) and the bucket/file-system name.

"Warehouses" are simply speaking named object storage locations (or the default warehouse's storage
location). Warehouses are relevant in Nessie when creating new tables and to provide the `location`
namespace property.

!!! info
    The currently open issues [#9331](https://github.com/projectnessie/nessie/issues/9331),
    [#9558](https://github.com/projectnessie/nessie/issues/9558) and
    [#9559](https://github.com/projectnessie/nessie/issues/9559) will further allow both more flexibility
    but also more security with respect to storage locations.

### Pre-existing tables

In Iceberg it is possible, although maybe not very likely, that tables within the same catalog use
different (base) locations - one table may "live" for example in `s3://my-bucket/` while another "lives"
in `gs://other-data/` and yet another in `s3://cold-data-bucket/`, potentially mixing one bucket in AWS S3
with a another bucket in a self-hosted Minio. 

### "Warehouse"

The term "warehouse" stems from Iceberg's meaning of a warehouse, which in turn comes from Spark's
terminology for a "warehouse", which effectively means the (default) storage location.

A "warehouse" in Nessie is a _named_ storage location - this "warehouse name" is configured by the user
in the Iceberg REST client using the `uri` parameter. If the `uri` does not define a specific "warehouse",
Nessie uses the default "warehouse" configured for Nessie.

Each "warehouse" in Nessie defines a (base) storage location, for example `s3://my-bucket/`.

Iceberg configuration defaults and overrides can be configured for each warehouse.

!!! note
    See above for an example Nessie configuration.

#### Example Iceberg REST `uri` parameters

* to connect to the default warehouse: `http://127.0.0.1:19120/iceberg`
* to connect to the warehouse named "pii": `http://127.0.0.1:19120/iceberg/|pii` - note the mandatory `|` character
* to connect to the warehouse named "sales" using the Nessie branch "experiments": `http://127.0.0.1:19120/iceberg/experiments|sales`

!!! note
    It is mandatory to configure the default (unnamed) warehouse and the storage location that this warehouse
    references.

### Creating new tables

The default storage `location` when creating a table is determined from the current "warehouse" and the
namespace in which the table is being created.

Examples, considering the above example Nessie configuration, assuming that a namespace `foo` exists:

* Creating a table named `foo.fancy_table` when connecting to the default warehouse, would set the `location`
  of that table by default to `s3://my-bucket/foo/fancy_table_<random-UUID>`
* Creating a table named `foo.more_data` when connecting to the warehouse named "sales", would set the
  `location` of that table by default to `s3://sales-data/foo/more_data_<random-UUID>`

!!! note
    After the table has been created using Iceberg's "staged table creation workflow", it is not possible
    to change the table's `location` property via Nessie.

### Object Storage Configurations

The Nessie configuration allows configuring default settings for each object storage type (S3, GCS, ADLS).
Bucket (or ADLS file-system) specific settings can be configured as well.

The effective configuration for an object storage location is determined by looking up the bucket specific
settings. Configuration options that have not been explicitly configured in the bucket specific settings,
will be taken from the default settings. If no bucket specific settings exists, only the default settings
will be used.

For example, considering the above example Nessie configuration:

* For a table `location` starting with `s3://my-bucket/`, the S3 configuration will use the endpoint
  `http://localhost:9000` using path-style access and the region `us-west-2`.
* For a table `location` starting with `s3://sales-data/`, the S3 configuration will also use the endpoint
  `http://localhost:9000` using path-style access but the region `us-east-1`.

### Namespaces

Some query engines require the `location` property to be set on namespaces. Nessie always returns the
`location` property for every namespace. If the `location` has not been explicitly configured, it defaults
to the current warehouse's storage location plus the namespace elements separated by slashes. If one of the
parent namespaces defines the `location` property, the "remaining" path parts for the requested namespace
are appended to that.

### Accessing a table

When clients access a table, Nessie uses the `location` table property to find the object storage configuration
by matching the `location` property against the object storage configurations. The "warehouse" is irrelevant
for the _process_ of _looking up_ the object storage configuration.

Based on the object storage configuration, Nessie returns the necessary configuration to access the table
by providing the matching Iceberg `FileIO` settings and, if configured, the down-scoped credentials.

For example, considering the above example Nessie configuration:

* Loading the table metadata for a table having `location` set to `s3://my-bucket/foo/fancy_table`, Nessie
  returns the `S3FileIO` type configured for the endpoint `http://localhost:9000` using path-style access and
  the region `us-west-2`.
* Loading the table metadata for a table having `location` set to `s3://sales-data/foo/more_data`, Nessie
  returns the `S3FileIO` type configured for the endpoint `http://localhost:9000` using path-style access but
  the region `us-east-1`.

### Object store credentials

The short-lived and down-scoped credentials that Nessie provides to clients according to the privileges the
client has been granted ("authorization"). Alternatively, Nessie allows using S3 signing, which provides the
same guarantees.

!!! warn
    Some query engines clients _may_ request either only S3 request signing or vended credentials. It is
    important to configure/enable those mechanisms tailored for your specific use case / environment.

!!! Info
    See [Server configuration Reference](../nessie-latest/index.md) for details on how to configure
    object store credentials for Iceberg clients.

!!! warn
    ADLS does not have a concept of IAM/STS like S3 or GCS. ADLS only provides the ability to restrict
    list/read/write/add/delete privileges for the _whole_ file system, while S3 and GCS allow much finer
    grained access control on path as well. This is a restriction imposed on us by Azure.

### Vended/down-scoped credentials vs request signing

Credential vending requires Nessie to ask an STS service (AWS, Google Cloud and some alternative S3
implementations like Minio) to generate object storage credentials with a privilege set matching the client's
privileges on that table.

S3 request signing on the other hand does not require any STS service, instead each individual request to
S3 has to be signed by Nessie. This requires a REST call for every S3 request.

Although credential vending requires an STS round-trip when loading the table-metadata, it does not require
any additional round trips and is overall the faster and less resource intensive approach.

S3 request signing might be the only option, if your S3 implementation does not provide an STS service.

## Seamless migration from "Nessie" to "Nessie with Iceberg REST"

You can safely use your current Nessie applications, those that use `type=nessie` when using Iceberg,
concurrently with applications using Nessie via Iceberg REST (`type=rest` with a URI like
`uri=http://127.0.0.1:19120/iceberg`).

## Migrate an Iceberg client configuration

To migrate existing Iceberg clients that use the `NessieCatalog` to use Nessie via Iceberg REST refer
to the following table.

| Iceberg option             | Old value                     | New value                    | Description/notes                                                                                                                                                                                                                                                                                                                                          |
|----------------------------|-------------------------------|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `type`                     | `nessie`                      | `rest`                       | Change the catalog type from "nessie" to "rest".                                                                                                                                                                                                                                                                                                           |
| `catalog-impl`             | `...NessieCatalog`            |                              | Use `type` = `rest`                                                                                                                                                                                                                                                                                                                                        |
| `uri`                      | `http://.../api/v2` (or `v1`) | `http://.../iceberg`         | Replace `api/v1` or `api/v2` with `iceberg`. If you want to connect to Nessie using a different branch, append the branch or tag name to the `uri` parameter, for example: `http://.../iceberg/my_branch`. To use a different warehouse (default storage location), append the `\|` character followed by then name of the warehouse configured in Nessie. |
| `ref`                      | Nessie branch name            | n/a                          | Migrate to `prefix` option                                                                                                                                                                                                                                                                                                                                 |
| `prefix`                   | n/a                           | (don't set, see description) | Don't set this for Nessie. If you want to connect to Nessie using a different branch, append the branch or tag name to the `uri` parameter, for example: `http://.../iceberg/my_branch`. Note that setting the `prefix` parameter doesn't work for pyiceberg.                                                                                              |
| `warehouse`                | *                             | n/a                          | Migrate object store configurations to the Nessie server configuration                                                                                                                                                                                                                                                                                     |
| `io`                       | *                             | n/a                          | Migrate object store configurations to the Nessie server configuration                                                                                                                                                                                                                                                                                     |
| (all S3/GCS/ADLS settings) |                               |                              | Remove all object store settings                                                                                                                                                                                                                                                                                                                           |

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

* the table name must be quoted using backticks (`` ` ``)
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
