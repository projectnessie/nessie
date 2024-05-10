# Configure Nessie with Iceberg REST

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
nessie.catalog.service.s3.endpoint=http://localhost:9000
nessie.catalog.service.s3.path-style-access=true
nessie.catalog.service.s3.region=us-west-2
nessie.catalog.service.s3.access-key-id=awsAccessKeyId
nessie.catalog.service.s3.secret-access-key=awsSecretAccessKey

# S3 configuration settings that are different for "bucket1"
nessie.catalog.service.s3.buckets.bucket1.endpoint=s3a://bucket1
nessie.catalog.service.s3.buckets.bucket1.access-key-id=awsAccessKeyId1
nessie.catalog.service.s3.buckets.bucket1.secret-access-key=awsSecretAccessKey1
nessie.catalog.service.s3.buckets.bucket1.region=us-east-1
```

See [Server configuration Reference](../nessie-latest/index.md).

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

| Iceberg option             | Old value                     | New value                     | Description/notes                                                      |
|----------------------------|-------------------------------|-------------------------------|------------------------------------------------------------------------|
| `type`                     | `nessie`                      | `rest`                        | Change the catalog type from "nessie" to "rest".                       |
| `catalog-impl`             | `...NessieCatalog`            |                               | Use `type` = `rest`                                                    |
| `uri`                      | `http://.../api/v2` (or `v1`) | `http://.../iceberg`          | Replace `api/v1` or `api/v2` with `iceberg`                            |
| `ref`                      | Nessie branch name            | n/a                           | Migrate to `prefix` option                                             |
| `prefix`                   | n/a                           | Nessie branch name (optional) | Migrate from the `ref` option                                          |
| `warehouse`                | *                             | n/a                           | Migrate object store configurations to the Nessie server configuration |
| `io`                       | *                             | n/a                           | Migrate object store configurations to the Nessie server configuration |
| (all S3/GCS/ADLS settings) |                               |                               | Remove all object store settings                                       |

!!! warn
    Current Iceberg REST clients do not support the OAuth2 authorization code and device code flows!

    Only Bearer and client-ID/secret work. We recommend bearer tokens over client-ID/client-secret
    configuration to not put those credentials at the risk of being compromised - it is easier to
    revoke a single bearer token than to change a password used by many applications. Keep in mind
    that bearer token are only valid for some given period of time.

    We want to contribute the advanced OAuth2 functionality that already exists for Nessie client
    to Apache Iceberg. 

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
