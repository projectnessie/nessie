# Authentication in Tools

When Nessie is integrated into a broader data processing environment, authentication settings need to be provided in
a way specific to the tool used.

## Spark

When Nessie is used in Spark-based environments (either with [Iceberg](./iceberg/index.md) 
or [Delta Lake](./deltalake/index.md)) the Nessie authentication settings are configured via Spark session properties.

=== "Java"
    ``` java
    // local spark instance, assuming NONE authentication
    conf.set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.nessie.authentication.type", "NONE")
        .set(...);
    spark = SparkSession.builder()
                        .master("local[2]")
                        .config(conf)
                        .getOrCreate();
    ```
=== "Python"
    ``` python
    # local spark instance, assuming NONE authentication
    spark = SparkSession.builder \
            .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.nessie.authentication.type", "NONE") \
            .config(...) 
            .getOrCreate()
    ```

### Property Prefixes

The `spark.sql.catalog.nessie` prefix identifies properties for the Nessie catalog. The `nessie` part is just
the name of the catalog in this case (not to be confused with the Nessie project name).

Multiple Nessie catalogs can be configured in the same Spark environment, each with its own
set of configuration properties and its own property name prefix.

# Authentication Settings

The value of the `authentication.type` property (with the appropriate prefix) can be one of the following:

* `NONE` (default)
* `BEARER`
* `AWS`

## Authentication Type `NONE`

For the Authentication Type `NONE` only the `authentication.type` property needs to be set
(with the appropriate prefix).

This is also the default authentication type if nothing else is configured.

## Authentication Type `BEARER`

For the `BEARER` Authentication Type the `authentication.token` property (with the appropriate prefix)
should be set to a valid [OpenID token](https://openid.net/specs/openid-connect-core-1_0.html).

## Authentication Type `AWS`

For the `AWS` Authentication Type the `authentication.aws.region` property (with the appropriate prefix)
should be set to the AWS region where the Nessie Server endpoint is located.

Additional AWS authentication configuration should be provided via standard AWS configuration files. 
