# Authentication in Spark

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

The `spark.sql.catalog.nessie` prefix identifies properties for the Nessie catalog. The `nessie` part is just
the name of the catalog in this case (not to be confused with the Nessie project name).

Property names without the `spark.sql.catalog.nessie` prefix essentially match regular Nessie Client property names.

# Authentication Settings

The value of the `authentication.type` property (with the appropriate prefix) can be one of the following:

* NONE
* BEARER
* AWS

## Authentication Type "NONE"

For the Authentication Type "NONE" only the `authentication.type` property needs to be set
(with the appropriate prefix).

This is also the default authentication type if nothing else is configured.

## Authentication Type "BEARER"

For the "BEARER" Authentication Type the `authentication.token` property (with the appropriate prefix)
should be set to a valid [OpenID token](https://openid.net/specs/openid-connect-core-1_0.html).

## Authentication Type "AWS"

For the "AWS" Authentication Type the `authentication.aws.region` property (with the appropriate prefix)
should be set to the AWS region where the Nessie Server endpoint is located.

Additional AWS authentication configuration should be provided via standard AWS configuration files. 


