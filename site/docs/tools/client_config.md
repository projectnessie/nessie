# Configuration and authentication in Tools

When Nessie is integrated into a broader data processing environment, authentication settings need to be provided in
a way specific to the tool used.

## Common Nessie client configuration options

| Configuration option               | Mandatory / default | Meaning                                                                                                                                                                   | 
|------------------------------------|---------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `nessie.uri`                       | Mandatory           | Nessie REST endpoint                                                                                                                                                      |
| `nessie.authentication.*`          | Recommended         | Authentication options, see [below](#authentication-settings)                                                                                                             |
| `nessie.ref`                       | Mandatory           | Name of the Nessie reference, usually `main`.                                                                                                                             |
| `nessie.ref.hash`                  | Optional            | Hash on `nessie.ref`, usually not specified.                                                                                                                              |
| `nessie.tracing`                   | Optional            | Boolean property to optionally enable tracing.                                                                                                                            |
| `nessie.transport.read-timeout`    | Optional            | Network level read timeout in milliseconds. When running with Java 11, this becomes a request timeout.                                                                    |
| `nessie.transport.connect-timeout` | Optional            | Network level connect timeout in milliseconds.                                                                                                                            |
| `nessie.http-redirects`            | Optional            | Optional, specify how redirects are handled. `NEVER`: Never redirect (default),`ALWAYS`: Always redirect, `NORMAL`: Always redirect, except from HTTPS URLs to HTTP URLs. |
| `nessie.ssl.cipher-suites`         | Optional            | Optional, specify the set of allowed SSL cipher suites.                                                                                                                   |
| `nessie.ssl.protocols`             | Optional            | Optional, specify the set of allowed SSL protocols.                                                                                                                       |
| `nessie.ssl.sni-hosts`             | Optional            | Optional, specify the set of allowed SNI hosts.                                                                                                                           |
| `nessie.ssl.sni-matcher`           | Optional            | Optional, specify a SNI matcher regular expression.                                                                                                                       |

### Java 11 connection pool options

The Java 11 HTTP client can be configured using Java system properties. Since Java's `HttpClient`
API does not support the configuration of these properties programmatically, Nessie cannot expose
those via its configuration mechanism.

| System property                     | Meaning                                                                                                          |
|-------------------------------------|------------------------------------------------------------------------------------------------------------------|
| `jdk.httpclient.connectionPoolSize` | The size of the HTTP connection pool.Defaults to `0`, which means the number of connections is unlimited.        |
| `jdk.httpclient.keepalive.timeout`  | Number of seconds an idle HTTP connection will be kept alive. Defaults is `1200` seconds.                        |
| `jdk.httpclient.receiveBufferSize`  | Size of the network level receive buffer size. Defaults to `0`, which means the operating system defaults apply. |
| `jdk.httpclient.sendBufferSize`     | Size of the network level send buffer size. Defaults to `0`, which means the operating system defaults apply.    |

!!! note
    See Javadoc of `javax.net.ssl.SSLParameters` for valid options/values for the configuration
    parameters starting with `nessie.ssl.`.

!!! note
    See Javadoc of `org.projectnessie.client.NessieConfigConstants` as well.

!!! note
    In case you run into issues with Nessie's new HTTP client for Java 11 and newer, you can try
    to use the legacy `URLConnection` based HTTP client by setting the system property
    `nessie.client.force-url-connection-client` to `true`.

## Spark

When Nessie is used in Spark-based environments (either with [Iceberg](./iceberg/index.md) 
or [Delta Lake](./deltalake/index.md)) the Nessie authentication settings are configured via Spark session properties (Replace `<catalog_name>` with the name of your catalog).

=== "Java"
    ``` java
    // local spark instance, assuming NONE authentication
    conf.set("spark.sql.catalog.<catalog_name>", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.<catalog_name>.authentication.type", "NONE")
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
            .config("spark.sql.catalog.<catalog_name>", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.<catalog_name>.authentication.type", "NONE") \
            .config(...) 
            .getOrCreate()
    ```

## Flink

When Nessie is used in Flink with [Iceberg](./iceberg/index.md), the Nessie authentication settings are configured when creating the Nessie catalog in Flink (Replace `<catalog_name>` with the name of your catalog):

```python
table_env.execute_sql(
        """CREATE CATALOG <catalog_name> WITH (
        'type'='iceberg',
        'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',
        'authentication.type'='NONE')""")
```

## Hive

When Nessie is used in Hive with [Iceberg](./iceberg/index.md), the Nessie authentication settings are configured through Hive Shell (Replace `<catalog_name>` with the name of your catalog):

```
SET iceberg.catalog.<catalog_name>.catalog-impl=org.apache.iceberg.nessie.NessieCatalog
SET iceberg.catalog.<catalog_name>.authentication.type=NONE
```

### Property Prefixes

The `spark.sql.catalog.<catalog_name>` prefix identifies properties for the Nessie catalog. The `<catalog_name>` part is just
the name of the catalog in this case (not to be confused with the Nessie project name).

Multiple Nessie catalogs can be configured in the same Spark environment, each with its own
set of configuration properties and its own property name prefix.

# Authentication Settings

The sections below discuss specific authentication settings. The property names are shown without
environment-specific prefixes for brevity. Nonetheless, in practice the property names should be
given appropriate prefixes (as in the example above) for them to be recognized by the tools and Nessie
code.

The value of the `authentication.type` property can be one of the following:

* `NONE` (default)
* `BEARER`
* `AWS`
* `BASIC` (deprecated)

## Authentication Type `NONE`

For the Authentication Type `NONE` only the `authentication.type` property needs to be set.

This is also the default authentication type if nothing else is configured.

## Authentication Type `BEARER`

For the `BEARER` Authentication Type the `authentication.token` property should be set to a valid
[OpenID token](https://openid.net/specs/openid-connect-core-1_0.html).

## Authentication Type `AWS`

For the `AWS` Authentication Type the `authentication.aws.region` property should be set to the
AWS region where the Nessie Server endpoint is located.

Additional AWS authentication configuration should be provided via standard AWS configuration files. 

## Authentication Type `BASIC`

For the `BASIC` Authentication Type the `authentication.username` and `authentication.password` properties
should be set.

Note: the `BASIC` authentication type is considered insecure and Nessie Servers do not support it in production
mode. This authentication type is can only be used when the Nessie Server runs in test or "development" mode.
