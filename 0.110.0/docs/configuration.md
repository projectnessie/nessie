---
title: "Server Configuration"
---

# Server Configuration

The Nessie server is configurable via properties as listed in
the [application.properties](https://github.com/projectnessie/nessie/blob/main/servers/quarkus-server/src/main/resources/application.properties)
file. 

These properties can be set when starting up the docker image in two different ways. For example, if 
you want to set Nessie to use the `JDBC` version store and provide a JDBC connection URL, you can 
either:

1. Set these values via the `JAVA_OPTS_APPEND` option in the Docker invocation. Each setting 
should be inserted inside the variable's value as `-D<name>=<value>` pairs: 

     ```bash
     docker run  -p 19120:19120 \
       -e JAVA_OPTS_APPEND="-Dnessie.version.store.type=JDBC -Dquarkus.datasource.jdbc.url=jdbc:postgresql://host.com:5432/db" \
       ghcr.io/projectnessie/nessie
     ```

2. Alternatively, set them via the `--env` (or `-e`) option in the Docker invocation. Each setting 
must be provided separately as `--env NAME=value` options:

    ```bash
    docker run -p 19120:19120 \
      --env NESSIE_VERSION_STORE_TYPE=JDBC \
      --env QUARKUS_DATASOURCE_JDBC_URL="jdbc:postgresql://host.com:5432/db" \
      ghcr.io/projectnessie/nessie
    ```

Note how the original property name is converted to an environment variable, e.g. 
`nessie.version.store.type` becomes `NESSIE_VERSION_STORE_TYPE`. The conversion is done by replacing 
all `.` with `_` and converting the name to upper case. 
See [here](https://smallrye.io/smallrye-config/Main/config/environment-variables/) for more details.

For more information on docker images, see [Docker image options](#docker-image-options) below.

## Server sizing

The minimum resources for Nessie are 4 CPUs and 4 GB RAM.

The recommended resources for Nessie depend on the actual use case and usage pattern(s). We recommend to try various
configurations, starting with 8 CPUs and 8 GB RAM.

The efficiency of Nessie's cache can be monitored using the metrics provided with the `cache=nessie-objects` tag,
especially the `cache.gets` values for `hit`/`miss` and the `cause`s provided by `cache.evictions`.

!!! note
    Nessie is a stateless service that heavily depends on the performance of the backend database (request duration and
    throughput) and works best with distributed key-value databases. Nessie has a built-in cache. Caches require
    memory, the more memory, the more efficient is the cache and the fewer operations need to be performed against the
    backend database.

!!! tip
    You can set the the `nessie.version.store.persist.reference-cache-ttl` configuration option to further
    reduce the load against the backing database. See [Version Store Advanced Settings](#version-store-advanced-settings)
    below.

!!! note
    Many things happen in parallel and some libraries that we have to depend on are not written in a "reactive way",
    especially with Iceberg REST. While the Iceberg REST parts in Nessie are built in a "reactive way",
    most Nessie core APIs are not.

## Supported operating systems

| Operating System | Production         | Development & prototyping | Comments                                                                                 |
|------------------|--------------------|---------------------------|------------------------------------------------------------------------------------------|
| Linux            | :heavy_check_mark: | :heavy_check_mark:        | Primarily supported operating systems, assuming recent kernel and distribution versions. |
| macOS            | :x:                | :heavy_check_mark:        | Supported for development and testing purposes.                                          |
| AIX              | :x:                | :x:                       | Not tested, might work or not.                                                           |
| Solaris          | :x:                | :x:                       | Not tested, might work or not.                                                           |
| Windows          | :x:                | :x:                       | Not supported in any way. Nessie server and admin tool refuse to start.                  |

## Providing secrets

Instead of providing secrets like passwords in clear text, you can also use a keystore. This
functionality is provided [natively via Quarkus](https://quarkus.io/guides/config-secrets#store-secrets-in-a-keystore).

See also the [secrets manager settings](#secrets-manager-settings) below for information about
Hashicorp Vault, Google Cloud and Amazon Services Secrets Managers.

## Core Nessie Configuration Settings

### Core Settings

{% include './generated-docs/smallrye-nessie_server.md' %}

Related Quarkus settings:

| Property                  | Default values | Type      | Description                                                            |
|---------------------------|----------------|-----------|------------------------------------------------------------------------|
| `quarkus.http.port`       | `19120`        | `int`     | Sets the HTTP port for the Nessie REST API endpoints.                  |
| `quarkus.management.port` | `9000`         | `int`     | Sets the HTTP port for management endpoints (health, metrics, Swagger) |

!!! info
    A complete set of configuration options for Quarkus can be found on [quarkus.io](https://quarkus.io/guides/all-config)

!!! info
    **Reverse Proxy Settings**

    These config options are mentioned only for documentation purposes. Consult the
    [Quarkus documentation](https://quarkus.io/guides/http-reference#reverse-proxy)
    for "Running behind a reverse proxy" and configure those depending on your actual needs.

    Do NOT enable these option unless your reverse proxy (for example istio or nginx)
    is properly setup to set these headers but also filter those from incoming requests.

### Catalog and Iceberg REST Settings

{% include './generated-docs/smallrye-nessie_catalog.md' %}

#### Warehouse defaults

{% include './generated-docs/smallrye-nessie_catalog_warehouseDefaults.md' %}

#### Warehouses

{% include './generated-docs/smallrye-nessie_catalog_warehouses.md' %}

#### S3 settings

{% include './generated-docs/smallrye-nessie_catalog_s3_config.md' %}

##### S3 default bucket settings

{% include './generated-docs/smallrye-nessie_catalog_s3_default_options.md' %}

##### S3 per bucket settings

{% include './generated-docs/smallrye-nessie_catalog_s3_buckets.md' %}

##### S3 transport

{% include './generated-docs/smallrye-nessie_catalog_s3_config_transport.md' %}

##### S3 STS, assume-role global settings

{% include './generated-docs/smallrye-nessie_catalog_s3_config_sts.md' %}

#### Google Cloud Storage settings

!!! note
    Support for GCS is experimental.

##### GCS buckets

{% include './generated-docs/smallrye-nessie_catalog_gcs.md' %}

##### GCS default bucket settings

{% include './generated-docs/smallrye-nessie_catalog_gcs_default_options.md' %}

##### GCS per bucket settings

{% include './generated-docs/smallrye-nessie_catalog_gcs_buckets.md' %}

##### GCS transport

{% include './generated-docs/smallrye-nessie_catalog_gcs_config_transport.md' %}

#### ADLS settings

!!! note
    Support for ADLS is experimental.

{% include './generated-docs/smallrye-nessie_catalog_adls.md' %}

##### ADLS default file-system settings

{% include './generated-docs/smallrye-nessie_catalog_adls_default_options.md' %}

##### ADLS per file-system  settings

{% include './generated-docs/smallrye-nessie_catalog_adls_buckets.md' %}

##### ADLS transport

{% include './generated-docs/smallrye-nessie_catalog_adls_config_transport.md' %}

#### Advanced catalog settings

##### Error Handling

{% include './generated-docs/smallrye-nessie_catalog_service_config_error_handling.md' %}

##### Performance Tuning

{% include './generated-docs/smallrye-nessie_catalog_service.md' %}

#### Secrets manager settings

Secrets for object stores are strictly separated from the actual configuration entries. This enables
the use of external secrets managers. Secrets are referenced using a URN notation.

The URN notation for Nessie secrets is `urn:nessie-secret:<provider>:<secret-name>`. `<provider>`
references the name of the provider, for example `quarkus` to resolve secrets via the
[Quarkus configuration](#quarkus-configuration-incl-environment-variables). `<secret-name>` is the
secrets manager specific name for the secret to resolve.

Retrieving secrets from external secrets managers like Hashicorp Vault and the Amazon, Google and Azure
secrets managers can take some time. Nessie mitigates this cost by caching retrieved secrets for some time,
by default 15 minutes (see config reference below). The default allows you to regularly rotate the object
store secrets by updating those in the external secrets manager, Nessie will pick those up within the
configured cache TTL. If you do not intent to rotate your secrets, you can bump the TTL to a very high value
to prevent cached secrets from being expired and hence perform unneeded requests to secrets managers.

{% include './generated-docs/smallrye-nessie_secrets.md' %}

##### Types of Secrets

* **Basic credentials** are composites of a `name` attribute and a `secret` attribute.
  AWS credentials are managed as basic credentials, where the `name` represents the access key ID and
  the `secret` represents the secret access key.
* **Tokens** are composites of a `token` attribute and an optional `expiresAt` attribute, latter
  represented as an instant.
* **Keys** consist of a single `key` attribute.

##### Quarkus configuration (incl environment variables)

Object store secrets managed via Quarkus' configuration mechanism (SmallRye Config) resolve components of
the secret types (basic credentials, tokens, keys) via individual configuration keys.

The Quarkus configuration key prefix (or environment variable name) is specified for the secret using the
URN notation `urn:nessie-secret:quarkus:<quarkus-configuration-key-prefix>.<secret-part>`.

The following example illustrates the Quarkus configuration entries to define the default S3 access-key and
secret-access-key:
```properties
# 
#                   Prefix of the Quarkus configuration keys for this secret ---+
#                                                                               |
#                     The URN for Quarkus secrets ---+                          |
#                                                    |                          |
#                                                    |------------------------- |--------------------
nessie.catalog.service.s3.default-options.access-key=urn:nessie-secret:quarkus:my-secrets.s3-default
# The AWS access-key and secret-access-key are referenced via the "secret-part" name,
# see 'Types of Secrets' above.
# `my-secrets.s3-default` is the `secret-part` as in the last part of the above property
my-secrets.s3-default.name=awsAccessKeyId
my-secrets.s3-default.secret=awsSecretAccessKey
```

##### Storing Secrets in Hashicorp Vault

Secrets in Hashicorp Vault are referenced using the URN prefix `urn:nessie-secret:vault:` followed by the
name/path of the secrets in Hashicorp Vault.

When using Hashicorp Vault make sure to configure the connection settings described in the
[Quarkus docs](https://docs.quarkiverse.io/quarkus-vault/dev/index.html#configuration-reference).

In Hashicorp Vault, secrets are stored as a map of strings to strings, where the map keys
are defined by the type of the secret as mentioned above.

For example, using the `vault` tool, a _basic credential_ is stored like this:
```shell
vault kv put secret/nessie-secrets/... name=the_username secret=the_secret_password
```
and similarly for AWS S3 access keys
```shell
vault kv put secret/nessie-secrets/... name=access_key secret=secret_access_key
```

A _token_ is stored like this:
```shell
vault kv put secret/nessie-secrets/... token=value_of_the_secret_token
```
and if it is an _expiring_ token with the expiration timestamp as an ISO instant.
```shell
vault kv put secret/nessie-secrets/... token=value_of_the_token expiresAt=2024-12-24T18:00:00Z
```

A _key_ is stored like this:
```shell
vault kv put secret/nessie-secrets/... key=value_of_the_secret_key
```

The paths mentioned above (`secret/nessie-secrets/...`) contain the path within Hashicorp Vault.
Those need to be specified in the Nessie secrets URN notation starting with `urn:nessie-secret:vault:`.


##### Storing Secrets in Google Cloud and Amazon Services Secrets Managers and Azure Key Vault

!!! warn
    Google Secrets Manager and Azure Key Vault are both not yet supported and considered experimental!
    The reason is that there is no good way to test those locally and in CI.

Secrets Store specifics:

| Secrets Manager                 | Nessie URN prefix           | Configuration Details                                                                                                                            |
|---------------------------------|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| Google Cloud Secrets Manager    | `urn:nessie-secret:google:` | [Quarkus Reference Docs](https://docs.quarkiverse.io/quarkus-google-cloud-services/main/index.html#_configuration_reference)                     |
| Amazon Services Secrets Manager | `urn:nessie-secret:amazon:` | [Quarkus Reference Docs](https://docs.quarkiverse.io/quarkus-amazon-services/dev/amazon-secretsmanager.html#_configuration_reference)            | 
| Azure Key Vault                 | `urn:nessie-secret:azure:`  | [Quarkus Reference Docs](https://docs.quarkiverse.io/quarkus-azure-services/dev/quarkus-azure-key-vault.html#_extension_configuration_reference) |

In Google Cloud and Amazon Services Secrets Managers and Azure Key Vault all secrets are stored as a
single string.

Since credentials consist of multiple values, Nessie expects the stored secret to be a JSON encoded
object.

Secrets are generally stored as JSON objects representing a map of strings to strings, where the map keys
are defined by the type of the secret as mentioned above.

For example, a _basic credential_ has to be stored as JSON like this, _without any leading or trailing
whitespaces or newlines_:

```json
{"name": "mysecret", "secret": "mypassword"}
```

A _token_ with an expiration date has to be stored as JSON like this, _without any leading or trailing
whitespaces or newlines_, where the `expiresAt` attribute is only needed for tokens that expire:

```json
{"token": "rkljmnfgoi4jfgoiujh23o4irj", "expiresAt": "2024-06-05T20:38:16Z"}
```

A _key_ however is always stored as is and _not_ encoded in JSON.

### Version Store Settings

{% include './generated-docs/smallrye-nessie_version_store.md' %}

### Support for the database specific implementations

| Database         | Status                                           | Configuration value for `nessie.version.store.type` | Notes                                                                                                                                                                                                                                                                                                      |
|------------------|--------------------------------------------------|-----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| "in memory"      | only for development and local testing           | `IN_MEMORY`                                         | Do not use for any serious use case.                                                                                                                                                                                                                                                                       |
| RocksDB          | production, single node only                     | `ROCKSDB`                                           |                                                                                                                                                                                                                                                                                                            |
| Google BigTable  | production                                       | `BIGTABLE`                                          |                                                                                                                                                                                                                                                                                                            |
| MongoDB          | production                                       | `MONGODB2` & `MONGODB` (deprecated)                 |                                                                                                                                                                                                                                                                                                            |
| Amazon DynamoDB  | beta, only tested against the simulator          | `DYNAMODB`                                          | Not recommended for use with Nessie Catalog (Iceberg REST) due to its restrictive row-size limit.                                                                                                                                                                                                          |
| PostgreSQL       | production                                       | `JDBC2` & `JDBC` (deprecated)                       |                                                                                                                                                                                                                                                                                                            |
| H2               | only for development and local testing           | `JDBC2` & `JDBC` (deprecated)                       | Do not use for any serious use case.                                                                                                                                                                                                                                                                       |
| MariaDB          | experimental, feedback welcome                   | `JDBC2` & `JDBC` (deprecated)                       |                                                                                                                                                                                                                                                                                                            |
| MySQL            | experimental, feedback welcome                   | `JDBC2` & `JDBC` (deprecated)                       | Works by connecting the MariaDB driver to a MySQL server.                                                                                                                                                                                                                                                  |
| CockroachDB      | experimental, known issues                       | `JDBC2` & `JDBC` (deprecated)                       | Known to raise user-facing "write too old" errors under contention.                                                                                                                                                                                                                                        |
| Apache Cassandra | experimental, known issues                       | `CASSANDRA2` & `CASSANDRA` (deprecated)             | Known to raise user-facing errors due to Cassandra's concept of letting the driver timeout too early, or database timeouts.                                                                                                                                                                                |

!!! warn

!!! warn
    Prefer the `CASSANDRA2` version store type over the `CASSANDRA` version store type, because it has way less storage overhead.
    The `CASSANDRA` version store type is _deprecated for removal_, please use the
    [Nessie Server Admin Tool](export_import.md) to migrate from the `CASSANDRA` version store type to `CASSANDRA2`.

!!! warn
    Prefer the `MONGODB2` version store type over the `MONGODB` version store type, because it has way less storage overhead.
    The `MONGODB` version store type is _deprecated for removal_, please use the
    [Nessie Server Admin Tool](export_import.md) to migrate from the `MONGODB` version store type to `MONGODB2`.

!!! warn
    Prefer the `JDBC2` version store type over the `JDBC` version store type, because it has way less storage overhead.
    The `JDBC` version store type is _deprecated for removal_, please use the
    [Nessie Server Admin Tool](export_import.md) to migrate from the `JDBC` version store type to `JDBC2`.

!!! note
    Relational databases are generally slower and tend to become a bottleneck when concurrent Nessie commits against
    the same branch happen. This is a general limitation of relational databases and the actual unpleasant performance
    penalty depends on the relational database itself, its configuration and whether and how replication is enabled.

#### BigTable Version Store Settings

{% include './generated-docs/smallrye-nessie_version_store_persist_bigtable.md' %}

Related Quarkus settings:

| Property                                                      | Default values | Type                  | Description                                                                                                              |
|---------------------------------------------------------------|----------------|-----------------------|--------------------------------------------------------------------------------------------------------------------------|
| `quarkus.google.cloud.project-id`                             |                | `String`              | The Google project ID, mandatory.                                                                                        |
| (Google authentication)                                       |                |                       | See [Quarkiverse](https://quarkiverse.github.io/quarkiverse-docs/quarkus-google-cloud-services/main/) for documentation. | 


!!! info
    A complete set of Google Cloud & BigTable configuration options for Quarkus can be found on [Quarkiverse](https://quarkiverse.github.io/quarkiverse-docs/quarkus-google-cloud-services/main/).

#### JDBC Version Store Settings

{% include './generated-docs/smallrye-nessie_version_store_persist_jdbc.md' %}

#### RocksDB Version Store Settings

{% include './generated-docs/smallrye-nessie_version_store_persist_rocks.md' %}

#### Cassandra Version Store Settings

{% include './generated-docs/smallrye-nessie_version_store_cassandra.md' %}

Related Quarkus settings:

| Property                                     | Default values | Type      | Description                                                                                                                          |
|----------------------------------------------|----------------|-----------|--------------------------------------------------------------------------------------------------------------------------------------|
| `quarkus.cassandra.keyspace`                 |                | `String`  | The Cassandra keyspace to use.                                                                                                       |
| `quarkus.cassandra.contact-points`           |                | `String`  | The Cassandra contact points, see [Quarkus docs](https://quarkus.io/guides/cassandra#connecting-to-the-cassandra-database).          | 
| `quarkus.cassandra.local-datacenter`         |                | `String`  | The Cassandra local datacenter to use, see [Quarkus docs](https://quarkus.io/guides/cassandra#connecting-to-the-cassandra-database). |
| `quarkus.cassandra.auth.username`            |                | `String`  | Cassandra authentication username, see [Quarkus docs](https://quarkus.io/guides/cassandra#connecting-to-the-cassandra-database).     |
| `quarkus.cassandra.auth.password`            |                | `String`  | Cassandra authentication password, see [Quarkus docs](https://quarkus.io/guides/cassandra#connecting-to-the-cassandra-database).     |
| `quarkus.cassandra.health.enabled`           | `false`        | `boolean` | See Quarkus docs.                                                                                                                    |

!!! info
    A complete set of the Quarkus Cassandra extension configuration options can be found on [quarkus.io](https://quarkus.io/guides/cassandra#connecting-to-the-cassandra-database)

#### DynamoDB Version Store Settings

{% include './generated-docs/smallrye-nessie_version_store_persist_dynamodb.md' %}

Related Quarkus settings:

| Property                                             | Default values | Type     | Description                                                                                                                                                                                                                                       |
|------------------------------------------------------|----------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `quarkus.dynamodb.aws.region`                        |                | `String` | Sets DynamoDB AWS region.                                                                                                                                                                                                                         |
| `quarkus.dynamodb.aws.credentials.type`              | `default`      | `String` | See [Quarkiverse](https://quarkiverse.github.io/quarkiverse-docs/quarkus-amazon-services/dev/amazon-dynamodb.html#_configuration_reference) docs for possible values. Sets the credentials provider that should be used to authenticate with AWS. |
| `quarkus.dynamodb.endpoint-override`                 |                | `URI`    | Sets the endpoint URI with which the SDK should communicate. If not specified, an appropriate endpoint to be used for the given service and region.                                                                                               |
| `quarkus.dynamodb.sync-client.type`                  | `url`          | `String` | Possible values are: `url`, `apache`. Sets the type of the sync HTTP client implementation                                                                                                                                                        |

!!! info
    A complete set of DynamoDB configuration options for Quarkus can be found on [Quarkiverse](https://quarkiverse.github.io/quarkiverse-docs/quarkus-amazon-services/dev/amazon-dynamodb.html#_configuration_reference).

#### MongoDB Version Store Settings

When setting `nessie.version.store.type=MONGODB2` which enables MongoDB as the version store used by the Nessie server, the following configurations are applicable in combination with `nessie.version.store.type`.

Related Quarkus settings:

| Property                            | Default values | Type     | Description                     |
|-------------------------------------|----------------|----------|---------------------------------|
| `quarkus.mongodb.database`          |                | `String` | Sets MongoDB database name.     |
| `quarkus.mongodb.connection-string` |                | `String` | Sets MongoDB connection string. |

!!! info
    A complete set of MongoDB configuration options for Quarkus can be found on [quarkus.io](https://quarkus.io/guides/all-config#quarkus-mongodb-client_quarkus-mongodb-client-mongodb-client).

#### In-Memory Version Store Settings

No special configuration options for this store type.

### Version Store Advanced Settings

The following configurations are advanced configurations for version stores to configure how Nessie will store the data
into the configured data store:

Usually, only the cache-capacity should be adjusted to the amount of the Java heap "available" for the cache. The
default is conservative, bumping the cache size is recommended.

{% include './generated-docs/smallrye-nessie_version_store_persist.md' %}

### Authentication settings

{% include './generated-docs/smallrye-nessie_server_authentication.md' %}

Related Quarkus settings:

| Property                               | Default values | Type      | Description                                                                                                                                                  |
|----------------------------------------|----------------|-----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `quarkus.oidc.auth-server-url`         |                | `String`  | Sets the base URL of the OpenID Connect (OIDC) server if `nessie.server.authentication.enabled=true`                                                         |
| `quarkus.oidc.client-id`               |                | `String`  | Sets client-id of the application if `nessie.server.authentication.enabled=true`. Each application has a client-id that is used to identify the application. |


### Authorization settings

{% include './generated-docs/smallrye-nessie_server_authorization.md' %}

### Metrics

Metrics are published using Micrometer; they are available from Nessie's management interface (port
9000 by default) under the path `/q/metrics`. For example, if the server is running on localhost,
the metrics can be accessed via http://localhost:9000/q/metrics.

Metrics can be scraped by Prometheus or any compatible metrics scraping server. See:
[Prometheus](https://prometheus.io) for more information.

Additional tags can be added to the metrics by setting the `nessie.metrics.tags.*` property. Each
tag is a key-value pair, where the key is the tag name and the value is the tag value. For example,
to add a tag `environment=prod` to all metrics, set `nessie.metrics.tags.environment=prod`. Many
tags can be added, such as below:

```properties
nessie.metrics.tags.service=nessie
nessie.metrics.tags.environment=prod
nessie.metrics.tags.region=us-west-2
```

Note that by default Nessie adds one tag: `application=Nessie`. You can override this tag by setting
the `nessie.metrics.tags.application=<new-value>` property.

A standard Grafana dashboard is available in the `grafana` directory of the Nessie repository [here]
(https://github.com/projectnessie/nessie/blob/main/grafana/nessie.json). You can use this dashboard
to visualize the metrics scraped by Prometheus. Note that this dashboard is a starting point and may
need to be customized to fit your specific needs. 

This Grafana dashboard expects the metrics to have a few tags defined: `service` and `instance`. The
`instance` tag is generally added by Prometheus automatically, but the `service` tag needs to be
added manually. You can configure Nessie to add this tag to all metrics by setting the below
property:

```properties
nessie.metrics.tags.service=<service-name>
```

Alternatively, you can modify the dashboard to remove unnecessary tags, or configure Prometheus to
add the missing ones. Here is an example configuration showing how to have the `service` tag added
by Prometheus:

```yaml
scrape_configs:
  - job_name: 'nessie'
    metrics_path: /q/metrics
    static_configs:
      - targets: ['nessie:9000']
        labels:
          service: nessie
```

### Traces

Since Nessie 0.46.0, traces are published using OpenTelemetry. See [Using
OpenTelemetry](https://quarkus.io/guides/opentelemetry) in the Quarkus documentation.

In order for the server to enable OpenTelemetry and publish its traces, the
`quarkus.otel.exporter.otlp.traces.endpoint` property _must_ be defined. Its value must be a
valid collector endpoint URL, with either `http://` or `https://` scheme. The collector must talk
the OpenTelemetry protocol (OTLP) and the port must be its gRPC port (by default 4317), e.g.
"http://otlp-collector:4317". _If this property is not set, the server will not publish traces._

Alternatively, it's possible to forcibly disable OpenTelemetry at runtime by setting the following 
property: `quarkus.otel.sdk.disabled=true`.

#### Troubleshooting traces

If the server is unable to publish traces, check first for a log warning message like the following:

```
SEVERE [io.ope.exp.int.grp.OkHttpGrpcExporter] (OkHttp http://localhost:4317/...) Failed to export spans. 
The request could not be executed. Full error message: Failed to connect to localhost/0:0:0:0:0:0:0:1:4317
```

This means that the server is unable to connect to the collector. Check that the collector is
running and that the URL is correct.

## Docker image options

By default, Nessie listens on port 19120. To expose that port on the host, use `-p 19120:19120`. 
To expose that port on a different port on the host system, use the `-p` option and map the
internal port to some port on the host. For example, to expose Nessie on port 8080 of the host 
system, use the following command:

```bash
docker run -p 8080:19120 ghcr.io/projectnessie/nessie
```

Then you can browse Nessie's UI on the host by pointing your browser to http://localhost:8080.

Note: this doesn't change the port Nessie listens on, it only changes the port on the host system
that is mapped to the port Nessie listens on. Nessie still listens on port 19120 inside the
container. If you want to change the port Nessie listens on, you can use the `QUARKUS_HTTP_PORT`
environment variable. For example, to make Nessie listen on port 8080 inside the container, 
and expose it to the host system also on 8080, use the following command:

```bash
docker run -p 8080:8080 -e QUARKUS_HTTP_PORT=8080 ghcr.io/projectnessie/nessie
```

### Nessie Docker image types

Nessie publishes a Java based multiplatform (for amd64, arm64, ppc64le, s390x) image running on OpenJDK 17.

### Advanced Docker image tuning (Java images only)

There are many environment variables available to configure the Docker image. If in doubt, leave
everything at its default. You can configure the behavior using the environment variables listed
below, which come from the base image used by Nessie, [ubi9/openjdk-21-runtime].

[ubi9/openjdk-21-runtime]: https://catalog.redhat.com/software/containers/ubi9/openjdk-21-runtime/6501ce769a0d86945c422d5f

#### Examples

| Example                                    | `docker run` option                                                                                                |
|--------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| Using another GC                           | `-e GC_CONTAINER_OPTIONS="-XX:+UseShenandoahGC"` lets Nessie use Shenandoah GC instead of the default parallel GC. |
| Set the Java heap size to a _fixed_ amount | `-e JAVA_OPTS_APPEND="-Xms8g -Xmx8g"` lets Nessie use a Java heap of 8g.                                           | 

#### Reference

| Environment variable             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `JAVA_OPTS` or `JAVA_OPTIONS`    | **NOT RECOMMENDED**. JVM options passed to the `java` command (example: "-verbose:class"). Setting this variable will override all options set by any of the other variables in this table. To pass extra settings, use `JAVA_OPTS_APPEND` instead.                                                                                                                                                                                                                                                                                    |
| `JAVA_OPTS_APPEND`               | User specified Java options to be appended to generated options in `JAVA_OPTS` (example: "-Dsome.property=foo").                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `JAVA_TOOL_OPTIONS`              | This variable is defined and honored by all OpenJDK distros, see [here](https://bugs.openjdk.org/browse/JDK-4971166). Options defined here take precedence over all else; using this variable is generally not necessary, but can be useful e.g. to enforce JVM startup parameters, to set up remote debug, or to define JVM agents.                                                                                                                                                                                                   |
| `JAVA_MAX_MEM_RATIO`             | Is used to calculate a default maximal heap memory based on a containers restriction. If used in a container without any memory constraints for the container then this option has no effect. If there is a memory constraint then `-XX:MaxRAMPercentage` is set to a ratio of the container available memory as set here. The default is `80` which means 80% of the available memory is used as an upper boundary. You can skip this mechanism by setting this value to `0` in which case no `-XX:MaxRAMPercentage` option is added. |
| `JAVA_DEBUG`                     | If set remote debugging will be switched on. Disabled by default (example: true").                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `JAVA_DEBUG_PORT`                | Port used for remote debugging. Defaults to "5005" (tip: use "*:5005" to enable debugging on all network interfaces).                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `GC_MIN_HEAP_FREE_RATIO`         | Minimum percentage of heap free after GC to avoid expansion. Default is 10.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `GC_MAX_HEAP_FREE_RATIO`         | Maximum percentage of heap free after GC to avoid shrinking. Default is 20.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `GC_TIME_RATIO`                  | Specifies the ratio of the time spent outside the garbage collection. Default is 4.                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `GC_ADAPTIVE_SIZE_POLICY_WEIGHT` | The weighting given to the current GC time versus previous GC times. Default is 90.                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `GC_METASPACE_SIZE`              | The initial metaspace size. There is no default (example: "20").                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `GC_MAX_METASPACE_SIZE`          | The maximum metaspace size. There is no default (example: "100").                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `GC_CONTAINER_OPTIONS`           | Specify Java GC to use. The value of this variable should contain the necessary JRE command-line options to specify the required GC, which will override the default of `-XX:+UseParallelGC` (example: `-XX:+UseG1GC`).                                                                                                                                                                                                                                                                                                                |


## Troubleshooting configuration issues

If you encounter issues with the configuration, you can ask Nessie to print out the configuration it
is using. To do this, set the log level for the `io.smallrye.config` category to `DEBUG`, and also
set the console appender level to `DEBUG`:

```properties
quarkus.log.console.level=DEBUG
quarkus.log.category."io.smallrye.config".level=DEBUG
```

!!! warn
    This will print out all configuration values, including sensitive ones like passwords. Don't
    do this in production, and don't share this output with anyone you don't trust!
