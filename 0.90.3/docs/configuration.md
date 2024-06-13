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

## Providing secrets

Instead of providing secrets like passwords in clear text, you can also use a keystore. This
functionality is provided [natively via Quarkus](https://quarkus.io/guides/config-secrets#store-secrets-in-a-keystore).

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

### Catalog and Iceberg REST Settings

{% include './generated-docs/smallrye-nessie_catalog.md' %}

#### S3 settings

{% include './generated-docs/smallrye-nessie_catalog_service_s3.md' %}

#### Google Cloud Storage settings

!!! note
    Support for GCS is experimental.

{% include './generated-docs/smallrye-nessie_catalog_service_gcs.md' %}

#### ADLS settings

!!! note
    Support for ADLS is experimental.

{% include './generated-docs/smallrye-nessie_catalog_service_adls.md' %}

#### Advanced catalog settings

{% include './generated-docs/smallrye-nessie_catalog_service.md' %}

### Version Store Settings

{% include './generated-docs/smallrye-nessie_version_store.md' %}

### Support for the database specific implementations

| Database         | Status                                           | Configuration value for `nessie.version.store.type`     | Notes                                                                                                                                                                                                                           |
|------------------|--------------------------------------------------|---------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| "in memory"      | only for development and local testing           | `IN_MEMORY`                                             | Do not use for any serious use case.                                                                                                                                                                                            |
| RocksDB          | production, single node only                     | `ROCKSDB`                                               |                                                                                                                                                                                                                                 |
| Google BigTable  | production                                       | `BIGTABLE`                                              |                                                                                                                                                                                                                                 |
| MongoDB          | production                                       | `MONGODB`                                               |                                                                                                                                                                                                                                 |
| Amazon DynamoDB  | beta, only tested against the simulator          | `DYNAMODB`                                              |                                                                                                                                                                                                                                 |
| PostgreSQL       | production                                       | `JDBC`                                                  |                                                                                                                                                                                                                                 |
| H2               | only for development and local testing           | `JDBC`                                                  | Do not use for any serious use case.                                                                                                                                                                                            |
| MariaDB          | experimental, feedback welcome                   | `JDBC`                                                  |                                                                                                                                                                                                                                 |
| MySQL            | experimental, feedback welcome                   | `JDBC`                                                  | Works by connecting the MariaDB driver to a MySQL server.                                                                                                                                                                       |
| CockroachDB      | experimental, known issues                       | `JDBC`                                                  | Known to raise user-facing "write too old" errors under contention.                                                                                                                                                             |
| Apache Cassandra | experimental, known issues                       | `CASSANDRA`                                             | Known to raise user-facing errors due to Cassandra's concept of letting the driver timeout too early, or database timeouts.                                                                                                     |
| ScyllaDB         | experimental, known issues                       | `CASSANDRA`                                             | Known to raise user-facing errors due to Cassandra's concept of letting the driver timeout too early, or database timeouts. Known to be slow in container based testing. Unclear how good Scylla's LWT implementation performs. |

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

When setting `nessie.version.store.type=MONGODB` which enables MongoDB as the version store used by the Nessie server, the following configurations are applicable in combination with `nessie.version.store.type`.

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
Metrics are published using prometheus and can be collected via standard methods. See:
[Prometheus](https://prometheus.io).

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

### Swagger UI
The Swagger UI allows for testing the REST API and reading the API docs. It is available 
via [localhost:9000/q/swagger-ui](http://localhost:9000/q/swagger-ui/)

# Docker image options

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

## Nessie Docker image types

Nessie publishes a Java based multiplatform (for amd64, arm64, ppc64le, s390x) image running on OpenJDK 17.

## Advanced Docker image tuning (Java images only)

There are many environment variables available to configure the Docker image. If in doubt, leave
everything at its default. You can configure the behavior using the following environment
variables. They come from the base image used by Nessie,
[ubi9/openjdk-21-runtime](https://catalog.redhat.com/software/containers/ubi9/openjdk-21-runtime/6501ce769a0d86945c422d5f?architecture=amd64&image=66315a8390b0479a656a2f97).
The extensive list of supported environment variables can be found 
[here](https://access.redhat.com/documentation/en-us/red_hat_jboss_middleware_for_openshift/3/html/red_hat_java_s2i_for_openshift/reference#configuration_environment_variables).

### Examples

| Example                                    | `docker run` option                                                                                                |
|--------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| Using another GC                           | `-e GC_CONTAINER_OPTIONS="-XX:+UseShenandoahGC"` lets Nessie use Shenandoah GC instead of the default parallel GC. |
| Set the Java heap size to a _fixed_ amount | `-e JAVA_OPTS_APPEND="-Xms8g -Xmx8g"` lets Nessie use a Java heap of 8g.                                           | 

### Reference

| Environment variable             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|----------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `JAVA_OPTS` or `JAVA_OPTIONS`    | **NOT RECOMMENDED**. JVM options passed to the `java` command (example: "-verbose:class"). Setting this variable will override all options set by any of the other variables in this table. To pass extra settings, use `JAVA_OPTS_APPEND` instead.                                                                                                                                                                                                                                                                                                              |
| `JAVA_OPTS_APPEND`               | User specified Java options to be appended to generated options in `JAVA_OPTS` (example: "-Dsome.property=foo").                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `JAVA_TOOL_OPTIONS`              | This variable is defined and honored by all OpenJDK distros, see [here](https://bugs.openjdk.org/browse/JDK-4971166). Options defined here take precedence over all else; using this variable is generally not necessary, but can be useful e.g. to enforce JVM startup parameters, to set up remote debug, or to define JVM agents.                                                                                                                                                                                                                             |
| `JAVA_MAX_MEM_RATIO`             | Is used when no `-Xmx` option is given in JAVA_OPTS. This is used to calculate a default maximal heap memory based on a containers restriction. If used in a container without any memory constraints for the container then this option has no effect. If there is a memory constraint then `-Xmx` is set to a ratio of the container available memory as set here. The default is `50` which means 50% of the available memory is used as an upper boundary. You can skip this mechanism by setting this value to `0` in which case no `-Xmx` option is added. |
| `JAVA_INITIAL_MEM_RATIO`         | Is used when no `-Xms` option is given in JAVA_OPTS. This is used to calculate a default initial heap memory based on the maximum heap memory. If used in a container without any memory constraints for the container then this option has no effect. If there is a memory constraint then `-Xms` is set to a ratio of the `-Xmx` memory as set here. The default is `25` which means 25% of the `-Xmx` is used as the initial heap size. You can skip this mechanism by setting this value to `0` in which case no `-Xms` option is added (example: "25")      |
| `JAVA_MAX_INITIAL_MEM`           | Is used when no `-Xms` option is given in JAVA_OPTS. This is used to calculate the maximum value of the initial heap memory. If used in a container without any memory constraints for the container then this option has no effect. If there is a memory constraint then `-Xms` is limited to the value set here. The default is 4096MB which means the calculated value of `-Xms` never will be greater than 4096MB. The value of this variable is expressed in MB (example: "4096")                                                                           |
| `JAVA_DIAGNOSTICS`               | Set this to get some diagnostics information to standard output when things are happening. This option, if set to true, will set `-XX:+UnlockDiagnosticVMOptions`. Disabled by default (example: "true").                                                                                                                                                                                                                                                                                                                                                        |
| `JAVA_DEBUG`                     | If set remote debugging will be switched on. Disabled by default (example: true").                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `JAVA_DEBUG_PORT`                | Port used for remote debugging. Defaults to 5005 (example: "8787").                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `CONTAINER_CORE_LIMIT`           | A calculated core limit as described in https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt. (example: "2")                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| `CONTAINER_MAX_MEMORY`           | Memory limit given to the container (example: "1024").                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `GC_MIN_HEAP_FREE_RATIO`         | Minimum percentage of heap free after GC to avoid expansion.(example: "20")                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `GC_MAX_HEAP_FREE_RATIO`         | Maximum percentage of heap free after GC to avoid shrinking.(example: "40")                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `GC_TIME_RATIO`                  | Specifies the ratio of the time spent outside the garbage collection.(example: "4")                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `GC_ADAPTIVE_SIZE_POLICY_WEIGHT` | The weighting given to the current GC time versus previous GC times. (example: "90")                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `GC_METASPACE_SIZE`              | The initial metaspace size. (example: "20")                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `GC_MAX_METASPACE_SIZE`          | The maximum metaspace size. (example: "100")                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `GC_CONTAINER_OPTIONS`           | Specify Java GC to use. The value of this variable should contain the necessary JRE command-line options to specify the required GC, which will override the default of `-XX:+UseParallelGC` (example: -XX:+UseG1GC).                                                                                                                                                                                                                                                                                                                                            |
| `HTTPS_PROXY`                    | The location of the https proxy. (example: "myuser@127.0.0.1:8080")                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `HTTP_PROXY`                     | The location of the http proxy. (example: "myuser@127.0.0.1:8080")                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `NO_PROXY`                       | A comma separated lists of hosts, IP addresses or domains that can be accessed directly. (example: "foo.example.com,bar.example.com")                                                                                                                                                                                                                                                                                                                                                                                                                            |
