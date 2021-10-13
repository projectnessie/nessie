# Configuration

The Nessie server is configurable via properties as listed in the [application.properties](https://github.com/projectnessie/nessie/blob/main/servers/quarkus-server/src/main/resources/application.properties) file.
These properties can be set when starting up the docker image by adding them to the Docker invocation prefixed with `-D`.  For example, if you want to 
set Nessie to use the INMEMORY version store running on port 8080, you would run the 
following:

```bash
docker run -p 8080:8080 projectnessie/nessie \
  -Dnessie.version.store.type=INMEMORY \
  -Dquarkus.http.port=8080
```

## Core Nessie Configuration Settings

### Core Settings

| Property                                    | Default values  | Type     | Description                                                         |
| ------------------------------------------- | --------------- | -------- |-------------------------------------------------------------------- |
| `nessie.server.default-branch`              | `main`          | `String` | Sets the default branch to use if not provided by the user.         |
| `nessie.server.send-stacktrace-to-client`   | `false`         | `boolean`| Sets if server stack trace should be sent to the client in case of error.  |


### Version Store Settings

| Property                                    | Default values  | Type               | Description                                                         |
| ------------------------------------------- | --------------- | ------------------ |-------------------------------------------------------------------- |
| `nessie.version.store.type`                 | `INMEMORY`      | `VersionStoreType` | Sets which type of version store to use by Nessie. Possible values are: `DYNAMO`, `INMEMORY`, `ROCKS`, `MONGO`.  |
| `nessie.version.store.trace.enable`         | `true`          | `boolean`          | Sets whether calls against the version-store are traced with OpenTracing/OpenTelemetry (Jaeger).  |
| `nessie.version.store.metrics.enable`       | `true`          | `boolean`          | Sets whether metrics for the version-store are enabled.  |

#### RocksDB Version Store Settings

When setting `nessie.version.store.type=ROCKS` which enables RockDB as the version store used by the Nessie server, the following configurations are applicable in combination with `nessie.version.store.type`:

| Property                                    | Default values  | Type               | Description                                                         |
| ------------------------------------------- | --------------- | ------------------ |-------------------------------------------------------------------- |
| `nessie.version.store.rocks.db-path`        |                 | `String`           | Sets RocksDB storage path, e.g: `/tmp/rocks-nessie`. |


#### MongoDB Version Store Settings

When setting `nessie.version.store.type=MONGO` which enables MongoDB as the version store used by the Nessie server, the following configurations are applicable in combination with `nessie.version.store.type`:

| Property                                        | Default values      | Type         | Description                                                         |
| ----------------------------------------------- | ------------------- | ------------ |-------------------------------------------------------------------- |
| `quarkus.mongodb.database`                      |                     | `String`     | Sets MongoDB database name.                                         |
| `quarkus.mongodb.connection-string`             |                     | `String`     | Sets MongoDB connection string.                                     |

!!! info
    A complete set of MongoDB configuration options for Quarkus can be found on [quarkus.io](https://quarkus.io/guides/all-config#quarkus-mongodb-client_quarkus-mongodb-client-mongodb-client)


#### DynamoDB Version Store Settings

When setting `nessie.version.store.type=DYNAMO` which enables DynamoDB as the version store used by the Nessie server, the following configurations are applicable in combination with `nessie.version.store.type`:

| Property                                        | Default values      | Type         | Description                                                         |
| ----------------------------------------------- | ------------------- | ------------ |-------------------------------------------------------------------- |
| `quarkus.dynamodb.aws.region`                   |                     | `String`     | Sets DynamoDB AWS region. |
| `quarkus.dynamodb.aws.credentials.type`         |                     |              | Sets the credentials provider that should be used to authenticate with AWS. |
| `quarkus.dynamodb.endpoint-override`            |                     | `URI`        | Sets the endpoint URI with which the SDK should communicate. If not specified, an appropriate endpoint to be used for the given service and region. |
| `quarkus.dynamodb.sync-client.type`             | `url`               | `url, apache`| Sets the type of the sync HTTP client implementation |

!!! info
    A complete set of DynamoDB configuration options for Quarkus can be found on [quarkus.io](https://quarkus.io/guides/all-config#quarkus-amazon-dynamodb_quarkus-amazon-dynamodb-amazon-dynamodb)

### Database Adapter Settings

The following configurations are advanced configurations to configure how Nessie will store the data into the configured data store:

| Property                                        | Default values      | Type     | Description                                                         |
| ----------------------------------------------- | ------------------- | -------- |-------------------------------------------------------------------- |
| `nessie.adapter.key-prefix`                     |                     | `String` | Sets prefix for all primary-keys.  |
| `nessie.adapter.parent-per-commit`              | `20`                | `int`    | Sets the number of parent-commit-hashes stored in Nessie store.  |
| `nessie.adapter.key-list-distance`              | `20`                | `int`    | Sets the number "reachable" or "known" keys for each `CommitLogEntry`.   |
| `nessie.adapter.max-key-list-size`              | `250_000`           | `int`    | Sets the maximum size of a database object/row. This parameter is respected for `CommitLogEntry` and `KeyList`. This value must not be "on the edge" - means: it must leave enough room for a somewhat large-ish list   |
| `nessie.adapter.default-max-key-list-size`      | `250_000`           | `int`    | Sets the default number of a database object/row. Database adapter implementations that actually do have a hard technical or highly recommended limit on a maximum db-object / db-row size limitation should use a "good" value here.   |
| `nessie.adapter.commit-timeout`                 | `500`               | `int`    | Sets the timeout for CAS-like operations in milliseconds.   |
| `nessie.adapter.commit-retries`                 | `Integer.MAX_VALUE` | `int`    | Sets the maximum retries for CAS-like operations.   |
| `nessie.adapter.nontx.parents-per-global-commit`| `50`                | `int`    | Sets the number of parent-global-commit-hashes.   |
| `nessie.adapter.tx.batch-size`                  | `20`                | `int`    | Sets the DML batch size, used when writing multiple commits to a branch during a transplant or merge operation or when writing "overflow full key-lists".   |

### Authentication settings

| Property                                        | Default values      | Type     | Description                                                         |
| ----------------------------------------------- | ------------------- | -------- |-------------------------------------------------------------------- |
| `nessie.server.authentication.enabled`          | `false`             | `boolean`| Sets whether [authentication](./authentication.md) should be enabled on the Nessie server.  |
| `quarkus.oidc.auth-server-url`                  |                     | `String` | Sets the base URL of the OpenID Connect (OIDC) server if `nessie.server.authentication.enabled=true` |
| `quarkus.oidc.client-id`                        |                     | `String` | Sets client-id of the application if `nessie.server.authentication.enabled=true`. Each application has a client-id that is used to identify the application. |


### Authorization settings

| Property                                        | Default values      | Type     | Description                                                         |
| ----------------------------------------------- | ------------------- | -------- |-------------------------------------------------------------------- |
| `nessie.server.authorization.enabled`           | `false`             | `boolean`| Sets whether [authorization](../features/metadata_authorization.md) should be enabled on the Nessie server.  |
| `nessie.server.authorization.rules.<ruleId>`    |                     | `Map`     | Sets the [authorization](../features/metadata_authorization.md) rules that can be used in CEL format.  |


## Quarkus Server Settings Related to Nessie

| Property                                        | Default values      | Type         | Description                                                         |
| ----------------------------------------------- | ------------------- | ------------ |-------------------------------------------------------------------- |
| `quarkus.http.port`                             | `19120`             | `int`        | Sets the HTTP port |
| `quarkus.http.auth.basic`                       |                     | `boolean`    | Sets if basic auth should be enabled. |


!!! info
    A complete set of configuration options for Quarkus can be found on [quarkus.io](https://quarkus.io/guides/all-config)

### Metrics
Metrics are published using prometheus and can be collected via standard methods. See:
[Prometheus](https://prometheus.io).


### Swagger UI
The Swagger UI allows for testing the REST API and reading the API docs. It is available 
via [localhost:19120/q/swagger-ui](http://localhost:19120/q/swagger-ui/)
