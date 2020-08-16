# Nessie configuration

Nessie is configurable via a `config.yaml` file found on the classpath. The default file looks like:

```yaml
defaultTag: master
databaseConfiguration:
  dbClassName: com.dremio.nessie.backend.dynamodb.DynamoDbBackend
  dbProps:
    endpoint: http://localhost:8000
    region: us-west-2
authenticationConfiguration:
  userServiceClassName: com.dremio.nessie.server.auth.BasicUserService
  authFilterClassName: com.dremio.nessie.server.auth.NessieAuthFilter
  enableLoginEndpoint: true
  enableUsersEndpoint: true
serviceConfiguration:
  port: 19120
  ui: true
  swagger: true
  metrics: true
```

Definitions are as follows:

* `defaultTag`: the default branch for Nessie (default: master)
* `dbClassName`: backend for Nessie. Class that implements the `Backend` interface.
* `dbProps`: map of parameters specific to the backend class. For DynamoDB this is only the region and endpoint to use
* `userServiceClassName`: Class implementing `UserService` which creates and manages `User` objects
* `authFilterClassName`: Class implementing container request filter. Responsible for verifing a jwt and allowing a user
  access to the rest endpoints.
* `enableLoginEndpoint`: boolean flag to control whether `api/v1/login` endpoint is published. This is only required when
  Nessie is authenticating users and generating `jwt`s
* `enableUsesEndpoint`: boolean flag to control whether `api/v1/users` endpoint is published. Likewise this is only
  required when Nessie is managing Users. 
* `port`: port to run on (not valid for Lambda functions)
* `ui`: boolean flag to control whether the UI is published. Located at `http://nessie-host:port/`
* `swagger`: boolean flag to control if the swagger UI and openapi definition is published.
  - Swagger UI is found at: `http://nessie-host:port/swagger-ui`
  - OpenAPI definition can be downloaded at `http://nessie-host:port/api/v1/openapi.yaml`
* `metrics`: whether to publish Prometheus metrics at `http://nessie-host:port/metrics`

It is expected that in custom deployments a custom UserService and possibly a AuthFilter will be used. These will detail
how a user is build and authenticated. Currently we use a basic auth schema and jwts however this could be extended to
use OAuth2.0, Okta etc.

Metrics are published using prometheus and can be collected via standard methods. See:
[Prometheus](https://prometheus.io).

The Swagger UI allows for testing the REST API and reading the API docs.
