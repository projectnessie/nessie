# Configuration

Nessie is configurable via setting available properties as listed in the [application.properties](https://github.com/projectnessie/nessie/blob/main/servers/quarkus-server/src/main/resources/application.properties) file. 
These configuration settings are able to be set when starting up the docker image.

## Core Nessie Configuration Settings

```properties
nessie.server.default-branch=main
nessie.server.should-sendstack-trace-to-api-client=true

# which type of version store to use: JGIT, INMEMORY, DYNAMO. JGIT is for local testing, DYNAMO preferred for production
nessie.version.store.type=DYNAMO

# JGit Options
# Which type of jgit version store to use: INMEMORY, DISK 
nessie.version.store.jgit.type=DISK
nessie.version.store.jgit.directory=/tmp/jgit

## Dyanmo version store specific configuration
# should Nessie create its own dynamo tables
nessie.version.store.dynamo.initialize=false
# table names for ref, tree and value tables respectively
nessie.version.store.dynamo.refTableName=nessie_refs
nessie.version.store.dynamo.treeTableName=nessie_trees
nessie.version.store.dynamo.valueTableName=nessie_values

## Dynamo Configuration
quarkus.dynamodb.aws.region=us-west-2
quarkus.dynamodb.aws.credentials.type=DEFAULT
```

## Generalized Server Settings

```properties
# Quarkus settings
## Visit here for all configs: https://quarkus.io/guides/all-config
## some parameters are only configured at build time. These have been marked as such https://quarkus.io/guides/config#overriding-properties-at-runtime
quarkus.log.level=INFO

## Quarkus http related settings
quarkus.http.port=19120
quarkus.http.test-port=19121
quarkus.http.access-log.enabled=true
# fixed at buildtime
quarkus.resteasy.path=/api/v1
quarkus.resteasy.gzip.enabled=true

## Quarkus auth settings
#quarkus.oidc.credentials.secret=
#quarkus.oidc.client-id=
#quarkus.oidc.auth-server-url=
# fixed at buildtime
quarkus.http.auth.basic=false
quarkus.oidc.enabled=false


## Quarkus swagger settings
# fixed at buildtime
quarkus.swagger-ui.always-include=false
quarkus.swagger-ui.enable=false

## Quarkus monitoring and tracing settings
## jaeger specific settings
quarkus.jaeger.service-name=nessie
quarkus.jaeger.sampler-type=ratelimiting
quarkus.jaeger.sampler-param=1
#quarkus.jaeger.endpoint=http://localhost:14268/api/traces
# fixed at buildtime
quarkus.jaeger.metrics.enabled=true


## sentry specific settings
quarkus.log.sentry.level=ERROR
quarkus.log.sentry.in-app-packages=com.dremio.nessie
quarkus.log.sentry=false
#quarkus.log.sentry.dsn=https://<fillin>.ingest.sentry.io/<fillin>
```

Metrics are published using prometheus and can be collected via standard methods. See:
[Prometheus](https://prometheus.io).

The Swagger UI allows for testing the REST API and reading the API docs. It is available 
via [localhost:19120/swagger-ui](http://localhost:19120/swagger-ui)
