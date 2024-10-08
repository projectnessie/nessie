# Podman/Docker Compose Examples

You can quickly get started with Nessie by playing with the examples provided in this directory. 
The examples are designed to be run with `docker-compose` or `podman-compose`.

## Instructions for podman

When you use podman, make sure that:
* you have `podman-compose` installed (on Ubuntu: `pip3 install podman-compose` as `root`).
* your network-backend is configured to use `netavark`, check `podman info | grep network`.
  This is needed to let podman-compose configure DNS entries in the pods.
* Settings for `/etc/containers/containers.conf`:
  ```
  [engine]
  compose_providers=[ "podman-compose" ]
  [network]
  network_backend = "netavark"
  ```
  _Might_ need to run `podman system reset --force` when changing the network backend. 

## Nessie all-in-one

The template brings up Nessie with Iceberg REST API, backed by a PostgreSQL database. Authentication
is provided by Keycloak. Object storage is provided by MinIO. Prometheus and Grafana are also
included for monitoring, and Jaeger for tracing. And finally, a Spark SQL server is included for
testing, and also a Nessie CLI container for interacting with the Nessie server.

```shell
docker-compose -f all-in-one/docker-compose.yml up
```

See usage instructions in the compose file.

## Nessie with MongoDB
The template brings up two containers, one for Nessie and one for MongoDB. Nessie uses MongoDB as a backing store.
```shell
docker-compose -f mongodb/docker-compose.yml up
```
- Nessie port - 19120
- MongoDB port - 27017
- MongoDB root credentials - root/password

## Nessie with Amazon DynamoDB
The template brings up two containers, one for Nessie and one for DynamoDB.
```shell
docker-compose -f dynamodb/docker-compose.yml up
```
- Nessie port - 19120
- DynamoDB port - 8000

## Nessie with an In-memory Store

```shell
docker-compose -f in_memory/docker-compose.yml up
```
- Nessie port - 19120

| WARNING: Bouncing Nessie server resets the in-memory store, which will in-turn reset the data|
| --- |

## Nessie with Keycloak

The template brings up two containers, one for Nessie and one for Keycloak (OIDC server).

```shell
docker-compose -f authn-keycloak/docker-compose.yml up
```

- Nessie port - 19120
- Keycloak server port - 8080

See usage instructions in the compose file.

You can configure new users, and reset the expiry time from the keycloak console as described [here](../servers/quarkus-server#readme).

## Nessie with Authelia

The template brings up two containers, one for Nessie and one for Authelia.

```shell
docker-compose -f authn-authelia/docker-compose.yml up
```

See usage instructions in the compose file.

## Nessie with OpenTelemetry

The template brings up two containers, one for Nessie and one for Jaeger (OpenTelemetry server).

```shell
docker-compose -f telemetry/docker-compose.yml up
```

## Nessie with Nginx

The template brings up three containers, Nessie, Nginx, and MinIO.

```shell
docker-compose -f telemetry/docker-compose.yml up
```

In this example, Nessie is accessible in HTTPS at
https://nessie-nginx.localhost.localdomain:8443/nessie/api/v2/. See usage instructions in the
compose file.

## Misc
- To check the status - `docker ps -a`
- To stop the containers - `docker-compose stop`
- To start the containers - `docker-compose start`
- To destroy the env - `docker-compose down`
