# Podman/Docker compose

You can quickly get started with Nessie variants by following docker templates.

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

## Nessie with Authentication Enabled

The template brings up two containers, one for Nessie and one for Keycloak (OIDC server).
```shell
docker-compose -f authn/docker-compose.yml up
```
- Nessie port - 19120
- Keycloak server port - 8080

The docker template uses bridge network to communicate with the keycloak server. Hence, the token has to be generated with the issuer host `keycloak` <br><br>
_Enter the Nessie container, and generate the token_
```shell
docker exec -it authn_nessie_1 /bin/bash
```
```shell
curl -X POST http://keycloak:8080/auth/realms/master/protocol/openid-connect/token \
--user admin-cli:none -H 'content-type: application/x-www-form-urlencoded' \
-d 'username=admin&password=admin&grant_type=password'
```
Use this token as a bearer token for authenticating the requests to Nessie.
```shell
curl --location --request GET 'http://localhost:19120/api/v1/trees' \
--header 'Authorization: Bearer <TOKEN>'
```
You can configure new users, and reset the expiry time from the keycloak console as described [here](../servers/quarkus-server#readme).

## Nessie with OpenTelemetry Enabled

The template brings up two containers, one for Nessie and one for Jaeger (OpenTelemetry server).

```shell
docker-compose -f telemetry/docker-compose.yml up
```

## Misc
- To check the status - `docker ps -a`
- To stop the containers - `docker-compose stop`
- To start the containers - `docker-compose start`
- To destroy the env - `docker-compose down`
