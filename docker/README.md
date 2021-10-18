# Docker

You can quickly get started with Nessie variants by following docker templates.

## Nessie with MongoDB
The template brings up two containers, one for nessie and one for DynamoDB. Nessie uses MongoDB as a backing store.
```
docker-compose up
```
- Nessie port - 19120
- MongoDB port - 27017
- MongoDB root credentials - root/password

## Nessie with Amazon DynamoDB
The template brings up two containers, one for nessie and one for DynamoDB.
```
docker-compose -f dynamodb/docker-compose.yml up
```
- Nessie port - 19120
- DynamoDB port - 8000

## Nessie with in-memory backing store

```
docker-compose -f in_memory/docker-compose.yml up
```
- Nessie port - 19120

| WARNING: Bouncing Nessie server resets the in-memory store, which will in-turn reset the data|
| --- |

## Nessie with Authentication Enabled
Nessie supports two modes of authentication - OIDC and Basic.

The template brings up two containers, one for nessie and one for Keycloak (OIDC server).
```
docker-compose -f authn/docker-compose.yml up
```
- Nessie port - 19120
- Keycloak server port - 8080

###OIDC Authentication

The docker template uses bridge network to communicate with the keycloak server. Hence, the token has to be generated with the issuer host `keycloak` <br><br>
_Enter the nessie container, and generate the token_
```
docker exec -it authn_nessie_1 /bin/bash
```
```
curl -X POST http://keycloak:8080/auth/realms/master/protocol/openid-connect/token \
--user admin-cli:none -H 'content-type: application/x-www-form-urlencoded' \
-d 'username=admin&password=admin&grant_type=password'
```
Use this token as a bearer token for authenticating the requests to Nessie.
```
curl --location --request GET 'http://localhost:19120/api/v1/trees' \
--header 'Authorization: Bearer <TOKEN>'
```
You can configure new users, and reset the expiry time from the keycloak console as described [here](../servers/quarkus-server#readme).

###Basic Authentication

Username password authentication not turned-on by default and is meant only for testing purposes.
The docker template doesn't support basic authentication out of the box, because `quarkus.http.auth.basic` property cannot be overridden at runtime.


## Misc
- To check the status - `docker ps -a`
- To stop the containers - `docker-compose stop`
- To start the containers - `docker-compose start`
- To destroy the env - `docker-compose down`