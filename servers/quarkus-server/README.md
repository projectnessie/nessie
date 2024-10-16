# Running in Test Mode With Authentication

Here's a very basic setup for running the Nessie Server with Keycloak authentication.

It is meant to be used for testing purposes only.

1. Start a local Keycloak server
    ```shell
    docker run -p 8080:8080 -e KC_BOOTSTRAP_ADMIN_USERNAME=admin -e KC_BOOTSTRAP_ADMIN_PASSWORD=admin --name keycloak quay.io/keycloak/keycloak:latest start-dev
    ```
1. Log into Keycloak Admin Console at http://localhost:8080/auth/
1. Create user `nessie` with password `nessie` (under the `master` realm)
1. Goto `Clients > Admin-cli > Advanced Settings` and set Access Token Lifespan to 1 day (for convenience)
1. Start Nessie server in test mode preconfigured to use the local Keycloak server:
    ```shell
    ./mvnw -pl :nessie-quarkus quarkus:dev -Dnessie.server.authentication.enabled=true -Dquarkus.oidc.auth-server-url=http://localhost:8080/auth/realms/master -Dquarkus.oidc.client-id=projectnessie
    ```
1. Generate a token for `nessie` (note that the default token lifespan is pretty short):
    ```shell
    curl -X POST http://localhost:8080/auth/realms/master/protocol/openid-connect/token \
      --user admin-cli:none -H 'content-type: application/x-www-form-urlencoded' \
      -d 'username=nessie&password=nessie&grant_type=password' |jq -r .access_token
    ```
1. Access Nessie API with the auth token
   * With `curl`:
       ```shell
       curl 'http://localhost:19120/api/v1/trees' --oauth2-bearer "$NESSIE_TOKEN" -v
       ```
   * With Nessie CLI:
     ```shell
     nessie --auth-token "$NESSIE_TOKEN" remote show
     ```
