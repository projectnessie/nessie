# Authentication with Keycloak

In this guide we walk through the process of configuring a Nessie Server to authenticate clients against 
a local [Keycloak](https://www.keycloak.org/) server. Docker is use at the runtime environments for both servers.

## Setting up Keycloak

For the purposes of this guide we will only do use a simple Keycloak configuration, that is still sufficient to
demonstrate how OpenID authentication works in Nessie servers.

First, start a Keycloak container using its latest Docker image.

```shell
docker run -p 8080:8080 -e KC_BOOTSTRAP_ADMIN_USERNAME=admin -e KC_BOOTSTRAP_ADMIN_PASSWORD=admin \
  --name keycloak quay.io/keycloak/keycloak:latest start-dev
```

Note the `admin` username and password. Those values will be required to log into the Keycloak Administration Console
that should now be available at http://localhost:8080/admin/.

**Note: when using keycloak < 17 change the URL to `http://localhost:8080/auth/admin/`**

The default realm is called `Master`. On the left-hand pane find the `Manage > Users` page and click `Add User` on the
right side of the (initially empty) users table.

Enter the username "nessie" and click `Save`. Now, under the `Credentials` tab of the `nessie` user page set password
to `nessie` and turn off the `Temporary` flag. Click `Set Password`.
Be sure also to remove all the `Required User Actions` if any.

For the sake of convenience let's increase the default token expiration time.
Goto `Clients` > `admin-cli` > `Advanced Settings`. Set `Access Token Lifespan` to 1 day and click `Save`.

Now we are ready to generate an `access_token` for the `nessie` user. Use the following command
to obtain a token. Then, store it in the `NESSIE_AUTH_TOKEN` environment variable.
It will be required to access Nessie APIs later.

=== "Plain Command"
    ```shell
    curl -X POST \
      http://localhost:8080/realms/master/protocol/openid-connect/token \
      --user admin-cli:none \
      -d 'username=nessie' \
      -d 'password=nessie' \
      -d 'grant_type=password'
    ```
=== "Bash"
    ```bash
    export NESSIE_AUTH_TOKEN=$(curl -X POST \
      http://localhost:8080/realms/master/protocol/openid-connect/token \
      --user admin-cli:none \
      -d 'username=nessie' \
      -d 'password=nessie' \
      -d 'grant_type=password' |jq -r .access_token
      )
    ```

**Note: when using keycloak < 17 change the URL to `http://localhost:8080/auth/realms/master/protocol/openid-connect/token`**

## Setting up Nessie Server

Start the Nessie server container from the `projectnessie/nessie` Docker image in authenticated mode,
using the Keycloak server for validating user credentials.

```shell
docker run -p 19120:19120 \
  -e QUARKUS_OIDC_AUTH_SERVER_URL=http://localhost:8080/realms/master \
  -e QUARKUS_OIDC_CLIENT_ID=projectnessie \
  -e NESSIE_SERVER_AUTHENTICATION_ENABLED=true \
  --network host ghcr.io/projectnessie/nessie:latest
```

**Note: when using keycloak < 17 change the URL to `http://localhost:8080/auth/realms/master/protocol/openid-connect/token`**

Note: this example uses a snapshot build. When Nessie 1.0 is released, the `latest` stable image will be usable
with the instructions from this guide.

## Using Nessie CLI

Now that the Nessie server runs in authenticated mode with a Keycloak, [clients](../nessie-latest/cli.md) have to provide
credentials in the form of bearer authentication tokens. For example:

```shell
nessie --auth-token $NESSIE_AUTH_TOKEN remote show
```

Note: since the name of the `NESSIE_AUTH_TOKEN` variable matches Nessie CLI configuration naming conventions,
the client can automatically find it in the environment, and it does not have to be specified as a command line option.
All `nessie` CLI command will automatically use that token for authenticating their requests.
For example:

```shell
nessie log
```
