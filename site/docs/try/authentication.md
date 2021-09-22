# Authentication

By default, Nessie servers run with authentication disabled and all requests are processed under the "anonymous"
user identity.

## OpenID Bearer Tokens

Nessie supports bearer tokens and uses [OpenID Connect](https://openid.net/connect/) for validating them.

To enable bearer authentication the following [configuration](./configuration.md) properties need to be set 
for the Nessie Server process:

* `nessie.server.authentication.enabled=true`
* `quarkus.oidc.auth-server-url=<OpenID Server URL>`
* `quarkus.oidc.client-id=<Client ID>`

When using Nessie [docker](./docker.md) images, the authentication options can be specified on
the docker command line as environment variables, for example:

```bash
$ docker run -p 19120:19120 -e QUARKUS_OIDC_CLIENT_ID=<Client ID> -e QUARKUS_OIDC_AUTH_SERVER_URL=<OpenID Server URL> -e NESSIE_SERVER_AUTHENTICATION_ENABLED=true --network host projectnessie/nessie
```

Note the use of the `host` docker network. In this example, it is assumed that the Open ID Server
is available on the host network. More advanced network setup is possible, of course.
