---
title: "Authentication"
---

# Authentication

By default, Nessie servers run with authentication disabled and all requests are processed under the "anonymous"
user identity. In Nessie clients this authentication type is known as `NONE`.

When a Nessie server runs as an AWS Lambda, access to its API is controlled by AWS authentication settings.
In this case there is no need to configure any additional authentication in the Nessie server.
In Nessie clients this authentication type is known as `AWS`.  

When a Nessie API is exposed to clients without any external authentication layer, the server itself can be
configured to authenticate clients using [OpenID tokens](https://openid.net/specs/openid-connect-core-1_0.html)
as described in the section below. On the client side this authentication type is known as `BEARER` authentication.

For client-side authentication settings refer to the following pages:

* [Nessie CLI](cli.md)
* [Java Client](../develop/java.md)
* [Authentication in Tools](client_config.md)

## OpenID Bearer Tokens

Nessie supports bearer tokens and uses [OpenID Connect](https://openid.net/connect/) for validating them.

To enable bearer authentication the following [configuration](configuration.md) properties need to be set 
for the Nessie Server process:

* `nessie.server.authentication.enabled=true`
* `quarkus.oidc.auth-server-url=<OpenID Server URL>`
* `quarkus.oidc.client-id=<Client ID>`

If you also want to use authentication for the Nessie Web UI, the following properties need to
be set as well:

* `quarkus.oidc.ui-app.auth-server-url=<OpenID Server URL>`
* `quarkus.oidc.ui-app.client-id=<Client ID>`
* `quarkus.oidc.ui-app.application-type=web-app`
* `quarkus.oidc.ui-app.tenant-paths=/,/tree*,/content*,/notfound*,/commits*`

When using Nessie [Docker](../guides/docker.md) images, the authentication options can be specified on
the `docker` command line as environment variables, for example:

```bash
$ docker run -p 19120:19120 \
  -e QUARKUS_OIDC_CLIENT_ID=$YOUR_OIDC_Client_ID \
  -e QUARKUS_OIDC_AUTH_SERVER_URL=$YOUR_OpenID_Server_URL \
  -e NESSIE_SERVER_AUTHENTICATION_ENABLED=true \
  --network host \
  ghcr.io/projectnessie/nessie
```

Note the use of the `host` Docker network. In this example, it is assumed that the Open ID Server
is available on the host network. More advanced network setup is possible, of course.
