---
search:
  exclude: true
---
<!--start-->

| Property | Description |
|----------|-------------|
| `nessie.authentication.oauth2.auth-endpoint` | URL of the OAuth2 authorization endpoint. For Keycloak, this is typically `https://<keycloak-server>/realms/<realm-name>/protocol/openid-connect/auth` .   <br><br>If using the "authorization_code" grant type, either this property or (`nessie.authentication.oauth2.issuer-url`) must be set. In case it is not set, the authorization endpoint  will be discovered from the issuer URL (`nessie.authentication.oauth2.issuer-url`), using the OpenID  Connect Discovery metadata published by the issuer.  |
| `nessie.authentication.oauth2.auth-code-flow.web-port` | Port of the OAuth2 authorization code flow web server. <br><br>When running a client inside a container make sure to specify a port and forward the port to  the container host.   <br><br>The port used for the internal web server that listens for the authorization code callback.  This is only used if the grant type to use is "authorization_code".   <br><br>Optional; if not present, a random port will be used. |
| `nessie.authentication.oauth2.auth-code-flow.timeout` | Defines how long the client should wait for the authorization code flow to complete. This is  only used if the grant type to use is "authorization_code". Optional, defaults to "PT5M". |
