---
search:
  exclude: true
---
<!--start-->

| Property | Description |
|----------|-------------|
| `nessie.authentication.oauth2.issuer-url` | OAuth2 issuer URL. <br><br>The root URL of the OpenID Connect identity issuer provider, which will be used for  discovering supported endpoints and their locations. For Keycloak, this is typically the realm  URL: `https://<keycloak-server>/realms/<realm-name>`.   <br><br>Endpoint discovery is performed using the OpenID Connect Discovery metadata published by the  issuer. See [OpenID Connect  Discovery 1.0 ](https://openid.net/specs/openid-connect-discovery-1_0.html) for more information.   <br><br>Either this property or (`nessie.authentication.oauth2.token-endpoint`) must be set.  |
| `nessie.authentication.oauth2.token-endpoint` | URL of the OAuth2 token endpoint. For Keycloak, this is typically `https://<keycloak-server>/realms/<realm-name>/protocol/openid-connect/token` .   <br><br>Either this property or (`nessie.authentication.oauth2.issuer-url`) must be set. In case it is  not set, the token endpoint will be discovered from the issuer URL (`nessie.authentication.oauth2.issuer-url`), using the OpenID Connect Discovery metadata published by the issuer. |
| `nessie.authentication.oauth2.grant-type` | The grant type to use when authenticating against the OAuth2 server. Valid values are:   <br><br> * "client_credentials" <br> * "password" <br> * "authorization_code" <br> * "device_code" <br> * "token_exchange" <br><br>Optional, defaults to "client_credentials". |
| `nessie.authentication.oauth2.client-id` | Client ID to use when authenticating against the OAuth2 server. Required if using OAuth2  authentication, ignored otherwise.  |
| `nessie.authentication.oauth2.client-secret` | Client secret to use when authenticating against the OAuth2 server. Required if using OAuth2  authentication, ignored otherwise.  |
| `nessie.authentication.oauth2.client-scopes` | Space-separated list of scopes to include in each request to the OAuth2 server. Optional,  defaults to empty (no scopes).   <br><br>The scope names will not be validated by the Nessie client; make sure they are valid  according to [RFC 6749  Section 3.3 ](https://datatracker.ietf.org/doc/html/rfc6749#section-3.3). |
