---
search:
  exclude: true
---
<!--start-->

| Property | Description |
|----------|-------------|
| `nessie.authentication.oauth2.impersonation.enabled` | Whether to enable "impersonation" mode. If enabled, each access token obtained from the OAuth2  server using the configured initial grant type will be exchanged for a new token, using the  token exchange grant type.  |
| `nessie.authentication.oauth2.impersonation.issuer-url` | For impersonation only. The root URL of an alternate OpenID Connect identity issuer provider,  to use when exchanging tokens only.   <br><br>If neither this property nor "nessie.authentication.oauth2.impersonation.token-endpoint" are  defined, the global token endpoint will be used. This means that the same authorization server  will be used for both the initial token request and the token exchange.   <br><br>Endpoint discovery is performed using the OpenID Connect Discovery metadata published by the  issuer. See [OpenID Connect  Discovery 1.0 ](https://openid.net/specs/openid-connect-discovery-1_0.html) for more information.  |
| `nessie.authentication.oauth2.impersonation.token-endpoint` | For impersonation only. The URL of an alternate OAuth2 token endpoint to use when exchanging  tokens only.   <br><br>If neither this property nor "nessie.authentication.oauth2.impersonation.issuer-url" are  defined, the global token endpoint will be used. This means that the same authorization server  will be used for both the initial token request and the token exchange.  |
| `nessie.authentication.oauth2.impersonation.client-id` | For impersonation only. An alternate client ID to use. If not provided, the global client ID  will be used. If provided, and if the client is confidential, then its secret must be provided  as well with "nessie.authentication.oauth2.impersonation.client-secret" – the global client  secret will NOT be used.  |
| `nessie.authentication.oauth2.impersonation.client-secret` | For impersonation only. The client secret to use, if "nessie.authentication.oauth2.impersonation.client-id" is defined and the token exchange client is  confidential.  |
| `nessie.authentication.oauth2.impersonation.scopes` | For impersonation only. Space-separated list of scopes to include in each token exchange  request to the OAuth2 server. Optional. If undefined, the global scopes configured through  "nessie.authentication.oauth2.client-scopes" will be used. If defined and null or empty, no  scopes will be used.   <br><br>The scope names will not be validated by the Nessie client; make sure they are valid  according to [RFC 6749  Section 3.3 ](https://datatracker.ietf.org/doc/html/rfc6749#section-3.3). |
