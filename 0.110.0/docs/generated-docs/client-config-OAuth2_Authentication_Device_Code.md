---
search:
  exclude: true
---
<!--start-->

| Property | Description |
|----------|-------------|
| `nessie.authentication.oauth2.device-auth-endpoint` | URL of the OAuth2 device authorization endpoint. For Keycloak, this is typically `http://<keycloak-server>/realms/<realm-name>/protocol/openid-connect/auth/device` .   <br><br>If using the "Device Code" grant type, either this property or (`nessie.authentication.oauth2.issuer-url`) must be set.  |
| `nessie.authentication.oauth2.device-code-flow.timeout` | Defines how long the client should wait for the device code flow to complete. This is only used  if the grant type to use is "device_code". Optional,  defaults to "PT5M". |
| `nessie.authentication.oauth2.device-code-flow.poll-interval` | Defines how often the client should poll the OAuth2 server for the device code flow to  complete.  This is only used if the grant type to use is "device_code". Optional, defaults to "PT5S". |
