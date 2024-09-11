Default bucket configuration, default/fallback values for all buckets are taken from this one.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.service.gcs.default-options.host` |  | `uri` | The default endpoint override to use. The endpoint is almost always used for testing purposes.   <br><br>If the endpoint URIs for the Nessie server and clients differ, this one defines the endpoint  used for the Nessie server.  |
| `nessie.catalog.service.gcs.default-options.external-host` |  | `uri` | When using a specific endpoint, see `host`, and the endpoint URIs for the Nessie server  differ, you can specify the URI passed down to clients using this setting.  Otherwise, clients  will receive the value from the `host` setting.  |
| `nessie.catalog.service.gcs.default-options.user-project` |  | `string` | Optionally specify the user project (Google term).  |
| `nessie.catalog.service.gcs.default-options.project-id` |  | `string` | The Google project ID.  |
| `nessie.catalog.service.gcs.default-options.quota-project-id` |  | `string` | The Google quota project ID.  |
| `nessie.catalog.service.gcs.default-options.client-lib-token` |  | `string` | The Google client lib token.  |
| `nessie.catalog.service.gcs.default-options.auth-type` |  | `NONE, USER, SERVICE_ACCOUNT, ACCESS_TOKEN, APPLICATION_DEFAULT` | The authentication type to use. If not set, the default is `NONE`. |
| `nessie.catalog.service.gcs.default-options.auth-credentials-json` |  | `uri` | Name of the key-secret containing the auth-credentials-JSON, this value is the name of the  credential to use, the actual credential is defined via secrets.   |
| `nessie.catalog.service.gcs.default-options.oauth2-token` |  | `uri` | Name of the token-secret containing the OAuth2 token, this value is the name of the credential  to use, the actual credential is defined via secrets.   |
| `nessie.catalog.service.gcs.default-options.downscoped-credentials.enable` |  | `boolean` | Flag to enable the currently experimental option to send short-lived and scoped-down  credentials to clients.  <br><br>The current default is to not enable short-lived and scoped-down credentials, but the  default may change to enable in the future.  |
| `nessie.catalog.service.gcs.default-options.downscoped-credentials.expiration-margin` |  | `duration` | The expiration margin for the scoped down OAuth2 token. <br><br>Defaults to the Google defaults. |
| `nessie.catalog.service.gcs.default-options.downscoped-credentials.refresh-margin` |  | `duration` | The refresh margin for the scoped down OAuth2 token. <br><br>Defaults to the Google defaults. |
| `nessie.catalog.service.gcs.default-options.read-chunk-size` |  | `int` | The read chunk size in bytes.  |
| `nessie.catalog.service.gcs.default-options.write-chunk-size` |  | `int` | The write chunk size in bytes.  |
| `nessie.catalog.service.gcs.default-options.delete-batch-size` |  | `int` | The delete batch size.  |
| `nessie.catalog.service.gcs.default-options.encryption-key` |  | `uri` | Name of the key-secret containing the customer-supplied AES256 key for blob encryption when  writing.   |
| `nessie.catalog.service.gcs.default-options.decryption-key` |  | `uri` | Name of the key-secret containing the customer-supplied AES256 key for blob decryption when  reading.   |
