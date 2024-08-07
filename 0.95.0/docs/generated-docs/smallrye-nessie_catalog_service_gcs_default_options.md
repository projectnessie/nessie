Default bucket configuration, default/fallback values for all buckets are taken from this one.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.service.gcs.default-options.default-options.auth-type` |  | `NONE, USER, SERVICE_ACCOUNT, ACCESS_TOKEN, APPLICATION_DEFAULT` | The authentication type to use. If not set, the default is `NONE`. |
| `nessie.catalog.service.gcs.default-options.default-options.auth-credentials-json` |  | `string` | Auth-credentials-JSON, this value is the name of the credential to use, the actual credential  is defined via secrets.   |
| `nessie.catalog.service.gcs.default-options.default-options.encryption-key` |  | `string` | Customer-supplied AES256 key for blob encryption when writing.  |
| `nessie.catalog.service.gcs.default-options.default-options.decryption-key` |  | `string` | Customer-supplied AES256 key for blob decryption when reading.  |
| `nessie.catalog.service.gcs.default-options.default-options.host` |  | `uri` | The default endpoint override to use. The endpoint is almost always used for testing purposes.   <br><br>If the endpoint URIs for the Nessie server and clients differ, this one defines the endpoint  used for the Nessie server.  |
| `nessie.catalog.service.gcs.default-options.default-options.external-host` |  | `uri` | When using a specific endpoint, see `host`, and the endpoint URIs for the Nessie server  differ, you can specify the URI passed down to clients using this setting.  Otherwise, clients  will receive the value from the `host` setting.  |
| `nessie.catalog.service.gcs.default-options.default-options.user-project` |  | `string` | Optionally specify the user project (Google term).  |
| `nessie.catalog.service.gcs.default-options.default-options.project-id` |  | `string` | The Google project ID.  |
| `nessie.catalog.service.gcs.default-options.default-options.quota-project-id` |  | `string` | The Google quota project ID.  |
| `nessie.catalog.service.gcs.default-options.default-options.client-lib-token` |  | `string` | The Google client lib token.  |
| `nessie.catalog.service.gcs.default-options.default-options.oauth2-token` |  | `` | OAuth2 token, this value is the name of the credential to use, the actual credential is defined  via secrets.   |
| `nessie.catalog.service.gcs.default-options.default-options.oauth2-token.token` |  | `string` |  |
| `nessie.catalog.service.gcs.default-options.default-options.oauth2-token.expires-at` |  | `instant` |  |
| `nessie.catalog.service.gcs.default-options.default-options.read-chunk-size` |  | `int` | The read chunk size in bytes.  |
| `nessie.catalog.service.gcs.default-options.default-options.write-chunk-size` |  | `int` | The write chunk size in bytes.  |
| `nessie.catalog.service.gcs.default-options.default-options.delete-batch-size` |  | `int` | The delete batch size.  |
