Configuration for Google Cloud Storage (GCS) object stores. 

Contains the default settings to be applied to all buckets. Specific settings for each bucket  can be specified via the `buckets` map.   

All settings are optional. The defaults of these settings are defined by the Google Java SDK  client.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_ |  | `` | Per-bucket configurations. The effective value for a bucket is taken from the per-bucket  setting. If no per-bucket setting is present, uses the defaults from the top-level GCS  settings.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.host` |  | `uri` | The default endpoint override to use. The endpoint is almost always used for testing purposes.   <br><br>If the endpoint URIs for the Nessie server and clients differ, this one defines the endpoint  used for the Nessie server.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.external-host` |  | `uri` | When using a specific endpoint, see `host`, and the endpoint URIs for the Nessie server  differ, you can specify the URI passed down to clients using this setting.  Otherwise, clients  will receive the value from the `host` setting.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.user-project` |  | `string` | Optionally specify the user project (Google term).  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.read-timeout` |  | `duration` | Override the default read timeout.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.connect-timeout` |  | `duration` | Override the default connection timeout.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.project-id` |  | `string` | The Google project ID.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.quota-project-id` |  | `string` | The Google quota project ID.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.client-lib-token` |  | `string` | The Google client lib token.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.auth-type` |  | `NONE, USER, SERVICE_ACCOUNT, ACCESS_TOKEN` | The authentication type to use.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.auth-credentials-json` |  | `string` | Auth-credentials-JSON, this value is the name of the credential to use, the actual credential  is defined via secrets.   |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.oauth2-token` |  | `` | OAuth2 token, this value is the name of the credential to use, the actual credential is defined  via secrets.   |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.oauth2-token.token` |  | `string` |  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.oauth2-token.expires-at` |  | `instant` |  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.max-attempts` |  | `int` | Override the default maximum number of attempts.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.logical-timeout` |  | `duration` | Override the default logical request timeout.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.total-timeout` |  | `duration` | Override the default total timeout.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.initial-retry-delay` |  | `duration` | Override the default initial retry delay.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.max-retry-delay` |  | `duration` | Override the default maximum retry delay.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.retry-delay-multiplier` |  | `double` | Override the default retry delay multiplier.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.initial-rpc-timeout` |  | `duration` | Override the default initial RPC timeout.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.max-rpc-timeout` |  | `duration` | Override the default maximum RPC timeout.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.rpc-timeout-multiplier` |  | `double` | Override the default RPC timeout multiplier.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.read-chunk-size` |  | `int` | The read chunk size in bytes.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.write-chunk-size` |  | `int` | The write chunk size in bytes.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.delete-batch-size` |  | `int` | The delete batch size.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.encryption-key` |  | `string` | Customer-supplied AES256 key for blob encryption when writing.  |
| `nessie.catalog.service.gcs.buckets.`_`<bucket-name>`_`.decryption-key` |  | `string` | Customer-supplied AES256 key for blob decryption when reading.  |
| `nessie.catalog.service.gcs.host` |  | `uri` | The default endpoint override to use. The endpoint is almost always used for testing purposes.   <br><br>If the endpoint URIs for the Nessie server and clients differ, this one defines the endpoint  used for the Nessie server.  |
| `nessie.catalog.service.gcs.external-host` |  | `uri` | When using a specific endpoint, see `host`, and the endpoint URIs for the Nessie server  differ, you can specify the URI passed down to clients using this setting.  Otherwise, clients  will receive the value from the `host` setting.  |
| `nessie.catalog.service.gcs.project-id` |  | `string` | The Google project ID.  |
| `nessie.catalog.service.gcs.quota-project-id` |  | `string` | The Google quota project ID.  |
| `nessie.catalog.service.gcs.client-lib-token` |  | `string` | The Google client lib token.  |
| `nessie.catalog.service.gcs.auth-type` |  | `NONE, USER, SERVICE_ACCOUNT, ACCESS_TOKEN` | The authentication type to use.  |
| `nessie.catalog.service.gcs.auth-credentials-json` |  | `string` | Auth-credentials-JSON, this value is the name of the credential to use, the actual credential  is defined via secrets.   |
| `nessie.catalog.service.gcs.oauth2-token` |  | `` | OAuth2 token, this value is the name of the credential to use, the actual credential is defined  via secrets.   |
| `nessie.catalog.service.gcs.oauth2-token.token` |  | `string` |  |
| `nessie.catalog.service.gcs.oauth2-token.expires-at` |  | `instant` |  |
| `nessie.catalog.service.gcs.max-attempts` |  | `int` | Override the default maximum number of attempts.  |
| `nessie.catalog.service.gcs.logical-timeout` |  | `duration` | Override the default logical request timeout.  |
| `nessie.catalog.service.gcs.total-timeout` |  | `duration` | Override the default total timeout.  |
| `nessie.catalog.service.gcs.initial-retry-delay` |  | `duration` | Override the default initial retry delay.  |
| `nessie.catalog.service.gcs.max-retry-delay` |  | `duration` | Override the default maximum retry delay.  |
| `nessie.catalog.service.gcs.retry-delay-multiplier` |  | `double` | Override the default retry delay multiplier.  |
| `nessie.catalog.service.gcs.initial-rpc-timeout` |  | `duration` | Override the default initial RPC timeout.  |
| `nessie.catalog.service.gcs.max-rpc-timeout` |  | `duration` | Override the default maximum RPC timeout.  |
| `nessie.catalog.service.gcs.rpc-timeout-multiplier` |  | `double` | Override the default RPC timeout multiplier.  |
| `nessie.catalog.service.gcs.read-chunk-size` |  | `int` | The read chunk size in bytes.  |
| `nessie.catalog.service.gcs.write-chunk-size` |  | `int` | The write chunk size in bytes.  |
| `nessie.catalog.service.gcs.delete-batch-size` |  | `int` | The delete batch size.  |
| `nessie.catalog.service.gcs.encryption-key` |  | `string` | Customer-supplied AES256 key for blob encryption when writing.  |
| `nessie.catalog.service.gcs.decryption-key` |  | `string` | Customer-supplied AES256 key for blob decryption when reading.  |
| `nessie.catalog.service.gcs.user-project` |  | `string` | Optionally specify the user project (Google term).  |
| `nessie.catalog.service.gcs.read-timeout` |  | `duration` | Override the default read timeout.  |
| `nessie.catalog.service.gcs.connect-timeout` |  | `duration` | Override the default connection timeout.  |
