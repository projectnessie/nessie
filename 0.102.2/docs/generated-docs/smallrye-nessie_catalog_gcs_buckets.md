---
search:
  exclude: true
---
<!--start-->

Per-bucket configurations. The effective value for a bucket is taken from the per-bucket  setting. If no per-bucket setting is present, uses the defaults from the top-level GCS settings  in `default-options`.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.host` |  | `uri` | The default endpoint override to use. The endpoint is almost always used for testing purposes.   <br><br>If the endpoint URIs for the Nessie server and clients differ, this one defines the endpoint  used for the Nessie server.  |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.external-host` |  | `uri` | When using a specific endpoint, see `host`, and the endpoint URIs for the Nessie server  differ, you can specify the URI passed down to clients using this setting.  Otherwise, clients  will receive the value from the `host` setting.  |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.user-project` |  | `string` | Optionally specify the user project (Google term).  |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.project-id` |  | `string` | The Google project ID.  |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.quota-project-id` |  | `string` | The Google quota project ID.  |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.client-lib-token` |  | `string` | The Google client lib token.  |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.auth-type` |  | `NONE, USER, SERVICE_ACCOUNT, ACCESS_TOKEN, APPLICATION_DEFAULT` | The authentication type to use. If not set, the default is `NONE`. |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.auth-credentials-json` |  | `uri` | Name of the key-secret containing the auth-credentials-JSON, this value is the name of the  credential to use, the actual credential is defined via secrets.   |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.oauth2-token` |  | `uri` | Name of the token-secret containing the OAuth2 token, this value is the name of the credential  to use, the actual credential is defined via secrets.   |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.downscoped-credentials.enable` |  | `boolean` | Flag to enable the currently experimental option to send short-lived and scoped-down  credentials to clients.  <br><br>The current default is to not enable short-lived and scoped-down credentials, but the  default may change to enable in the future.  |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.downscoped-credentials.expiration-margin` |  | `duration` | The expiration margin for the scoped down OAuth2 token. <br><br>Defaults to the Google defaults. |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.downscoped-credentials.refresh-margin` |  | `duration` | The refresh margin for the scoped down OAuth2 token. <br><br>Defaults to the Google defaults. |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.read-chunk-size` |  | `int` | The read chunk size in bytes.  |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.write-chunk-size` |  | `int` | The write chunk size in bytes.  |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.delete-batch-size` |  | `int` | The delete batch size.  |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.encryption-key` |  | `uri` | Name of the key-secret containing the customer-supplied AES256 key for blob encryption when  writing.   |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.decryption-key` |  | `uri` | Name of the key-secret containing the customer-supplied AES256 key for blob decryption when  reading.   |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.name` |  | `string` | The human consumable name of the bucket. If unset, the name of the bucket will be extracted  from the configuration option name, e.g. if `nessie.catalog.service.s3.bucket1.name=my-bucket` is set, the bucket name will be `my-bucket` ; otherwise, it will be `bucket1`.   <br><br>This can be used; if the bucket name contains non-alphanumeric characters, such as dots or  dashes.  |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.authority` |  | `string` | The authority part in a storage location URI. This is the bucket name for S3 and GCS, for ADLS  this is the storage account name (optionally prefixed with the container/file-system name).  Defaults to (`#name()`).   <br><br>For S3 and GCS this option should mention the name of the bucket.   <br><br>For ADLS: The value of this option is using the `container@storageAccount` syntax. It  is mentioned as `<file_system>@<account_name>` in the [Azure  Docs ](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri). Note that the `<file_system>@` part is optional, `<account_name>` is the  fully qualified name, usually ending in `.dfs.core.windows.net`. |
| `nessie.catalog.service.gcs.buckets.`_`<key>`_`.path-prefix` |  | `string` | The path prefix for this storage location.  |
