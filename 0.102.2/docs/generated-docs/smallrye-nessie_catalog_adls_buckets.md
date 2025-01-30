---
search:
  exclude: true
---
<!--start-->

Per-bucket configurations. The effective value for a bucket is taken from the per-bucket  setting. If no per-bucket setting is present, uses the defaults from the top-level ADLS  settings in `default-options`.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.service.adls.file-systems.`_`<key>`_`.auth-type` |  | `NONE, STORAGE_SHARED_KEY, SAS_TOKEN, APPLICATION_DEFAULT` | The authentication type to use.  |
| `nessie.catalog.service.adls.file-systems.`_`<key>`_`.account` |  | `uri` | Name of the basic-credentials secret containing the fully-qualified account name, e.g. `"myaccount.dfs.core.windows.net"` and account key, configured using the `name` and `secret` fields. If not specified, it will be queried via the configured credentials provider.  |
| `nessie.catalog.service.adls.file-systems.`_`<key>`_`.sas-token` |  | `uri` | Name of the key-secret containing the SAS token to access the ADLS file system.  |
| `nessie.catalog.service.adls.file-systems.`_`<key>`_`.user-delegation.enable` |  | `boolean` | Enable short-lived user-delegation SAS tokens per file-system. <br><br>The current default is to not enable short-lived and scoped-down credentials, but the  default may change to enable in the future.  |
| `nessie.catalog.service.adls.file-systems.`_`<key>`_`.user-delegation.key-expiry` |  | `duration` | Expiration time / validity duration of the user-delegation _key_, this key is  _not_ passed to the client.  <br><br>Defaults to 7 days minus 1 minute (the maximum), must be >= 1 second. |
| `nessie.catalog.service.adls.file-systems.`_`<key>`_`.user-delegation.sas-expiry` |  | `duration` | Expiration time / validity duration of the user-delegation _SAS token_, which  _is_ sent to the client.  <br><br>Defaults to 3 hours, must be >= 1 second. |
| `nessie.catalog.service.adls.file-systems.`_`<key>`_`.endpoint` |  | `string` | Define a custom HTTP endpoint. In case clients need to use a different URI, use the `.external-endpoint` setting.  |
| `nessie.catalog.service.adls.file-systems.`_`<key>`_`.external-endpoint` |  | `string` | Define a custom HTTP endpoint, this value is used by clients.  |
| `nessie.catalog.service.adls.file-systems.`_`<key>`_`.retry-policy` |  | `NONE, EXPONENTIAL_BACKOFF, FIXED_DELAY` | Configure the retry strategy.  |
| `nessie.catalog.service.adls.file-systems.`_`<key>`_`.max-retries` |  | `int` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.file-systems.`_`<key>`_`.try-timeout` |  | `duration` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.file-systems.`_`<key>`_`.retry-delay` |  | `duration` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.file-systems.`_`<key>`_`.max-retry-delay` |  | `duration` | Mandatory, if `EXPONENTIAL_BACKOFF` is configured.   |
| `nessie.catalog.service.adls.file-systems.`_`<key>`_`.name` |  | `string` | The human consumable name of the bucket. If unset, the name of the bucket will be extracted  from the configuration option name, e.g. if `nessie.catalog.service.s3.bucket1.name=my-bucket` is set, the bucket name will be `my-bucket` ; otherwise, it will be `bucket1`.   <br><br>This can be used; if the bucket name contains non-alphanumeric characters, such as dots or  dashes.  |
| `nessie.catalog.service.adls.file-systems.`_`<key>`_`.authority` |  | `string` | The authority part in a storage location URI. This is the bucket name for S3 and GCS, for ADLS  this is the storage account name (optionally prefixed with the container/file-system name).  Defaults to (`#name()`).   <br><br>For S3 and GCS this option should mention the name of the bucket.   <br><br>For ADLS: The value of this option is using the `container@storageAccount` syntax. It  is mentioned as `<file_system>@<account_name>` in the [Azure  Docs ](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri). Note that the `<file_system>@` part is optional, `<account_name>` is the  fully qualified name, usually ending in `.dfs.core.windows.net`. |
| `nessie.catalog.service.adls.file-systems.`_`<key>`_`.path-prefix` |  | `string` | The path prefix for this storage location.  |
