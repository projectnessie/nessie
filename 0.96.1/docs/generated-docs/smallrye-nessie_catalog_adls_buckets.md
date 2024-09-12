ADLS file-system specific options, per file system name.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.name` |  | `string` | The name of the filesystem. If unset, the name of the bucket will be extracted from the  configuration option, e.g. if `nessie.catalog.service.adls.filesystem1.name=my-filesystem` is set, the name of the filesystem  will be `my-filesystem`; otherwise, it will be `filesystem1`.   <br><br>This should only be defined if the filesystem name contains non-alphanumeric characters,  such as dots or dashes.  |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.auth-type` |  | `NONE, STORAGE_SHARED_KEY, SAS_TOKEN, APPLICATION_DEFAULT` | The authentication type to use.  |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.account` |  | `uri` | Name of the basic-credentials secret containing the fully-qualified account name, e.g. `"myaccount.dfs.core.windows.net"` and account key, configured using the `name` and `secret` fields. If not specified, it will be queried via the configured credentials provider.  |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.sas-token` |  | `uri` | Name of the key-secret containing the SAS token to access the ADLS file system.  |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.user-delegation.enable` |  | `boolean` | Enable short-lived user-delegation SAS tokens per file-system. <br><br>The current default is to not enable short-lived and scoped-down credentials, but the  default may change to enable in the future.  |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.user-delegation.key-expiry` |  | `duration` | Expiration time / validity duration of the user-delegation _key_, this key is  _not_ passed to the client.  <br><br>Defaults to 7 days minus 1 minute (the maximum), must be >= 1 second. |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.user-delegation.sas-expiry` |  | `duration` | Expiration time / validity duration of the user-delegation _SAS token_, which  _is_ sent to the client.  <br><br>Defaults to 3 hours, must be >= 1 second. |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.endpoint` |  | `string` | Define a custom HTTP endpoint. In case clients need to use a different URI, use the `.external-endpoint` setting.  |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.external-endpoint` |  | `string` | Define a custom HTTP endpoint, this value is used by clients.  |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.retry-policy` |  | `NONE, EXPONENTIAL_BACKOFF, FIXED_DELAY` | Configure the retry strategy.  |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.max-retries` |  | `int` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.try-timeout` |  | `duration` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.retry-delay` |  | `duration` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.max-retry-delay` |  | `duration` | Mandatory, if `EXPONENTIAL_BACKOFF` is configured.   |
