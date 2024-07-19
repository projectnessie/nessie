ADLS file-system specific options, per file system name.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.service.adls.buckets.file-systems.`_`<filesystem-name>`_`.name` |  | `string` | The name of the filesystem. If unset, the name of the bucket will be extracted from the  configuration option, e.g. if `nessie.catalog.service.adls.filesystem1.name=my-filesystem` is set, the name of the filesystem  will be `my-filesystem`; otherwise, it will be `filesystem1`.   <br><br>This should only be defined if the filesystem name contains non-alphanumeric characters,  such as dots or dashes.  |
| `nessie.catalog.service.adls.buckets.file-systems.`_`<filesystem-name>`_`.sas-token` |  | `string` | SAS token to access the ADLS file system.  |
| `nessie.catalog.service.adls.buckets.file-systems.`_`<filesystem-name>`_`.account` |  | `` | Fully-qualified account name, e.g. `"myaccount.dfs.core.windows.net"` and account key,  configured using the `name` and `secret` fields. If not specified, it will be  queried via the configured credentials provider.   <br><br>**It is strongly recommended to use the SAS token instead of a shared account!** |
| `nessie.catalog.service.adls.buckets.file-systems.`_`<filesystem-name>`_`.account.name` |  | `string` |  |
| `nessie.catalog.service.adls.buckets.file-systems.`_`<filesystem-name>`_`.account.secret` |  | `string` |  |
| `nessie.catalog.service.adls.buckets.file-systems.`_`<filesystem-name>`_`.endpoint` |  | `string` | Define a custom HTTP endpoint. In case clients need to use a different URI, use the `.external-endpoint` setting.  |
| `nessie.catalog.service.adls.buckets.file-systems.`_`<filesystem-name>`_`.external-endpoint` |  | `string` | Define a custom HTTP endpoint, this value is used by clients.  |
| `nessie.catalog.service.adls.buckets.file-systems.`_`<filesystem-name>`_`.retry-policy` |  | `NONE, EXPONENTIAL_BACKOFF, FIXED_DELAY` | Configure the retry strategy.  |
| `nessie.catalog.service.adls.buckets.file-systems.`_`<filesystem-name>`_`.max-retries` |  | `int` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.buckets.file-systems.`_`<filesystem-name>`_`.try-timeout` |  | `duration` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.buckets.file-systems.`_`<filesystem-name>`_`.retry-delay` |  | `duration` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.buckets.file-systems.`_`<filesystem-name>`_`.max-retry-delay` |  | `duration` | Mandatory, if `EXPONENTIAL_BACKOFF` is configured.   |
