Default file-system configuration, default/fallback values for all file-systems are taken from  this one.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.service.adls.default-options.default-options.sas-token` |  | `string` | SAS token to access the ADLS file system.  |
| `nessie.catalog.service.adls.default-options.default-options.auth-type` |  | `NONE, STORAGE_SHARED_KEY, SAS_TOKEN, APPLICATION_DEFAULT` | The authentication type to use.  |
| `nessie.catalog.service.adls.default-options.default-options.account` |  | `` | Fully-qualified account name, e.g. `"myaccount.dfs.core.windows.net"` and account key,  configured using the `name` and `secret` fields. If not specified, it will be  queried via the configured credentials provider.   <br><br>**It is strongly recommended to use the SAS token instead of a shared account!** |
| `nessie.catalog.service.adls.default-options.default-options.account.name` |  | `string` |  |
| `nessie.catalog.service.adls.default-options.default-options.account.secret` |  | `string` |  |
| `nessie.catalog.service.adls.default-options.default-options.endpoint` |  | `string` | Define a custom HTTP endpoint. In case clients need to use a different URI, use the `.external-endpoint` setting.  |
| `nessie.catalog.service.adls.default-options.default-options.external-endpoint` |  | `string` | Define a custom HTTP endpoint, this value is used by clients.  |
| `nessie.catalog.service.adls.default-options.default-options.retry-policy` |  | `NONE, EXPONENTIAL_BACKOFF, FIXED_DELAY` | Configure the retry strategy.  |
| `nessie.catalog.service.adls.default-options.default-options.max-retries` |  | `int` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.default-options.default-options.try-timeout` |  | `duration` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.default-options.default-options.retry-delay` |  | `duration` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.default-options.default-options.max-retry-delay` |  | `duration` | Mandatory, if `EXPONENTIAL_BACKOFF` is configured.   |
