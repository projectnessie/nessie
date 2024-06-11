---
search:
  exclude: true
---
<!--start-->

Configuration for ADLS Gen2 object stores. 

Contains the default settings to be applied to all "file systems" (think: buckets). Specific  settings for each file system can be specified via the `file-systems` map.   

All settings are optional. The defaults of these settings are defined by the ADLS client  supplied by Microsoft. See [Azure SDK for Java  documentation ](https://learn.microsoft.com/en-us/azure/developer/java/sdk/)

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.service.adls.max-http-connections` |  | `int` | Override the default maximum number of HTTP connections that Nessie can use against all ADLS  Gen2 object stores.   |
| `nessie.catalog.service.adls.connect-timeout` |  | `duration` | Override the default TCP connect timeout for HTTP connections against ADLS Gen2 object stores.  |
| `nessie.catalog.service.adls.connection-idle-timeout` |  | `duration` | Override the default idle timeout for HTTP connections.  |
| `nessie.catalog.service.adls.write-timeout` |  | `duration` | Override the default write timeout for HTTP connections.  |
| `nessie.catalog.service.adls.read-timeout` |  | `duration` | Override the default read timeout for HTTP connections.  |
| `nessie.catalog.service.adls.response-timeout` |  | `duration` | Override the default response timeout for HTTP connections.  |
| `nessie.catalog.service.adls.configuration.`_`<name>`_ |  | `string` | Custom settings for the ADLS Java client.  |
| `nessie.catalog.service.adls.write-block-size` |  | `long` | Override the default write block size used when writing to ADLS.  |
| `nessie.catalog.service.adls.read-block-size` |  | `int` | Override the default read block size used when writing to ADLS.  |
| `nessie.catalog.service.adls.account` |  | `` | Fully-qualified account name, e.g. `"myaccount.dfs.core.windows.net"` and account key,  configured using the `name` and `secret` fields. If not specified, it will be  queried via the configured credentials provider.  |
| `nessie.catalog.service.adls.account.name` |  | `string` |  |
| `nessie.catalog.service.adls.account.secret` |  | `string` |  |
| `nessie.catalog.service.adls.sas-token` |  | `string` | SAS token to access the ADLS file system.  |
| `nessie.catalog.service.adls.endpoint` |  | `string` | Define a custom HTTP endpoint. In case clients need to use a different URI, use the `.external-endpoint` setting.  |
| `nessie.catalog.service.adls.external-endpoint` |  | `string` | Define a custom HTTP endpoint, this value is used by clients.  |
| `nessie.catalog.service.adls.retry-policy` |  | `NONE, EXPONENTIAL_BACKOFF, FIXED_DELAY` | Configure the retry strategy.  |
| `nessie.catalog.service.adls.max-retries` |  | `int` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.try-timeout` |  | `duration` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.retry-delay` |  | `duration` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.max-retry-delay` |  | `duration` | Mandatory, if `EXPONENTIAL_BACKOFF` is configured.   |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_ |  | `` | ADLS file-system specific options, per file system name.  |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.account` |  | `` | Fully-qualified account name, e.g. `"myaccount.dfs.core.windows.net"` and account key,  configured using the `name` and `secret` fields. If not specified, it will be  queried via the configured credentials provider.  |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.account.name` |  | `string` |  |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.account.secret` |  | `string` |  |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.sas-token` |  | `string` | SAS token to access the ADLS file system.  |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.endpoint` |  | `string` | Define a custom HTTP endpoint. In case clients need to use a different URI, use the `.external-endpoint` setting.  |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.external-endpoint` |  | `string` | Define a custom HTTP endpoint, this value is used by clients.  |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.retry-policy` |  | `NONE, EXPONENTIAL_BACKOFF, FIXED_DELAY` | Configure the retry strategy.  |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.max-retries` |  | `int` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.try-timeout` |  | `duration` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.retry-delay` |  | `duration` | Mandatory, if any `retry-policy` is configured.   |
| `nessie.catalog.service.adls.file-systems.`_`<filesystem-name>`_`.max-retry-delay` |  | `duration` | Mandatory, if `EXPONENTIAL_BACKOFF` is configured.   |
