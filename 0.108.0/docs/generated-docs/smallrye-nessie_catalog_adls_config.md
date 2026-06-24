---
search:
  exclude: true
---
<!--start-->

Configuration for ADLS Gen2 object stores. 

Default settings to be applied to all "file systems" (think: buckets) can be set in the `default-options` group. Specific settings for each file system can be specified via the `file-systems` map.   

All settings are optional. The defaults of these settings are defined by the ADLS client  supplied by Microsoft. See [Azure SDK for Java  documentation ](https://learn.microsoft.com/en-us/azure/developer/java/sdk/)

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.service.adls.configuration.`_`<name>`_ |  | `string` | For configuration options, see `com.azure.core.util.Configuration`.  |
