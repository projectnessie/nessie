---
search:
  exclude: true
---
<!--start-->

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.service.adls.transport.max-http-connections` |  | `int` | Override the default maximum number of HTTP connections that Nessie can use against all ADLS  Gen2 object stores.   |
| `nessie.catalog.service.adls.transport.connect-timeout` |  | `duration` | Override the default TCP connect timeout for HTTP connections against ADLS Gen2 object stores.  |
| `nessie.catalog.service.adls.transport.connection-idle-timeout` |  | `duration` | Override the default idle timeout for HTTP connections.  |
| `nessie.catalog.service.adls.transport.write-timeout` |  | `duration` | Override the default write timeout for HTTP connections.  |
| `nessie.catalog.service.adls.transport.read-timeout` |  | `duration` | Override the default read timeout for HTTP connections.  |
