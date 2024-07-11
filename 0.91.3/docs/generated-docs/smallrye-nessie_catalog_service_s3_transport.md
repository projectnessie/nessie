---
search:
  exclude: true
---
<!--start-->

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.service.s3.transport.http.expect-continue-enabled` |  | `boolean` | Override default behavior whether to expect an HTTP/100-Continue.  |
| `nessie.catalog.service.s3.transport.http.connection-time-to-live` |  | `duration` | Override default time-time of a pooled connection.  |
| `nessie.catalog.service.s3.transport.http.connection-max-idle-time` |  | `duration` | Override default max idle time of a pooled connection.  |
| `nessie.catalog.service.s3.transport.http.connection-acquisition-timeout` |  | `duration` | Override default connection acquisition timeout. This is the time a request will wait for a  connection from the pool.  |
| `nessie.catalog.service.s3.transport.http.connect-timeout` |  | `duration` | Override the default TCP connect timeout.  |
| `nessie.catalog.service.s3.transport.http.read-timeout` |  | `duration` | Override the default connection read timeout.  |
| `nessie.catalog.service.s3.transport.http.max-http-connections` |  | `int` | Override the default maximum number of pooled connections.  |
