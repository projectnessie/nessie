| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.service.s3.transport.http.expect-continue-enabled` |  | `boolean` | Override default behavior whether to expect an HTTP/100-Continue.  |
| `nessie.catalog.service.s3.transport.http.connection-time-to-live` |  | `duration` | Override default time-time of a pooled connection.  |
| `nessie.catalog.service.s3.transport.http.connection-max-idle-time` |  | `duration` | Override default max idle time of a pooled connection.  |
| `nessie.catalog.service.s3.transport.http.connection-acquisition-timeout` |  | `duration` | Override default connection acquisition timeout. This is the time a request will wait for a  connection from the pool.  |
| `nessie.catalog.service.s3.transport.http.connect-timeout` |  | `duration` | Override the default TCP connect timeout.  |
| `nessie.catalog.service.s3.transport.http.read-timeout` |  | `duration` | Override the default connection read timeout.  |
| `nessie.catalog.service.s3.transport.http.max-http-connections` |  | `int` | Override the default maximum number of pooled connections.  |
| `nessie.catalog.service.s3.transport.trust-all-certificates` |  | `boolean` | Instruct the S3 HTTP client to accept all SSL certificates, if set to `true`. Enabling  this option is dangerous, it is strongly recommended to leave this option unset or `false` . |
| `nessie.catalog.service.s3.transport.trust-store.path` |  | `path` | Override to set the file path to a custom SSL trust store. `nessie.catalog.service.s3.trust-store.type` and `nessie.catalog.service.s3.trust-store.password` must be supplied as well when providing a  custom trust store.   <br><br>When running in k8s or Docker, the path is local within the pod/container and must be  explicitly mounted.  |
| `nessie.catalog.service.s3.transport.trust-store.type` |  | `string` | Override to set the type of the custom SSL trust store specified in `nessie.catalog.service.s3.trust-store.path` . <br><br>Supported types include `JKS`, `PKCS12`, and all key store types supported by  Java 17.  |
| `nessie.catalog.service.s3.transport.trust-store.password` |  | `` | Override to set the password for the custom SSL trust store specified in `nessie.catalog.service.s3.trust-store.path` .  |
| `nessie.catalog.service.s3.transport.trust-store.password.key` |  | `string` |  |
| `nessie.catalog.service.s3.transport.key-store.path` |  | `path` | Override to set the file path to a custom SSL key store. `nessie.catalog.service.s3.key-store.type` and `nessie.catalog.service.s3.key-store.password` must be supplied as well when providing a custom  key store.   <br><br>When running in k8s or Docker, the path is local within the pod/container and must be  explicitly mounted.  |
| `nessie.catalog.service.s3.transport.key-store.type` |  | `string` | Override to set the type of the custom SSL key store specified in `nessie.catalog.service.s3.key-store.path` . <br><br>Supported types include `JKS`, `PKCS12`, and all key store types supported by  Java 17.  |
| `nessie.catalog.service.s3.transport.key-store.password` |  | `` | Override to set the password for the custom SSL key store specified in `nessie.catalog.service.s3.key-store.path` .  |
| `nessie.catalog.service.s3.transport.key-store.password.key` |  | `string` |  |