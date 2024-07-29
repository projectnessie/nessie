| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.service.s3.sts.sts.session-grace-period` |  | `duration` | The time period to subtract from the S3 session credentials (assumed role credentials) expiry  time to define the time when those credentials become eligible for refreshing.   |
| `nessie.catalog.service.s3.sts.sts.session-cache-max-size` |  | `int` | Maximum number of entries to keep in the session credentials cache (assumed role credentials).  |
| `nessie.catalog.service.s3.sts.sts.clients-cache-max-size` |  | `int` | Maximum number of entries to keep in the STS clients cache.  |
