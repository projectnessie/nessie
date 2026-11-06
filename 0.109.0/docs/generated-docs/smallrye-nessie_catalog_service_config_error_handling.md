---
search:
  exclude: true
---
<!--start-->

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.object-stores.health-check.enabled` | `true` | `boolean` | Nessie tries to verify the connectivity to the object stores configured for each warehouse and  exposes this information as a readiness check.  It is recommended to leave this setting enabled. |
| `nessie.catalog.error-handling.throttled-retry-after` | `PT10S` | `duration` | Advanced property. The time interval after which a request is retried when storage I/O responds  with some "retry later" response.  |
