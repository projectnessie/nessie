---
search:
  exclude: true
---
<!--start-->

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.object-stores.health-check.enabled` | `true` | `boolean` | Nessie tries to verify the connectivity to the object stores configured for each warehouse and  exposes this information as a readiness check.  It is recommended to leave this setting enabled. |
