---
search:
  exclude: true
---
<!--start-->

Configuration for Nessie authorization settings.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.server.authorization.enabled` | `false` | `boolean` | Enable Nessie authorization.  |
| `nessie.server.authorization.type` | `CEL` | `string` | Sets the authorizer type to use.  |
| `nessie.server.authorization.rules.`_`<name>`_ |  | `string` | CEL authorization rules where the key represents the rule id and the value the CEL expression.  |
