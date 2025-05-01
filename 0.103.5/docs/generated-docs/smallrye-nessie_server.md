---
search:
  exclude: true
---
<!--start-->

Nessie server configuration to be injected into the JAX-RS application.

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.server.default-branch` | `main` | `string` | The default branch to use if not provided by the user.  |
| `nessie.server.send-stacktrace-to-client` | `false` | `boolean` | Whether stack traces should be sent to the client in case of error. The default is `false` to not expose internal details for security reasons.  |
| `nessie.server.access-checks-batch-size` | `100` | `int` | The number of entity-checks that are grouped into a call to `BatchAccessChecker`. The default  value is quite conservative, it is the responsibility of the operator to adjust this value  according to the capabilities of the actual authz implementation. Note that the number of  checks can be slightly exceeded by the implementation, depending on the call site.  |
