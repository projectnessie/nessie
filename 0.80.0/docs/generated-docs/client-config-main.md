| Property | Description |
|----------|-------------|
| `nessie.uri` | Config property name ("nessie.uri") for the Nessie service URL.  |
| `nessie.authentication.type` | ID of the authentication provider to use, default is no authentication. <br><br>Valid values are `BASIC`, `BEARER`, `OAUTH2` and `AWS`.   <br><br>The value is matched against the values returned as the supported auth-type by  implementations of (`NessieAuthenticationProvider`) across all available authentication  providers.   <br><br>Note that "basic" HTTP authentication is not considered secure, use `BEARER` instead.  |
| `nessie.ref` | Name of the initial Nessie reference, usually `main`.  |
| `nessie.ref.hash` | Commit ID (hash) on "nessie.ref", usually not specified.  |
| `nessie.tracing` | Enable adding the HTTP headers of an active OpenTracing span to all Nessie requests. Disabled  by default.  |
| `nessie.client-builder-name` | Name of the Nessie client to use. If not specified, the implementation prefers the new Java  HTTP client ( `JavaHttp`), if running on Java 11 or newer, or the Java `URLConnection` client. The Apache HTTP client ( `ApacheHttp`) can be used, if it has been  made available on the classpath.  |
| `nessie.client-builder-impl` | Similar to "nessie.client-builder-name", but uses a class name. <br><br>_Deprecated_ Prefer using Nessie client implementation _names_, configured via "nessie.client-builder-name". |
| `nessie.enable-api-compatibility-check` | Enables API compatibility check when creating the Nessie client. The default is `true`.   <br><br>You can also control this setting by setting the system property `nessie.client.enable-api-compatibility-check` to `true` or `false`. |
| `nessie.client-api-version` | Explicitly specify the Nessie API version number to use. The default for this setting depends  on the client being used.  |
