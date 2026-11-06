| Property | Description |
|----------|-------------|
| `nessie.http2-upgrade` | Optional, allow HTTP/2 upgrade, if set to `true`. <br><br>This parameter only works on Java 11 and newer with the Java HTTP client. |
| `nessie.http-redirects` | Optional, specify how redirects are handled. <br><br> * `NEVER`: Never redirect.    <br> * `ALWAYS`: Always redirect.    <br> * `NORMAL`: Always redirect, except from HTTPS URLs to HTTP URLs.  <br><br>This parameter only works on Java 11 and newer with the Java HTTP client. |
