---
search:
  exclude: true
---
<!--start-->

| Property | Description |
|----------|-------------|
| `nessie.transport.read-timeout` | Network level read timeout in milliseconds. When running with Java 11, this becomes a request  timeout. Default is 25000 ms.  |
| `nessie.transport.connect-timeout` | Network level connect timeout in milliseconds, default is 5000.  |
| `nessie.transport.disable-compression` | Config property name ("nessie.transport.disable-compression") to disable compression on the  network layer, if set to `true`.  |
| `nessie.ssl.no-certificate-verification` | Optional, disables certificate verifications, if set to `true`. Can be useful for testing  purposes, not recommended for production systems.  |
| `nessie.ssl.cipher-suites` | Optional, list of comma-separated cipher suites for SSL connections. <br><br>This parameter only works on Java 11 and newer with the Java HTTP client. |
| `nessie.ssl.protocols` | Optional, list of comma-separated protocols for SSL connections. <br><br>This parameter only works on Java 11 and newer with the Java HTTP client. |
| `nessie.ssl.sni-hosts` | Optional, comma-separated list of SNI host names for SSL connections. <br><br>This parameter only works on Java 11 and newer with the Java HTTP client. |
| `nessie.ssl.sni-matcher` | Optional, a single SNI matcher for SSL connections. <br><br>Takes a single SNI hostname _matcher_, a regular expression representing the SNI  hostnames to match.   <br><br>This parameter only works on Java 11 and newer with the Java HTTP client. |
