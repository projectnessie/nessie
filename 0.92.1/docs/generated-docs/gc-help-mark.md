```
Usage: nessie-gc.jar mark-live [-hV] [-c=<defaultCutoffPolicy>]
                               [--identify-parallelism=<parallelism>] [--nessie-api=<nessieApi>]
                               [--nessie-client=<nessieClientName>] [-R=<cutoffPolicyRefTime>]
                               [--time-zone=<zoneId>] [-u=<nessieUri>]
                               [--write-live-set-id-to=<liveSetIdFile>] [-C[=<String=String>[,
                               <String=String>...]...]]... [-o[=<String=String>[,
                               <String=String>...]...]]... ([--inmemory] | [[--jdbc]
                               --jdbc-url=<url> [--jdbc-properties[=<String=String>[,
                               <String=String>...]...]]... [--jdbc-user=<user>]
                               [--jdbc-password=<password>] [--jdbc-schema=<schemaCreateStrategy>]])
Run identify-live-content phase of Nessie GC, must not be used with the in-memory contents-storage.
  -c, --default-cutoff=<defaultCutoffPolicy>
                             Default cutoff policy. Policies can be one of:
                             - number of commits as an integer value
                             - a duration (see java.time.Duration)
                             - an ISO instant
                             - 'NONE', means everything's considered as live
  -C, --cutoff[=<String=String>[,<String=String>...]...]
                             Cutoff policies per reference names. Supplied as a
                               ref-name-pattern=policy tuple.
                             Reference name patterns are regular expressions.
                             Policies can be one of:
                             - number of commits as an integer value
                             - a duration (see java.time.Duration)
                             - an ISO instant
                             - 'NONE', means everything's considered as live
  -h, --help                 Show this help message and exit.
      --identify-parallelism=<parallelism>
                             Number of Nessie references that can be walked in parallel.
      --inmemory             Flag whether to use the in-memory contents storage. Prefer a JDBC
                               storage.
      --inmemory             Flag whether to use the in-memory contents storage. Prefer a JDBC
                               storage.
      --jdbc                 Flag whether to use the JDBC contents storage.
      --jdbc                 Flag whether to use the JDBC contents storage.
      --jdbc-password=<password>
                             JDBC password used to authenticate the database access.
      --jdbc-password=<password>
                             JDBC password used to authenticate the database access.
      --jdbc-properties[=<String=String>[,<String=String>...]...]
                             JDBC parameters.
      --jdbc-properties[=<String=String>[,<String=String>...]...]
                             JDBC parameters.
      --jdbc-schema=<schemaCreateStrategy>
                             How to create the database schema. Possible values: CREATE,
                               DROP_AND_CREATE, CREATE_IF_NOT_EXISTS.
      --jdbc-schema=<schemaCreateStrategy>
                             How to create the database schema. Possible values: CREATE,
                               DROP_AND_CREATE, CREATE_IF_NOT_EXISTS.
      --jdbc-url=<url>       JDBC URL of the database to connect to.
      --jdbc-url=<url>       JDBC URL of the database to connect to.
      --jdbc-user=<user>     JDBC user name used to authenticate the database access.
      --jdbc-user=<user>     JDBC user name used to authenticate the database access.
      --nessie-api=<nessieApi>
                             Class name of the NessieClientBuilder implementation to use, defaults
                               to HttpClientBuilder suitable for REST. Using this parameter is not
                               recommended. Prefer the --nessie-client parameter instead.
      --nessie-client=<nessieClientName>
                             Name of the Nessie client to use, defaults to HTTP suitable for REST.
  -o, --nessie-option[=<String=String>[,<String=String>...]...]
                             Parameters to configure the NessieClientBuilder.
  -R, --cutoff-ref-time=<cutoffPolicyRefTime>
                             Reference timestamp for durations specified for --cutoff. Defaults to
                               'now'.
      --time-zone=<zoneId>   Time zone ID used to show timestamps.
                             Defaults to system time zone.
  -u, --uri=<nessieUri>      Nessie API endpoint URI, defaults to http://localhost:19120/api/v2.
  -V, --version              Print version information and exit.
      --write-live-set-id-to=<liveSetIdFile>
                             Optional, the file name to persist the created live-set-id to.

```
