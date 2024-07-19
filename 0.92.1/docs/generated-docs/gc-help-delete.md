---
search:
  exclude: true
---
<!--start-->

```
Usage: nessie-gc.jar delete [-hV] [--time-zone=<zoneId>] ([--inmemory] | [[--jdbc] --jdbc-url=<url>
                            [--jdbc-properties[=<String=String>[,<String=String>...]...]]...
                            [--jdbc-user=<user>] [--jdbc-password=<password>]
                            [--jdbc-schema=<schemaCreateStrategy>]]) (-l=<liveSetId> |
                            -L=<liveSetIdFile>)
Delete a live-set, must not be used with the in-memory contents-storage.
  -h, --help                 Show this help message and exit.
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
  -l, --live-set-id=<liveSetId>
                             ID of the live content set.
  -L, --read-live-set-id-from=<liveSetIdFile>
                             The file to read the live-set-id from.
      --time-zone=<zoneId>   Time zone ID used to show timestamps.
                             Defaults to system time zone.
  -V, --version              Print version information and exit.

```
