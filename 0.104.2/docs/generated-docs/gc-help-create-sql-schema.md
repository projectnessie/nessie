---
search:
  exclude: true
---
<!--start-->

```
Usage: nessie-gc.jar create-sql-schema [-hV] ([--jdbc] --jdbc-url=<url> [--jdbc-properties
                                       [=<String=String>[,<String=String>...]...]]...
                                       [--jdbc-user=<user>] [--jdbc-password=<password>]
                                       [--jdbc-fetch-size[=<fetchSize>]]
                                       [--jdbc-schema=<schemaCreateStrategy>])
JDBC schema creation.
  -h, --help               Show this help message and exit.
      --jdbc               Flag whether to use the JDBC contents storage.
      --jdbc-fetch-size[=<fetchSize>]
                           JDBC fetch size, defaults to 100.
      --jdbc-password=<password>
                           JDBC password used to authenticate the database access.
      --jdbc-properties[=<String=String>[,<String=String>...]...]
                           JDBC parameters.
      --jdbc-schema=<schemaCreateStrategy>
                           How to create the database schema. Possible values: CREATE,
                             DROP_AND_CREATE, CREATE_IF_NOT_EXISTS.
      --jdbc-url=<url>     JDBC URL of the database to connect to.
      --jdbc-user=<user>   JDBC user name used to authenticate the database access.
  -V, --version            Print version information and exit.

```
