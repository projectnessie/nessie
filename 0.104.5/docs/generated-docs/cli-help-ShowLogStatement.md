Shows the Nessie commit log.

By default, the commit log fetched for the current reference of the Nessie CLI, or
in the branch or tab specified using the `IN` clause. By default entities on the latest
commit of the branch or tag will be listed, which can be overridden using the `AT` clause.

The output can be limited using the `LIMIT` clause. It is safe to omit the `LIMIT` clause
for ANSI terminals, because the commit log will be safely paged with neither overloading
the Nessie CLI or Nessie server.
