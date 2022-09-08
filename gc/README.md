# Nessie GC

See [here](../site/docs/features/gc-internals.md).


```shell
docker run --rm -e POSTGRES_USER=pguser -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_DB=nessie_gc -p 5432:5432 postgres:14
gc/gc-tool/build/executable/nessie-gc show-sql-create-schema-script --jdbc-url jdbc:postgresql://127.0.0.1:5432/nessie_gc --jdbc-user pguser --jdbc-password mysecretpassword
```
