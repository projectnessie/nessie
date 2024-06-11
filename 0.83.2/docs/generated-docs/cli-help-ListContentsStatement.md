Lists all tables, views and namespaces either in the current reference of the Nessie CLI, or
in the branch or tab specified using the `IN` clause. By default entities on the latest
commit of the branch or tag will be listed, which can be overridden using the `AT` clause.

An optional CEL-filter can be specified, which is evaluated on the server side.

The optional `STARTING WITH` clause starts the output at the content-key with the given value.

The optional `CONTAINING` clause only outputs entities with a content-key that contain the
given value.
