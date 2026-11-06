Creates a new Nessie branch or tag using the name specified using the `ReferenceName` parameter.
The reference type is specified using the `ReferenceType` parameter.

By default, the new branch or tag is created from the latest commit on the current reference of
the Nessie CLI (see `USE` statement). Another source reference name can use specified using the
`FROM` clause. The optional `AT` clause allows specifying a different commit ID (hash) to create
the new reference from.

This command will fail, if a references with the name `ReferenceName` already exists, unless the
optional `IF NOT EXISTS` is specified.
