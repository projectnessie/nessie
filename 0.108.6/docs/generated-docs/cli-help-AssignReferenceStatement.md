Assigns a Nessie branch or tag using the name specified using the `ReferenceName` parameter to
another commit. The reference type is specified using the `ReferenceType` parameter.

By default, the branch or tag is updated to the latest commit on the current reference of the
Nessie CLI (see `USE` statement). Another target reference name can use specified using the
`TO` clause. The optional `AT` clause allows specifying a different commit ID (hash) to assign
the reference to.
