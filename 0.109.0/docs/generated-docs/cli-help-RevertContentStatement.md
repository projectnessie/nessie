Creates a new commit in Nessie to set the state of the given content-keys to the same state as those content keys
existed on the `TO STATE` reference.

This command is for example useful when another system has created a Nessie commit but the referenced Iceberg
metadata object was not written.

It is recommended to use the `DRY` keyword to investigate the changes before actually committing those to Nessie.

Contents that exist on the `TO STATE` are updated in the new Nessie commit to that state.
Contents that do not exist on the `TO STATE` are deleted only if the `ALLOW DELETES` clause is specified, otherwise
the command will fail.

Note that this command will always create a new commit, even if the actual contents at the "tip" of the target branch
and the `TO STATE` are equal.
