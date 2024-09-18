Merges a branch or tag into another branch, supporting manual conflict resolution.

The optional `DRY` keyword defines that Nessie shall simulate a merge operation. This is useful to
check whether a merge operation would succeed.

Specifying the name of the "from" reference is mandatory. By default, the latest commit of the "from"
branch or tag will be merged, which can be overridden using the `AT` clause.

By default, `MERGE` uses the CLI's current reference as the target branch. The `INTO` clause can
be used to specify another target branch.

Nessie merge operations currently support three different merge behaviors:

* `NORMAL`: a merge succeeds, if the content does not have a conflicting change in the target branch.
* `FORCE`: a merge always succeeds, the content from the "from" reference will be applied onto the target branch.
* `DROP`: like `NORMAL`, but does not cause a conflict, so does not fail the whole merge operation.

The merge behavior for all contents defaults to `NORMAL` and can be changed using the `BEHAVIOR` clause.

Specific merge behaviors can be specified using the `BEHAVIORS` clause for individual content keys.
