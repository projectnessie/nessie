# Key Concepts


## In Nessie, 

## Branches and Branching
The below diagram is an example of the most common model for branching in Nessie. 

![branching example](/img/branches.png)

As with git, it is expected that all Nessie branches start from `main`. The branching model should always maintain a linear history for `master`. By default Nessie tries to maintain consistency at a database level, as such if two branches from a common ancestor diverge they will conflict and won't be able to be merged without forcing (deleting history) or cherry-picking (explicitly deciding what changes are non-overlapping).

Master is treated as the `gold` source and is the default branch that normal consumers would read from. Reads for a
particular commit (bare branch) are consistent and always produce the same results however reads for a branch are not
necessarily consistent if the branch has changed and the client explicitly updates the branch.

### Typical Workflow
1. create latest and stage branches - At this stage all three branches point to the same commit and reading off each
would produce the same result.
2. commits are performed against `latest` branch. This has no effect on readers for `stage` or `master` branch.
3. commit (T2) is merged to `stage` branch from `latest` branch. This succeeds as they both have a common ancestor.
4. Readers are free to validate from the `stage` branch. In this case an exception is found and the `stage` branch is
not merged into `master`
5. commits on the `latest` branch to fix the exception above. Again these don't affect other readers
6. merge from `latest` to `stage` again and verify. Because of the linear history this merge succeeds
7. Validation succeeds and the readers on `master` can now see the *validated* changes performed on the `latest` branch
8. The `latest` and `stage` branch could be deleted or could live in perpetuity. Deleting is easier as there are fewer
merge conflict potentials in the future. This is similar to the git model.

## Tags


## Nessie's Hierarchical Namespace
