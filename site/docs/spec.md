# Nessie Spec

## Nessie Branch
A Nessie Branch represents a point in time state of the database (or a portion of it). A branch has a set of tables
associated with it and each of these tables are fixed. The state of a table on one branch may be different from the
state of a table on another branch. A Nessie branch is effectively a `ref` in Git language. As such it is simply a tuple
of a name and an id. The id is a SHA1 hash of a commit which describes the state of the whole known database. Branches
can be ephemeral, permanent or a branch can be unwrapped to only the id: effectively a detached state in a git repo.

## Nessie Table
A Nessie table is at its minimum an id and a metadata location. The id is how a table is referenced in the underlying
system (Iceberg uses name/namespace and Delta Lake uses the full storage path). The metadata location is the `HEAD` of
the table at a specific point in time. A commit is effectively a list of tables with their associated metadata location
and a Table is built in reference to a specific commit as such it has a unique metadata location. A transaction in the
source system is a commit which alters a Table and a commit is always done in the context of a Branch. The Table
object may contain an optional Metadata object which contains a richer set of data.

### Nessie Table Metadata
The table metadata object is specific to the underlying source system and contains a portion of the metadata files
stored by the source system. This object is useful in the context of the UI which is able to display a much richer
set of information about a table and its branches.

## Locking

Locking has to take place at two places:

1. Client to Server
2. Server to Database

In both intstances an optimistic locking model is used. The nature of the git like model means that Table objects as 
well as associated commits and trees are immutable, as such they don't have to be explicitly locked. The Branch
(ref-like) objects do require locking.

### Client to server locking
The client must send an `If-Match` header with requests that produce modifications (Create, Update, Delete). This header
is the commit id which will act as the parent of the change the client is requesting. The commit id is derived from the
Branch id or the `E-Tag` header present on most Nessie responses. Nessie enforces a linear history so a commit will be
rejected if it were to violate that condition. If a commit is rejected the client has to re-attemt the change on a valid
parent commit. The most common violation is when a client tries to commit on a branch that has already been moved
forward by another committer/writer. In this case Nessie will attempt a merge. A merge succeeds if the changes on both
commits are non-conflicting. Non-conflicting changes means the writers changed a non-overlapping set of tables. If both
writers changed the same table then Nessie is not able to resolve the conflict and the second commiter has to try again.
The new `HEAD` commit is returned to the client after a successful commit for use in further writes.

### Server to Database locking
If a server is able to resolve a non-conflicting commit it will create the commit and add it to the database. The server
now has to change the `ref` so that the branch points to this newly created commit. This is done again by optimistic
locking. The server submits a `ref` object to the database with a version attached to it. The database ensures the
version matches before updating the existing record. Successful updates mean the server can return the new commit
to the client with a successful update. However failures mean the Nessie server has to update its current version and
try again. Updating the version likely will introduce a new parent and require re-writing the commit. This may introduce
conflicts which require returning to the client and re-attempting the transaction. 

## Git-like Model

The Nessie backend uses a model inspired by git to negotiate transactions and ensure atomicity. Below is a comparison
of the git terminology to Nessie:

* Git Object: This is a file name & its contents in git and in Nessie it is the table name & metadata location
(optionally the metadata as well). The file name is the hash of the table name with the first two characters of the hash
being the directory and the rest of the hash being the file name. This evenly spreads out the Nessie tables in an
attempt to avoid very large tree re-write operations on commits.
* Git Tree: This is in git a list of filenames and the hash of the associated git object. Nessie is similar: the tree is
the hashed filenames and the git object (Table). In both cases the trees are heirarcical and the base tree associated
to a commit is a list of trees (which are in turn list of Tables). As with Git Objects the Trees are immutable and once
written to the database are not modified.
* Git commit: In both cases the commit is a trees hash and relevant commit info (commiter, time, etc.). Again this
object is immutable.
* Git ref: This is a pointer to a specific commit in both cases. In Nessie a ref is mapped to the Branch object. Nessie
does not support symbolic refs nor does it have anything other than unpeeled refs.

The following list is all Nessie operations and their corresponding git commands:

* create branch: `git checkout -b branchname`. This is done on a base branch (eg `git checkout basebranch` happens first)
alternatively this is created `bare` with no ancestor and therefore empty.
* delete branch `git branch -d branchname`. This is always done regardless of whether `branchname` has been fully
merged.
* create table, update table, delete table, update branch. All variations on `git add/rm <filename> && git commit`.
Nessie deletes tables by setting the `deleted` parameter to true, a create is semantically identical to an update of a
table and update branch encompasses any changes to tables on that branch. Nessie write the objects, trees, commit to the
database and then atomically swaps the ref to point to the new commit.
* merge. `git checkout branch && git merge mergeBranch`. This attempts to write all changes in `mergeBranch` to `branch`
this will fail if `branch` is not a direct ancestor of `mergeBranch`. This is primarily to ensure database consistency.
If you know no conflicts exist or you want to re-write `branch` with whatever `mergeBranch` has then you would `force`
the merge. Forcing the merge would result in loss of history/data.
* cherry-pick. `git cherry-pick <all commits from mergeBranch>`. This applies all commits from `mergeBranch` onto
`baseBranch`. We lose database consistency here but are able to merge sibling commits. This will still fail if the same
table has been changed on both `baseBranch` and `mergeBranch`.


