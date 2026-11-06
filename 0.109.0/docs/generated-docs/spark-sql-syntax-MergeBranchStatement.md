---
search:
  exclude: true
---
<!--start-->

> `MERGE` <br>
      \[ `DRY` \] <br>
      \[ **ReferenceType** \] <br>
      **ExistingReference** <br>
      \[ `AT` \[ `TIMESTAMP` | `COMMIT` \] **TimestampOrCommit** \] <br>
      \[ `INTO` **ExistingReference** \] <br>
      \[ `BEHAVIOR` **MergeBehaviorKind** \] <br>
      \[ `BEHAVIORS` **ContentKey** `=` **MergeBehaviorKind** \{ `AND` **ContentKey** `=` **MergeBehaviorKind** \} \] <br>
      \[ `IN` **CatalogName** \]