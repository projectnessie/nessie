---
search:
  exclude: true
---
<!--start-->

> `REVERT` `CONTENT` <br>
      \[ `DRY` \] <br>
      `OF` **ContentKey** <br>
      \{ `AND` **ContentKey** \} <br>
      \[ `ON` \[ **ReferenceType** \] **ExistingReference** \] <br>
      `TO` `STATE` <br>
      ( `ON` \[ **ReferenceType** \] **ExistingReference** \[ `AT` \[ `TIMESTAMP` | `COMMIT` \] **TimestampOrCommit** \]<br>
      | `AT` \[ `TIMESTAMP` | `COMMIT` \] **TimestampOrCommit**<br>
      ) <br>
      \[ `ALLOW` `DELETES` \]