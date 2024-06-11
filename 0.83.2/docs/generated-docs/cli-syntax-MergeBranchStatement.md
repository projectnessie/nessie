> `MERGE`
  \[ `DRY` \]
  \[ **ReferenceType** \]
  **ExistingReference**
  \[ `AT`
      \[
              `TIMESTAMP` | `COMMIT`
          \]
      **TimestampOrCommit** \]
  \[ `INTO` **ExistingReference** \]
  \[ `BEHAVIOR` **MergeBehaviorKind** \]
  \[ `BEHAVIORS` **ContentKey** `=` **MergeBehaviorKind**
      \{ `AND` **ContentKey** `=` **MergeBehaviorKind** \}
      \]
  
