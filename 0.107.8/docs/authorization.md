---
title: "Authorization (Access Control)"
---

# Authorization (Access Control)

## Authorization scope
It is important to note that Nessie does not store data directly but only data location and other metadata. 

As a consequence, the Nessie authorization layer can only really control access to **metadata**, but might not prevent data itself to be accessed directly without interacting with Nessie. 
It is then expected that another system can control access to data itself to make sure unauthorized access isn't possible.

The same is true for access to historical data, which is one of Nessie's main features.
For example, while it might seem safe committing a change that removes undesired sensitive data and restricting access to only the latest version of the dataset,
the truth is that the sensitive data may still exist on the data lake and be accessed by other means 
(similar to how redacting a PDF by adding black boxes on top of sensitive information does not prevent people to read what is written beneath in most cases). 
The only safe way to remove this data is to remove it from the table (e.g. via `DELETE` statements) and then run the [Garbage Collection](https://projectnessie.org/features/management/#garbage-collection) algorithm to ensure the data has been removed from Nessie history and deleted on the data lake.

## Stories
Here's a list of common authorization scenarios:

* Alice attempts to execute a query against the table `Foo` on branch `prod`. As she has read access to the table on this branch, Nessie allows the execution engine to get the table details.
* Bob attempts to execute a query against the table `Foo` on branch `prod`. However, Bob does not have read access to the table. Nessie returns an authorization error, and the execution engine refuses to execute the query.
* Carol has access to the content on branch `prod`, but not to the table `Foo` on this branch. Carol creates a new reference named `carol-branch` with the same hash as `prod`, and attempts to change permissions on table `Foo`. However, request is denied and Carol cannot access the content of `Foo`.
* Dave has access to the content on branch `prod`, and wants to update the content of the table `Foo`. He creates a new reference named `dave-experiment`, and executes several queries against this branch to modify table `Foo`. Each modification is a commit done against `dave-experiment` branch which is approved by the Nessie server. When all the desired modifications are done, Dave attempts to merge the changes back to the `prod` branch. However, Dave doesn't have the rights to modify the `prod` branch, causing Nessie to deny the request.

## Access control model
Any object in Nessie can be designated by a pair of coordinates (**reference**, **path**), therefore access control is also designed around those two concepts.

### Access control against references
References can be designated by their name (branches and tags) and there are several operations that can be exercised:

* view/list available references
* create a new named reference
* assign a hash to a reference
* delete a reference
* list objects present in the tree
* read objects content in the tree
* commit a change against the reference

Note that a user needs to be able to view a reference in order to list objects on that reference.

### Access control against paths
For a specific reference, an **entity** is designated by its path which is why a simple way of performing access control can be done by applying restrictions on path.

Several operations can be exercised against an **entity**:

 * create a new entity
 * delete an entity
 * update entity's content

Note that those operations combine themselves with the reference operations. For example to actually be able to update the content of an entity, user needs both permission to do the update AND to commit the change against the reference where the change will be stored

## Service Provider Interface
The SPI is named [AccessChecker](https://github.com/projectnessie/nessie/blob/main/servers/services/src/main/java/org/projectnessie/services/authz/AccessChecker.java) and uses [AccessContext](https://github.com/projectnessie/nessie/blob/main/servers/services/src/main/java/org/projectnessie/services/authz/AccessContext.java), which carries information about the overall context of the operation.
Implementers of `AccessChecker` are completely free to define their own way of creating/updating/checking authorization rules.

### ContentId Usage
Note that there is a `contentId` parameter in some methods of the [AccessChecker](https://github.com/projectnessie/nessie/blob/main/servers/services/src/main/java/org/projectnessie/services/authz/AccessChecker.java), which allows checking specific rules for a given entity at a given point in time.
The `contentId` parameter refers to the ID of a `Content` object and its contract is defined [here](https://projectnessie.org/develop/spec/#content-id).

One can think of this similar to how permissions are defined in Google Docs. There are some permissions that are specific to the parent folder and to the doc itself. When a Doc is moved from one folder to another, it inherits the permissions of the parent folder.
However, the doc-specific permissions are carried over with the doc and still apply.
The same is true in the context of entities. There are some rules that apply to an entity in a global fashion, and then there's the possibility to define rules specific to the `contentId` of an entity.

## Reference implementation for Metadata Authorization

The reference implementation allows defining authorization rules via [application.properties](https://github.com/projectnessie/nessie/blob/main/servers/quarkus-server/src/main/resources/application.properties) and is therefore dependent on [Quarkus](https://quarkus.io/).
Nessie's metadata authorization can be enabled via `nessie.server.authorization.enabled=true`.

### Authorization Rules
Authorization rule definitions are using a **Common Expression Language (CEL)** expression (an intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md).

Rule definitions are of the form `nessie.server.authorization.rules.<ruleId>=<rule_expression>`, where `<ruleId>` is a unique identifier for the rule.

`<rule_expression>` is basically a CEL expression string, which allows lots of flexibility on a given set of variables. 

#### Variables

Certain variables are available within the `<rule_expression>` depending on context:

* **op** - refers to the type name (string) of the operation that is being authorized.
  See [BatchAccessChecker](https://github.com/projectnessie/nessie/blob/main/servers/services/src/main/java/org/projectnessie/services/authz/BatchAccessChecker.java)
  and [Check](https://github.com/projectnessie/nessie/blob/main/servers/services/src/main/java/org/projectnessie/services/authz/Check.java) types for details.
* **role** - refers to the user's primary role and can be any string.
* **roles** - refers to the list of all known user roles.
* **ref** - refers to a string representing a branch/tag name or `DETATCHED` for direct access to a commit id.
* **path** - refers to the URI path representation (`ContentKey.toPathString()`) of the [content key](https://github.com/projectnessie/nessie/blob/main/api/model/src/main/java/org/projectnessie/model/ContentKey.java) for the object related to the authorization check.
* **contentType** - refers to a (possibly empty) string representing the name of the object's [`Content.Type`](https://github.com/projectnessie/nessie/blob/main/api/model/src/main/java/org/projectnessie/model/Content.java).
* **type** - refers to the repository config type to be retrieved or updated.
* **api** - contains information about the receiving API. This is a composite object with two properties:
  * **apiName** the name of the API, can be `Nessie` or `Iceberg`
  * **apiVersion** the version of the API - for `Nessie` it can be 1 or 2, for `Iceberg` currently 1
* **actions** a list of actions (strings), available for some Iceberg endpoints.

#### Actions

The list of `actions` (strings) is available for some Iceberg endpoints that perform changes against
an entity (table, view, namespace). The list of actions is empty for the Nessie REST API.

**Catalog operations**

Available for all updating Iceberg endpoints
for the `Check` types `CREATE_ENTITY`, `UPDATE_ENTITY` and `DELETE_ENTITY`.

* `CATALOG_CREATE_ENTITY` - create a table/view/namespace
* `CATALOG_UPDATE_ENTITY` - update a table/view/namespace
* `CATALOG_DROP_ENTITY` - dropping a table/view/namespace
* `CATALOG_RENAME_ENTITY_FROM` - renaming a table (from)
* `CATALOG_RENAME_ENTITY_TO` - renaming a table (to)
* `CATALOG_REGISTER_ENTITY` - registering a table (from)
* `CATALOG_UPDATE_MULTIPLE` - update multiple tables
* `CATALOG_S3_SIGN` - S3 request signing

**Iceberg metadata updates**

Available for Iceberg endpoints that update entities, represents the kinds of metadata updates,
for the `Check` types `CREATE_ENTITY`, `UPDATE_ENTITY`.

* `META_ADD_VIEW_VERSION`
* `META_SET_CURRENT_VIEW_VERSION`
* `META_SET_STATISTICS`
* `META_REMOVE_STATISTICS`
* `META_SET_PARTITION_STATISTICS`
* `META_REMOVE_PARTITION_STATISTICS`
* `META_ASSIGN_UUID`
* `META_ADD_SCHEMA`
* `META_SET_CURRENT_SCHEMA`
* `META_ADD_PARTITION_SPEC`
* `META_SET_DEFAULT_PARTITION_SPEC`
* `META_ADD_SNAPSHOT`
* `META_ADD_SORT_ORDER`
* `META_SET_DEFAULT_SORT_ORDER`
* `META_SET_LOCATION`
* `META_SET_PROPERTIES`
* `META_REMOVE_PROPERTIES`
* `META_REMOVE_LOCATION_PROPERTY`
* `META_SET_SNAPSHOT_REF`
* `META_REMOVE_SNAPSHOT_REF`
* `META_UPGRADE_FORMAT_VERSION`

**from Iceberg's snapshot summary**

Available for Iceberg updates that add a snapshot, for the `Check` types `CREATE_ENTITY`, `UPDATE_ENTITY`.

* `SNAP_ADD_DATA_FILES`
* `SNAP_DELETE_DATA_FILES`
* `SNAP_ADD_DELETE_FILES`
* `SNAP_ADD_EQUALITY_DELETE_FILES`
* `SNAP_ADD_POSITION_DELETE_FILES`
* `SNAP_REMOVE_DELETE_FILES`
* `SNAP_REMOVE_EQUALITY_DELETE_FILES`
* `SNAP_REMOVE_POSITION_DELETE_FILES`
* `SNAP_ADDED_RECORDS`
* `SNAP_DELETED_RECORDS`
* `SNAP_ADDED_POSITION_DELETES`
* `SNAP_DELETED_POSITION_DELETES`
* `SNAP_ADDED_EQUALITY_DELETES`
* `SNAP_DELETED_EQUALITY_DELETES`
* `SNAP_REPLACE_PARTITIONS`
* `SNAP_OP_APPEND`
* `SNAP_OP_REPLACE`
* `SNAP_OP_OVERWRITE`
* `SNAP_OP_DELETE`

#### Checks for Reference operations

Applicable `op` types:

* `VIEW_REFERENCE`
* `CREATE_REFERENCE`
* `DELETE_REFERENCE`
* `ASSIGN_REFERENCE_TO_HASH`
* `DELETE_REFERENCE`
* `READ_ENTRIES`
* `LIST_COMMIT_LOG`
* `COMMIT_CHANGE_AGAINST_REFERENCE`

Available variables:

* `role`
* `roles`
* `ref`
* `api`

#### Checks for Content operations

Applicable `op` types:

* `READ_CONTENT_KEY`
* `READ_ENTITY_VALUE`
* `CREATE_ENTITY`
* `UPDATE_ENTITY`
* `DELETE_ENTITY`
  
Available variables:

* `role`
* `roles`
* `ref`
* `path`
* `contentType`
* `api`
* `actions` (for `CREATE_ENTITY`, `UPDATE_ENTITY`, `DELETE_ENTITY` against Iceberg REST)

#### Checks for Repository Config operations

Applicable `op` types:

* `READ_REPOSITORY_CONFIG`
* `UPDATE_REPOSITORY_CONFIG`
 
Available variables:

* `role`
* `roles`
* `type`
* `api`

#### Relevant CEL features

Since all available authorization rule variables are strings, the relevant CEL-specific things that are worth mentioning are shown below:

* [equality and inequality](https://github.com/google/cel-spec/blob/master/doc/langdef.md#equality-and-ordering)
* [regular expressions](https://github.com/google/cel-spec/blob/master/doc/langdef.md#regular-expressions)
* [operators & functions](https://github.com/google/cel-spec/blob/master/doc/langdef.md#list-of-standard-definitions)

### Example authorization rules

Below are some basic examples that show how to give a permission for a particular operation. In reality, one would want to keep the number of authorization rules for a single user/role low and grant permissions for all required operations through as few rules as possible.

* allows viewing the branch/tag starting with the name `allowedBranch` for the role that starts with the name `test_`:
```
nessie.server.authorization.rules.allow_branch_listing=\
  op=='VIEW_REFERENCE' && role.startsWith('test_') && ref.startsWith('allowedBranch')
```

* allows creating branches/tags that match the regex `.*allowedBranch.*` for the role `test_user`:
```
nessie.server.authorization.rules.allow_branch_creation=\
  op=='CREATE_REFERENCE' && role=='test_user' && ref.matches('.*allowedBranch.*')
```

* allows deleting branches/tags that end with `allowedBranch` for the role named `test_user123`:
```
nessie.server.authorization.rules.allow_branch_deletion=\
  op in ['VIEW_REFERENCE', 'DELETE_REFERENCE'] && role=='test_user123' && ref.endsWith('allowedBranch')
```

* allows listing the commit log for all branches/tags starting with `dev`:
```
nessie.server.authorization.rules.allow_listing_commitlog=\
  op in ['VIEW_REFERENCE', 'LIST_COMMIT_LOG'] && ref.startsWith('dev')
```

* allows reading the entity value where teh `path` starts with `allowed.` for the role `test_user`:
```
nessie.server.authorization.rules.allow_reading_entity_value=\
  op in ['VIEW_REFERENCE', 'READ_ENTITY_VALUE'] && role=='test_user' && path.startsWith('allowed.')
```

* allows deleting the entity where the `path` starts with `dev.` for all roles:
```
nessie.server.authorization.rules.allow_deleting_entity=\
  op in ['VIEW_REFERENCE', 'DELETE_ENTITY'] && path.startsWith('dev.')
```

* allows listing reflog for the role `admin_user`:
```
nessie.server.authorization.rules.allow_listing_reflog=\
  op=='VIEW_REFLOG' && role=='admin_user'
```

### Example authorization rules from Stories section

As mentioned in the Stories section, a few common scenarios that are possible are:

* Alice attempts to execute a query against the table `Foo` on branch `prod`. As she has read access to the table on this branch, Nessie allows the execution engine to get the table details.
* Bob attempts to execute a query against the table `Foo` on branch `prod`. However, Bob does not have read access to the table. Nessie returns an authorization error, and the execution engine refuses to execute the query.
* Carol has access to the content on branch `prod`, but not to the table `Foo` on this branch. Carol creates a new reference named `carol-branch` with the same hash as `prod`, and attempts to change permissions on table `Foo`. However, request is denied and Carol cannot access the content of `Foo`.
* Dave has access to the content on branch `prod`, and wants to update the content of the table `Foo`. He creates a new reference named `dave-experiment`, and executes several queries against this branch to modify table `Foo`. Each modification is a commit done against `dave-experiment` branch which is approved by the Nessie server. When all the desired modifications are done, Dave attempts to merge the changes back to the `prod` branch. However, Dave doesn't have the rights to modify the `prod` branch, causing Nessie to deny the request.

Below are the respective authorization rules for these scenarios:
```
# read access for all on the prod branch
nessie.server.authorization.rules.prod=\
  op in ['VIEW_REFERENCE'] && ref=='prod' && role in ['Alice', 'Bob', 'Carol', 'Dave']

# alice & dave can read Foo  
nessie.server.authorization.rules.reading_foo_on_prod=\
  op in ['READ_ENTITY_VALUE'] && ref=='prod' && path=='Foo' && role in ['Alice', 'Dave']

# specific rules for carol on her branch
nessie.server.authorization.rules.carol-branch=\
  op in ['VIEW_REFERENCE', 'CREATE_REFERENCE', 'DELETE_REFERENCE', 'COMMIT_CHANGE_AGAINST_REFERENCE'] && ref=='carol-branch' && role=='Carol'

# specific rules for dave on his branch
nessie.server.authorization.rules.dave-experiment=\
  op in ['VIEW_REFERENCE', 'CREATE_REFERENCE', 'DELETE_REFERENCE', 'COMMIT_CHANGE_AGAINST_REFERENCE'] && ref=='dave-experiment' && role=='Dave'

# bob can read/update/delete BobsBar only
nessie.server.authorization.rules.bob=\
  op in ['READ_ENTITY_VALUE', 'UPDATE_ENTITY', 'DELETE_ENTITY'] && path=='BobsBar` && role=='Bob')

# carol can read/update/delete CarolsSecret
nessie.server.authorization.rules.carol=\
  op in ['READ_ENTITY_VALUE', 'UPDATE_ENTITY', 'DELETE_ENTITY'] && path=='CarolsSecret` && role=='Alice')

# dave can read/update/delete DavesHiddenX
nessie.server.authorization.rules.dave=\
  op in ['READ_ENTITY_VALUE', 'UPDATE_ENTITY', 'DELETE_ENTITY'] && path=='DavesHiddenX` && role=='Dave')

```
