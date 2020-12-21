Authorization
===========
This document aims at providing some ideas regarding Nessie and authorization, namely:
* what Nessie authorization should generally cover
* a Java interface to perform access control, to be used by the JAX-RS server implementation
* a proposal for an implementation to be bundled with Nessie service


## Authorization scope
It is important to remember that Nessie does not store data directly but only data location and other metadata. As a consequence Nessie authorization layer can only really control access to metadata, but might not prevent data itself to be accessed directly without interacting with Nessie. It is then expected that another system can control access to data itself to make sure no authorized access is made.
The same is true for access to historical data which is one of Nessie main features. For example while it might seem safe when detecting some undesired sensitive data to commit a change to remove it and to restrict access to only the latest version of the dataset, the truth is that the sensitive data may still exist on the datalake and be accessed by other means (similar to how redacting a PDF by adding black boxes on top of sensitive information does not prevent people to read what is written beneath in most cases). The only safe method to remove this data is to remove it from the datalake, and to add commits to Nessie to point to the new data location.

## Stories
Here's a list of common authorization scenarios:

* Alice attempts to execute a query against the table `Foo` on branch `prod`. As she has read access to the table on this branch, Nessie allows the execution engine to get the table details so it can locate the data and try to access it.
* Bob attempts to execute a query against the table `Foo` on branch `prod`. However Bob does not have read access to the table. Nessie returns an authorization error, and the execution engine refuses to execute the query.
* Carol has access to the branch `prod` content, but not to the table `Foo` on this branch. Carol creates a new reference named `carol-branch`with the same hash as `prod`, and attempts to change permission on table `Foo`. However request is denied, and Carol cannot access the content of `Foo`.
* Dave has access to the branch `prod` content, and wants to update the content of the table `Foo`. He creates a new reference named `dave-experiment`, and executes several queries against this branch to modify table `Foo`. Each modification is a commit done against `dave-experiment` branch which is approved by Nessie server. When all the desired modifications are done, Dave attempts to merge the changes back to the `prod` branch. However Dave doesn't have the rights to modify the `prod` branch, causing Nessie to deny the request.

## Access control model
Any object in Nessie can be designated by a pair of coordinates (reference, path). It seems logical that access control is also designed around those two concepts

### Reference access control
References can be designated by name or by hash. However the practicality of using hashes for access control seems very low as those hashes are supposedly random and not known in advance. So focus will be on named references (branches and tags).

Several operations can be exercised against a reference:
* create a new named reference
* assign a hash to a reference
* delete a reference
* list objects present in the tree
* read objects content in the tree
* commit a change against the reference

### Path access control
For a specific reference, an entity is designated by its path which is why a simple way of performing access control can be done by applying restrictions on path.

Several operations can be exercised against a entity:
* create a new entity
* delete an entity
* update entity's content

Note that those operations combine themselves with the reference operations. For example to actually be able to update the content of an entity, user needs both permission to do the update AND to commit the change against the reference where the change will be stored

### Other operations

Some operations like performing garbage collection or changing access control do apply to the whole Nessie repository, and as such are control by their own specific set of permissions.


## Server interface
Although multiple server implementations may exist and conversely multiple access control interface may exist, this section aims to describe an interface to be used by the `JAX-RS` reference implementation of Nessie present under `servers/services`. The interface is composed of various methods for each operation to validate, which accept some access control context providing user identity and some arguments regarding the object to be accessed.

More concretely, interface would look like this:

    interface AccessChecker {
      void canCreateReference(AccessContext context, NamedRef ref) throws AccessControlException;
      void canDeleteReference(AccessContext context, NamedRef ref) throws AccessControlException;
      [...]
      void canReadValue(AccessContext context, Reference ref, ContentsKey key) throws AccessControlException;
    }

The `AccessContext` object passed as argument contains information regarding the overall context of the operation and will be created by the server itself:

    interface AccessContext {
      /**
       * Provide a unique id for the operation being validated (for correlation purposes).
       */
      String operationId();

      /**
       * Provide the user identity.
       */
      Principal user();
      
      [...]
    }

## Server reference implementation

An implementation of the `AccessChecker` interface could be written with the following characteristics:
* Rules are written for either references or paths. Each rule contains a list of operations, and for each operation, a list of users (or group of users) and if the operation is granted or denied
* To designate references or path, the use of wildcard is allowed. Rule ordering is based on how specific the match is. For example, `foo/bar/baz` matches both`foo/*` and `foo/bar/*`, but `foo/bar/*` is more specific, so rules under `foo/bar/*` would take precedence over `foo/*`
* Rules would be stored under a special branch inside Nessie itself (meaning access control over rules would be done in the same manner as regular access control), under two different entities, one for the reference rules and one for the path rules (allowing for example to do an initial reference rule check at the edge).

> Written with [StackEdit](https://stackedit.io/).
