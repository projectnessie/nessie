# Overview

Nessie is designed to work with table formats that support a **write-once, immutable asset 
and metadata model**. These types of formats rely on a transaction arbitrator to decide 
the order of operations within a table. Nessie developers have named this operation a "root 
pointer store" (or RPS). This is because these formats all have the same need of 
determining what is the "latest" version of data. This decision needs to be maintained 
via a check-and-set operation about what the current state of a table is.

## Root Pointer Store
Each table format provides at least one RPS facility. Existing RPS models include:

* RPS by convention: E.g. "only one writer is allowed"
* RPS by consistent fileSystem: E.g. one file can be created with a certain name
* RPS by external locking: E.g. calling Hive Metastore lock apis

Nessie formalizes and extends the concept of an RPS. It adds two main types 
of operations: coordination of multiple per-table root pointers and historical versioning 
across changes. This allows users to combine the rich capabilities of existing table 
formats with the Nessie capabilities around versioning and transactions.

## Table Formats

Nessie currently works with the following formats.
 
* [Iceberg Tables](iceberg.md)
* [Delta Lake Tables](deltalake.md)

We expect that Nessie will continue to add table formats as more are created.

## SQL Views

In addition to table formats, Nessie also supports storing SQL views within the Nessie 
repository. This allows tools working in tandem with Nessie to provide very powerful versioned, 
semantic-layering system. See more in our documentation on [SQL Views](views.md).

## Other Object Types

There has been discussion about adding additional types of objects to Nessie for the 
purpose of creating a consistent repository between input assets (jobs, models, etc) 
and output assets. This is something that will evaluated based on demand. There are 
currently three options being considered: more structured object types (such as spark 
job), blob types and support for git sub-modules (where Nessie offers a new object type that 
refers to a particular commit within a git repository). If you have more thoughts on 
this, please provide feedback on the [mailing list](https://groups.google.com/g/projectnessie/).
