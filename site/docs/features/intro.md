# Introduction

Nessie is an OSS service and libraries that enable you to maintain multiple versions 
of your data and leverage Git-like Branches & Tags for your Data Lake. Nessie enhances the following 
table formats with version control techniques:

* Apache Iceberg Tables ([more](../tables/iceberg.md))
* Delta Lake Tables ([more](../tables/deltalake.md))
* Hive Metastore Tables ([more](../tables/hive.md))
* SQL Views ([more](../tables/views.md))

## Basic Concepts

Nessie is heavily inspired by Git. The main concepts Nessie exposes map directly to 
[Git concepts](https://git-scm.com/book/en/v2). In most cases, you simply need to replace 
references of files and directories in Git with Tables in Nessie. The primary concepts in Nessie are:
 
* Commit: Consistent snapshot of all tables at a particular point in time.
* Branch: Human-friendly reference that a user can add commits to.
* Tag: Human-friendly reference that points to a particular commit.
* Hash: Hexadecimal string representation of a particular commit.

Out of the box, Nessie starts with a single branch called `main` that points to the 
beginning of time. A user can immediately start adding tables to that branch. For example 
(in pseudo code):

```
$ create t1
...
$ insert 2 records into t1
...
$ create t2
...
$ insert 2 records into t2
...
```

A user can then use the Nessie CLI to view the history of the main branch. You'll see 
that each operation was automatically recorded as a commit within Nessie:

```
$ nessie log
hash4    t2 data added 
hash3    t2 created
hash2    t1 data added
hash1    t1 created
```

A user can then create a new tag referencing this point in time. After doing 
so, a user can continue changing the tables but that point in time snapshot will 
maintain that version of data.

```
$ nessie tag mytag hash4

$ insert records into t1

$ select count(*) from t1 join t2
.. record 1 ..
.. record 2 ..
.. record 3 ..
.. 3 records ..

$ select count(*) from t1@mytag join t2@mytag
.. record 1 ..
.. record 2 ..
.. only 2 records ..
```

## Data and Metadata

Nessie does not make copies of your underlying data. Instead, it works to version 
separate lists of files associated with your dataset. Whether using Spark, Hive or 
some other tool, each mutation operation you do will add or delete one or more files from 
the definition of your table. Nessie keeps tracks of which files are related to each 
of your tables at every point in time and then allows you to recall those as needed.

## Scale & Performance

Nessie is built for very large data warehouses. Nessie [supports](../develop/kernel.md) 
millions of tables and thousands of commits/second. Because Nessie builds on top of Iceberg 
and Delta Lake, each table can have millions of files. As such, Nessie can support 
data warehouses several magnitudes larger than the largest in the world today. This 
is possible in large part due to the separation of transaction management (Nessie) from 
table metadata management (Iceberg and Delta Lake).

## Technology 
Nessie can be [deployed in multiple ways](../try) and is composed primarily of the Nessie service, 
which exposes a set of [REST APIs](../develop/rest.md) and a simple browser UI. This service works with multiple
libraries to expose Nessie's version control capabilities to common data management technologies.

Nessie was built as a Cloud native technology and is designed to be highly scalable, 
[performant](../develop/kernel.md) and resilient. Built 
on Java and leveraging [Quarkus](https://quarkus.io/), it is compiled to a GraalVM native image 
that starts in less than 20ms. This makes Nessie work very well in Docker and FaaS environments. 
Nessie has a pluggable storage backend and comes pre-packaged with support for DynamoDB and local 
storage.

## License and Governance
Nessie is Apache-Licensed and built in an open source, consensus-driven GitHub community. 
Nessie was originally conceived and built by engineers at [Dremio](http://dremio.com).

## Getting Started

* Read more about [Nessie transactions](transactions.md)
* Get started with the Nessie [quickstart](../try).
