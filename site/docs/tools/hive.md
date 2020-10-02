# Hive

Nessie provides offers an optional component called the HMS Bridge Service. It 
allows you to run a vanilla Hive Metastore server that is enhanced to also support Nessie 
capabilities. This can be used to expose Nessie functionality to Hive or any Hive metastore 
compatible tool (such as [AWS Athena](https://docs.aws.amazon.com/athena/latest/ug/connect-to-data-source-hive.html)).

!!! info
    HMS here stands for Hive Metastore and is not related to the [HMS Queen Elizabeth](https://en.wikipedia.org/wiki/HMS_Queen_Elizabeth_(R08)) or her sisterships.

# How It Works

The HMS Bridge implements a new [RawStore](https://hive.apache.org/javadocs/r3.1.2/api/org/apache/hadoop/hive/metastore/RawStore.html) for the Hive metastore. 
The RawStore is an interface that Hive exposes to provide alternative backing storage 
systems. Typically, people use the default option of [ObjectStore](https://hive.apache.org/javadocs/r3.1.2/api/org/apache/hadoop/hive/metastore/ObjectStore.html) interacting with traditional relational 
databases. The HMS Bridge packages the Nessie REST client and leverages the Nessie 
service to provide versioning capabilities for Hive databases, tables and partitions.

## HMS Bridge Modes

The HMS Bridge can run in two ways: 

**Pure Nessie**
: In this mode, all tables are Nessie based and no backing relational store is required. This is appropriate for new deployments.

**Delegation Mode**
: In this mode the HMS Bridge can enhance your existing Hive Metastore. In this mode, 
your Hive metastore works as it does without the HMS Bridge for all but a set of whitelisted 
databases. When you interact with those whitelisted databases, you'll have Nessie's 
versioning capabilities. This allows people who are already using Hive metastore extensively to continue to do so while 
still taking advantage of Nessie's capabilities for some tables and databases.

## Setup
Currently, the HMS Bridge works with Hive 2.x and Hive 3.x. To get started, do the following steps:

1. Install Hive (2.3.7, 3.1.2, or similar). The easiest way is to download a [tarball 
from Apache](https://hive.apache.org/downloads.html).
1. Download the [Hive 2](https://repo.maven.apache.org/maven2/org/projectnessie/nessie-hms-hive2/{{ versions.java }}/nessie-hms-hive2-{{ versions.java }}.jar) or [Hive 3](https://repo.maven.apache.org/maven2/org/projectnessie/nessie-hms-hive3/{{ versions.java }}/nessie-hms-hive3-{{ versions.java }}.jar) Nessie 
plugin jar and copy into the Hive `lib/` folder:
1. Add the following properties to hive-site.xml:
    
    ```xml
     <property>
      <name>hive.metastore.rawstore.impl</name>
      <description>The specific Nessie store for Hive, see list below.</description>
      <value>org.projectnessie.Hive2NessieRawStore</value>
     </property>
    <property>
      <name>nessie.url</name>
      <description>
        The HTTP endpoint for the Nessie service.
      </description>
      <value>http://localhost:19120/api/v1</value>
    </property>
    <property>
      <name>nessie.dbs</name>
      <description>
        A whitelist of databases that should be managed by Nessie,
        only relevant when using Delegation mode.
        `$nessie` admin database is always whitelisted.
      </description>
      <value>nessiedb,mynessiedb,git4data</value>
    </property>
    ```
    
    |Class|Hive Version|Mode|
    |-|-|-|
    |org.projectnessie.Hive2NessieRawStore|Hive 2|Nessie Only|
    |org.projectnessie.DelegatingHive2NessieRawStore|Hive 2|Nessie + Legacy Tables|
    |org.projectnessie.Hive3NessieRawStore|Hive 3|Nessie Only|
    |org.projectnessie.DelegatingHive3NessieRawStore|Hive 3|Nessie + Legacy Tables|
    
1. Start the metastore with `bin/hive --service metastore`
1. Start working with Hive Metastore using HMS Bridge. If you've chosen delegation 
mode, your existing Hive Metastore tables will continue to exist.


!!! note
    In Hive 3 there is something called Hive Metastore "Standalone" mode. This was designed to 
    try to support Kafka schema registry scenarios. It does not come packaged with sufficient 
    capabilities to work as a Hive Metastore for querying purposes. To use Hive in this context
    you need a complete Hive install (the same as using Hive Metastore without Nessie). 

## Use

### What's Supported
Because Nessie Tables provide a set of new capabilities beyond what Hive has traditionally done, only a subset of Hive features are available with these tables. (If you are running in Nessie + Legacy mode, this only applies to Nessie tables, legacy tables will continue to operate as they do today.) These features currently include:

* CREATE, ALTER and DROP of:
    * databases
    * external tables
    * views
* Inserting data into an empty table
* ADD and DROP for partitions
* Altering table properties

Note: When using external tables with HMS Bridge, the tables must be created with the "immutable=true" property. Nessie reject tables that are not configured with this property. This property does not mean the tables are immutable. What it does is disable Hive from mutating a tables state without informing the Hive metastore. This guarantees that Hive versions are maintained correctly.

### Query Semantics
By default, Hive will work with the default branch configured in Nessie. You can do normal operations against your Hive metastore and Nessie will behave just like a traditional metastore. However, under the covers, Nessie is versioning all changes to your Hive metastore. This allows to do more fancy things as well.

Because Hive does not natively support Branching and Tags, Nessie presents a pseudo-database called '$nessie' which can be used  within a HiveQL context to:
* list available branches & tags
* change the default ref for your session
* create tags and branches

#### List available branches and tags

You can list available named references within Nessie by listing the tables in the $nessie database.

```
hive> SHOW TABLES IN `$nessie` 
OK
main
dev
...
```

#### Create New Branch or Tag
To create a branch or tag, you will use CREATE VIEW syntax along with a dummy SQL query. The name of the view will be the new named reference. You can optionally provide `type` and `ref` parameters to further define your new object.

||property||description||
|`type`|Defaults to `branch`. Whether to create a named reference of type `tag` or `branch`|
|`ref`|Defaults to the head of the current default branch (typically `main`). Can be set to either a hash, branch or tag. If set to a tag or branch, resolves the named reference to a hash for creation.|

Example uses:

```
# create a new branch called my_new_branch pointing to the HEAD of the current ref context.
hive> CREATE VIEW `$nessie`.my_new_branch AS SELECT 1;
OK
# create a new tag called my_new_tag pointing to the current tip of the default branch.
hive> CREATE VIEW `$nessie`.my_new_tag TBLPROPERTIES("type"="tag") AS SELECT 1;
OK
# create a new tag called my_new_specific_tag that points to the reference "abcd..."
hive> CREATE VIEW `$nessie`.my_new_specific_tag TBLPROPERTIES("type"="tag", "ref"="abcd...") AS SELECT 1;
OK
```

Note: The `SELECT 1` in the queries above is arbitrary and only serves to allow Hive to understand the HiveQL operation and then delegate it to Nessie. Any valid SQL can be provided there and will be thrown away, not stored. If you are running in Hive 2, it doesn't support creating views of queries without a FROM clause. In those cases, you'll need to use some other dummy statement such as `SELECT * from <known table> LIMIT 1`.

#### Set Your Context
To change the context of the environment your session is working within, you will alter the `$nessie` database. You can set the default context to any valid Nessie reference. For example:

```
hive> ALTER DATABASE `$nessie` SET DBPROPERTIES ("ref"="my_special_branch")
OK
```
Once you change context, all read operations will default to that context for the life 
of that session. All write operations will also be done within that context.

#### Querying across multiple contexts
You can use absolute references to query across context. You do this by appending `@<refname>` to the table you want to query. For example, if you want to see all records in a table from yesterday and now, you might write a query such as:

```
hive> SELECT * from t1 UNION ALL `t1@yesterday`
OK
```

This can be used to directly access any available reference within Nessie

```
hive> SELECT * from `t1@prod_data_branch` UNION ALL `t1@my_special_tag` UNION ALL `t1@2019-12-25`
OK
```

!!! note
    You can only use absolute references (t1@<ref>) for SELECT queries. For mutation operations 
    (such as CREATE/ALTER/DROP), you must first assign the context that you want to 
    do the operation within. IOW, absolute references are read-only.

### Not Yet Supported

Because Nessie has a special way of representing the data, the following types of objects and 
operations are not currently supported:

* Grants, role, privileges
* Statistics
* Functions
* Tokens
* Notifications
* Constraints and referential integrity (primary/foreign keys)

### File Management
When using Nessie with Hive, you must rely on Nessie's garbage collection system in order 
to clean up files that are no longer referenced. Because there may be historical versions 
of tables pointing to specific files, using an external process to delete those files when 
not instructed/managed by Nessie will result in historical queries returning partial datasets. 
Because of this, the following tables types are unlikely to ever be supported by Nessie's 
versioning management system:

* Internal tables (Hive doesn't import metastore about changes)
* External tables without the "immutable=true" property. 

Additionally, when creating a new external table, Nessie will always prepend the table's 
location with a randomly generated Guid. This ensures the following set of operations 
still maintain history and work as expectd:

```
hive> CREATE EXTERNAL TABLE t1 (a int, b int) PARTITIONED BY (c int) TBLPROPERTIES ("immutable"="true");
OK
hive> INSERT TABLE t1 PARTITION(c) SELECT 1 AS a, 1 AS b,1 AS c UNION ALL SELECT 2,2,2 UNION ALL SELECT 3,3,3;
...
OK
hive> CREATE VIEW `$nessie`.original AS SELECT 1;
OK
hive> DROP TABLE t1;
OK
hive> CREATE EXTERNAL TABLE t1 (a int, b int) PARTITIONED BY (c int) TBLPROPERTIES ("immutable"="true");
OK
hive> INSERT TABLE t1 PARTITION(c) SELECT 10 AS a, 10 AS b, 1 AS c;
...
OK
```

In this situation, Nessie's versioning is in full effect. If I query `t1`, I will see 
the record with a=10. If I query `t1@original`, I will see the records with a=[1,2,3]. Without Nessie's 
automatic prefixing, default Hive behavior would actually lead to an exception when 
the second insertion is done since it would find a file already existing on the path 
of the default external table. 
