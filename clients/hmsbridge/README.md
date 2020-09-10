
# HMS Bridge

The HMS Bridge is a new [RawStore](https://hive.apache.org/javadocs/r3.1.2/api/index.html) for the Hive metastore that can be used to expose Nessie functionality to Hive or any HMS compatible tool (such as [AWS Athena](https://docs.aws.amazon.com/athena/latest/ug/connect-to-data-source-hive.html)). It provides a HMS facade on top of the core Nessie service that serves as a bridge between legacy systems and modern data version control capabilities.

The HMS Bridge can run in two ways: 

* **Nessie Tables**: In this mode, all tables are Nessie based and no backing relational store is required. This is appropriate for new deployments.
* **Nessie Tables and Legacy Tables**: In this mode, only whitelisted databases will be handled by Nessie. Other databases will still be handled by traditional Hive metastore mechanisms. This allows people who are already using Hive metastore extensively to continue to do so while still taking advantage of Nessie's capabilities for some tables and databases.


## Setup
Currently, the HMS Bridge works with Hive 2.x and Hive 3.x. To get started, do the following steps:

1. Install Hive (2.3.7, 3.1.2, or similar). Easiest way is to download tarball and unzip
1. Build HMS Bridge using mvn install
1. Copy generated jar file from first step into Hive lib folder
  * Hive 2:  `cp clients/hmsbridge/hive2/nessie-hms-hive2-1.0-SNAPSHOT.jar lib`    
  * Hive 3:  `cp clients/hmsbridge/hive3/nessie-hms-hive3-1.0-SNAPSHOT.jar lib`    
1. Add the following properties to hive-site.xml or similar:

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
	      only relevant when using delegating Nessie + Legacy Table mode.
	      `$nessie` admin database is always whitelisted.
	    </description>
	    <value>nessiedb,mynessiedb,git4data</value>
	  </property>
	  
	</pre>
	```
	
	||Class||Hive Version||Mode||
	|org.projectnessie.Hive2NessieRawStore|Hive 2|Nessie Only|
	|org.projectnessie.DelegatingHive2NessieRawStore|Hive 2|Nessie + Legacy Tables|
	|org.projectnessie.Hive3NessieRawStore|Hive 3|Nessie Only|
	|org.projectnessie.DelegatingHive3NessieRawStore|Hive 3|Nessie + Legacy Tables|
	
1. Start the metastore with `bin/hive --service metastore`
1. Start working with Hive Metastore using HMS Bridge


## Use

### What's Supported
Because Nessie Tables provide a set of new capabilities beyond what Hive has traditionally done, only a subset of Hive features are available with these tables. (If you are running in Nessie + Legacy mode, this only applies to Nessie tables, legacy tables will continue to operate as they do today.) These features currently include:

* CREATE/ALTER/DROP of:
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
# create a new branch called my_new_branch pointing to the current tip of the default branch.
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
Once you change context, all read operations will default to that context. All write operations will also be within that context.

#### Querying across multiple contexts
You can use absolute references to query across context. You do this by appending `@<refname>` to the table you want to query. For example, if you want to see all records in a table from yesterday and now, you might write a query such as:

```
hive> SELECT * from t1 UNION ALL `t1@my_branch`
OK
hive> SELECT * from t1 UNION ALL `t1@my_special_tag` UNION ALL `t1@2019-12-25`
OK
...
```

Note: you can only use absolute references for SELECT queries. For mutation operations (such as CREATE/ALTER/DROP), you must first assign the context that you want to do the operation within. Absolute references are read-only.

### Not Yet Supported
* Grants, role, privleges
* Stats
* Functions
* Tokens
* Notifications
* Contraints and referenential integrity (primary/foreign keys)


### File Management
When using Nessie with Hive, you must rely on Nessie's garbage collection system in order to clean up files that are no longer referenced. Because there may be historical versions of tables pointing to specific files, using an external process to delete those files when not instructed/managed by Nessie will result in historical queries returning partial datasets. Because of this, the following tables types are unlikely to ever be supported by Nessie's versioning management system:

* Internal tables (Hive doesn't import metastore about changes)
* External tables without the "immutable=true" property. 
