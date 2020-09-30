# Apache Iceberg

[Apache Iceberg](https://iceberg.apache.org/) is a Apache Software Foundation project that provides a rich, relatively new 
table format. It provides:

* Single table ACID transactions
* Scalable metadata
* Appends via file addition
* Updates, deletes and merges via single record operations

## Iceberg Extension Points

Iceberg exposes two primary classes for working with datasets. These are Catalog and 
TableOperations. Nessie implements each. These classes are currently available within 
the [Nessie source code](https://github.com/projectnessie/nessie/tree/main/clients/iceberg/src/main/java/com/dremio/nessie/iceberg) 
but work is being done to contribute them to the Iceberg project so they come pre-packaged
for any user who is working with Iceberg.

## Iceberg Snapshots

Iceberg supports the concept of snapshots. Snapshots are point in time versions of 
a table and are managed as part of each commit operation. Snapshots are limited to 
single table versioning. Nessie versions and commits provide a broader set of snapshot 
capabilities as they support multiple tables. Nessie is happy to 
coexist with Iceberg Snapshots. When working with Nessie, Iceberg snapshots will also 
be versioned along the rest of Iceberg metadata within the Nessie commit model. 

### Automatic Snapshot Import
We are exploring the [creation of a tool](https://github.com/projectnessie/nessie/issues/126) where a 
user can import table snapshots across multiple Iceberg tables into a single Nessie
repository to capture historical data snapshots (interleaved across time). 

## Hive Compatibility


### SerDe Compatibility

There is currently work [in progress](https://github.com/projectnessie/nessie/issues/124) that 
provides updates to the existing HiveCatalog so that it can recognize a Nessie Catalog 
pointer and reroute the metadata lookup to a Nessie server. This allows existing workflows 
to continue to work while also moving versioning responsibilities to Nessie.

### Hive Table Cloning

In Nessie, if HMS is used for read-only operation against Iceberg tables, [work is being 
done](https://github.com/projectnessie/nessie/issues/125) so that Nessie can automatically 
clone the latest metadata file location to the Hive Metastore each time  

### HMS Bridge Compatibility

There is a plan to expose Nessie native tables automatically to Hive users when they 
are interacting through the HMS bridge.
