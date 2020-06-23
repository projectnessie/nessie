## Nessie Root pointer store

Nessie is a root pointer store (RPS) for [Iceberg](https://iceberg.apache.org) and [Delta Lake](https://delta.io). 
A root pointer store is defined as the component of a data system which tells clients what the current state of a 
distributed data store is. A RPS owns table transaction arbitration and is responsible for ensuring atomicity in 
transactionsns. Nessie performs this function as well as offering several novel features including:

* [branch workflow](branches.md)
* cross table transactions
* Notification system (*todo*)
* table management services (*todo*)
* multi-standard support (Iceberg and Delta Lake)


Nessie is built to be lightweight and highly scalable. It can be runs as a serverless function as a docker image or 
as an embedded service and uses a lightweight KV store as a backend.

## Architecture

The Iceberg format (as well as the Delta Lake format) relies on a set of metadata files stored with (or near) the actual
data tables. This allows Iceberg to fulfill the same role as the Hive Metastore for transactions without the need for
expensive metadata scans or centralised planning (see [Iceberg
performance](https://iceberg.incubator.apache.org/performance/)). This includes
things such as partitioning (including hidden partitions), schema migrations, appends and deletes.  It does however
require a pointer to the active metadata set to function. This pointer allows the Iceberg client to acquire and read the
current schema, files and partitions in the dataset. Iceberg currently relies on the Hive metastore or hdfs to perform
this role. The requirements for this root pointer store is it must hold (at least) information about the location of the
current up to date metadata file and it must be able to update this location atomically. In Hive this is accomplished by
locks and in hdfs by using atomic file swap operations. These operations don’t exist in eventually consistent cloud
object stores, necessitating a Hive metastore for cloud data lakes. The Nessie system is designed to store the
root metadata pointer and perform atomic updates to this pointer, obviating the need for a Hive metastore. Removing the
need for a Hive metastore simplifies deployment and broadens the reach of tools that can work with Iceberg tables.
The above is specific to how Iceberg behaves however Delta Lake operates in a near identical way. 

The Nessie service is a lightweight Java based REST API server. It uses a standard REST API optimistic locking strategy
to ensure atomic transactions. This relies on the `ETag` and `If-Match` headers and allows for a very light weight and
scalable implementation. The implementation uses configurable authentication (eg IAM on AWS, JWT elsewhere) and a 
configurable backend (currently supporting DynamoDB on AWS) and uses the optimistic locking features of cloud based
key value stores to ensure scalability across servers. This architecture allows for Nessie to run in a docker container,
as a Lambda function or in a number of other configurations.

