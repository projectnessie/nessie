# Architecture

Nessie builds on the recent ecosystem developments around table formats. The rise of 
very large metadata and eventually consistent cloud data lakes (S3 specifically) drove 
the need for an updated model around metadata managmeent. Where consistent directory 
listings in HDFS used to be sufficient, there were many features lacking. This includes 
snapshotting, consistency and fast planning. Apache Iceberg and Delta Lake were both 
created to help alleviate those problems.

For more insight into why we created Nessie, you can read a founding [blog post](https://www.dremio.com/blog/) by one of it's 
creators.

## Inspiration

The Iceberg format (as well as the Delta Lake format) relies on a set of metadata files stored with (or near) the actual
data tables. This allows Iceberg to fulfill the same role as the Hive Metastore for transactions without the need for
expensive metadata scans or centralized planning (see [Iceberg
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

The Nessie service is a lightweight Java based REST API server. It uses a standard optimistic locking strategy
to ensure atomic transactions. This relies on every operation carrying an expected 
hash state for the store and allows for a very light weight and
scalable implementation. The implementation uses configurable authentication (eg IAM on AWS, JWT elsewhere) and a 
configurable backend (currently supporting DynamoDB on AWS) and uses the optimistic locking features of cloud based
key value stores to ensure scalability across servers. This architecture allows for Nessie to run in a docker container,
as a Lambda function or in a number of other configurations.
