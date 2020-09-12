# Hive Tables

Hive has now existed for the more than a decade. Hive is composed of two main components: 
the Hive Engine and the Hive Metastore service (HMS). The Hive metastore has become the defacto
metadata storage standard. This includes managed service offerings from all three major cloud 
vendors and support in most tools including: Spark, Dremio, Presto, Athena, Apache 
Flink, etc.
 
In many ways Hive tables are much less sophisticated than the offerings provided by 
Delta Lake and Iceberg. One key difference in the level of resolution of metadata HMS 
maintains. While a key component of both Delta Lake and Iceberg is a complete listing 
of all files in the dataset, Hive only maintains lists of partitions and relies on 
underlying filesystem listing for things like file set consistency. Despite these weaknesses,
the prevalence of HMS as a metadata format and storage system means that providing 
a Nessie capability is still valuable.

## Nessie HMS Bridge

Nessie provides a HMS bridge that exposes a Nessie catalog as a Hive Metastore service
through the use of Hive extension APIs. Internally, this is done as an alternative

Nessie's HMS bridge is run separately from the core Nessie service and leverages Nessies 
APIs to operate against metadata.

### Hive Types Supported

The Nessie HMS Bridge provides support for Hive views and external tables that are 
mutated by partition[^1]


### Querying multiple HMS

Because the Nessie HMS Bridge only supports

### Limitations

Similar to AWS Glue, Nessie's HMS capabilities are focused on the highest value use 
cases. As such, certain features are not supported. Nessie does not support

[^1]: In Hive, tables can be marked as immutable to disallow the addition of new data 
files to existing partitions. The tables can still be mutated through the use of OVERWRITE 
or ADD and DROP partition. This flag must be enabled for Nessie to accept the table 
for storage. This is to ensure that Nessie always exposes a consistent view of data.