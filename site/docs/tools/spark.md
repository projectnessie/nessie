# Spark

## Iceberg client

To access Nessie from a spark cluster make sure the `spark.jars` spark option is set to include
`nessie-iceberg-spark-1.0-SNAPSHOT.jar`. This fat jar has all the required iceberg and nessie libraries in it.

Nessie implements Iceberg's custom catalog [interface](http://iceberg.apache.org/custom-catalog/). The docs for the 
[Java api](https://iceberg.apache.org/java-api-quickstart) in Iceberg explain how to use a `Catalog`. The only change is
that a Iceberg catalog should be instantiated:

```java
Catalog catalog = new NessieCatalog(spark.sparkContext().hadoopConfiguration())
```

The Nessie Catalog needs the following parameters set in the Spark/Hadoop config.

```
nessie.url = full url to nessie
nessie.username = username if using basic auth, omitted otherwise
nessie.password = password if using basic auth, omitted otherwise
nessie.auth.type = authentication type (BASIC or AWS)
```

You can read or write an Iceberg table into Spark using the Iceberg reader:
`df.write.format("iceberg").mode("append").save("testing.table")`. By default the spark reader/writer will use the
default branch. This can be changed by setting `nessie.view-branch` in the hadoop config or by adding
`.option("nessie.view-branch", 'branchname')` to the read/write command.

The `NessieCatalog` has methods to merge and create branches however it may be more intuitive to use the python library
or the cli to manage branches.


!!! warning
    Currently Nessie Iceberg is only supported for Spark 2



## Delta Lake
Similarly to Iceberg the Delta Lake library should be added to the `spark.jars` parameter. This fat jar has all the
required libraries for Nessie operation. The Nessie functionality is implemented as a `LogStore` and can be activated by
setting `spark.delta.logStore.class=com.dremio.nessie.deltalate.DeltaLake`. Now Delta will use Nessie to facilitate
atomic transactions. Similar to iceberg the following must be set on the hadoop configuration:

```
nessie.url = full url to nessie
nessie.username = username if using basic auth, omitted otherwise
nessie.password = password if using basic auth, omitted otherwise
nessie.auth.type = authentication type (BASIC or AWS)
nessie.view-branch = branch name. Optional, if not set the server default branch will be used
```

Once the log store is set all other operations are the same as any other Delta Lake log store. See [Getting
Started](https://docs.delta.io/latest/quick-start.html) to learn more.

!!! warning
    Spark 3 is not yet supported for Delta Lake and Nessie. Delta version must be `<=0.6.1`

!!! warning
    Currently branch operations (eg merge and create) are not supported directly from the Delta Lake connector. Use
    either the command line tool or the python library.