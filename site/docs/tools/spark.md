# Spark

Nessie can be used with Spark in several different ways. These include:

## Iceberg client

You can follow along interactively in a Jupyter notebook by following the instructions 
[here](https://github.com/projectnessie/nessie/python/demo).

To access Nessie from a spark cluster make sure the `spark.jars` spark option is set to include
`nessie-iceberg-spark-1.0-SNAPSHOT.jar`(todo link to jar). This fat jar has all the required iceberg and nessie
libraries in it. In pyspark this would look like

``` python
SparkSession.builder
            .config('spark.jars', 'path/to/nessie-iceberg-spark-1.0-SNAPSHOT.jar')
            ... rest of spark config
            .getOrCreate()
```

Nessie implements Iceberg's custom catalog [interface](http://iceberg.apache.org/custom-catalog/). The docs for the 
[Java api](https://iceberg.apache.org/java-api-quickstart) in Iceberg explain how to use a `Catalog`. The only change is
that a Iceberg catalog should be instantiated.

=== "Java"
    ``` java
    Catalog catalog = new NessieCatalog(spark.sparkContext().hadoopConfiguration())
    ```
=== "Python"
    ``` python
    catalog = jvm.NessieCatalog(sc._jsc.hadoopConfiguration())
    ```

!!! note
    Iceberg's python libraries are still under active development. Actions against catalogs in pyspark
    still have to go through the jvm objects. See the [demo](https://github.com/projectnessie/nessie/python/demo) 
    directory for details.

The Nessie Catalog needs the following parameters set in the Spark/Hadoop config.

```
nessie.url = full url to nessie
nessie.username = username if using basic auth, omitted otherwise
nessie.password = password if using basic auth, omitted otherwise
nessie.auth.type = authentication type (BASIC, NONE or AWS)
```

These are set as follows in code (or through other methods as described [here](https://spark.apache.org/docs/latest/configuration.html))

=== "Java"
    ``` java
    //for a local spark instance
    conf.set("spark.hadoop.nessie.url", url)
        .set("spark.hadoop.nessie.ref", branch)
        .set("spark.hadoop.nessie.auth_type", authType);
    spark = SparkSession.builder()
                        .master("local[2]")
                        .config(conf)
                        .getOrCreate();
    ```
=== "Python"
    ``` python
    # here we are assuming NONE authorisation
    spark = SparkSession.builder \
            .config("spark.jars", "../../clients/iceberg-spark3/target/nessie-iceberg-spark3-1.0-SNAPSHOT.jar") \
            .config("spark.hadoop.nessie.url", "http://localhost:19120/api/v1") \
            .config("spark.hadoop.nessie.ref", "main") \
            .config("spark.sql.catalog.nessie", "com.describedmio.nessie.iceberg.spark.NessieIcebergSparkCatalog") \
            .getOrCreate()
    ```
Note above we specified the option `spark.hadoop.nessie.ref`. This value sets the default branch that the iceberg
catalog will use. This can be changed by changing the `hadoopConfiguration` however best practice would be to use a
single write context (branch) for the duration of the spark session. Read context can be changed dynamically as shown
below.

We have also specified `spark.sql.catalog.nessie` to point to our `NessieIcebergSparkCatalog`. This is a Spark3 only
option to set the catalog `nessie` to be managed by Nessie's Catalog implementation.

### Writing

Spark support is constantly evolving and the differences in Spark3 vs Spark2.4 is considerable. See the 
[iceberg](https://iceberg.apache.org/spark/#spark) docs for an up to date support table.

#### Spark2

Spark2.4 supports reads, appends, overwrites in Iceberg. Nessie tables in iceberg can be written via the Nessie Iceberg 
Catalog instantiated above. Iceberg in Spark2.4 has no ability to create tables so before a table can be appended to or
overwritten the table must be first created via an Iceberg Catalog. This is straightforward in Java but requires
addressing jvm objects directly in Python (until the python library for iceberg is released).

=== "Java"
    ``` java linenums="11"
    // first instantiate the catalog
    NessieCatalog catalog = new NessieCatalog(sc.hadoopConfiguration())

    // Creating table by first creating a table name with namespace
    TableIdentifier region_name = TableIdentifier.parse("testing.region")
  
    // next create the schema
    Schema region_schema = Schema([
      Types.NestedField.optional(1, "R_REGIONKEY", Types.LongType.get()),
      Types.NestedField.optional(2, "R_NAME", Types.StringType.get()),
      Types.NestedField.optional(3, "R_COMMENT", Types.StringType.get()),
    ])
    
    // and the partition
    PartitionSpec region_spec = PartitionSpec.unpartitioned()

    // finally create the table
    catalog.createTable(region_name, region_schema, region_spec)
    
    ```
=== "Python"
    ``` python linenums="1"
    sc = spark.sparkContext
    jvm = sc._gateway.jvm

    # import jvm libraries for iceberg catalogs and schemas
    java_import(jvm, "com.dremio.nessie.iceberg.NessieCatalog")
    java_import(jvm, "org.apache.iceberg.catalog.TableIdentifier")
    java_import(jvm, "org.apache.iceberg.Schema")
    java_import(jvm, "org.apache.iceberg.types.Types")
    java_import(jvm, "org.apache.iceberg.PartitionSpec")

    # first instantiate the catalog
    catalog = jvm.NessieCatalog(sc._jsc.hadoopConfiguration())

    # Creating table by first creating a table name with namespace
    region_name = jvm.TableIdentifier.parse("testing.region")
  
    # next create the schema
    region_schema = jvm.Schema([
      jvm.Types.NestedField.optional(1, "R_REGIONKEY", jvm.Types.LongType.get()),
      jvm.Types.NestedField.optional(2, "R_NAME", jvm.Types.StringType.get()),
      jvm.Types.NestedField.optional(3, "R_COMMENT", jvm.Types.StringType.get()),
    ])
    
    # and the partition
    region_spec = jvm.PartitionSpec.unpartitioned()

    # finally create the table
    region_table = catalog.createTable(region_name, region_schema, region_spec)
    ```

Lines 1-10 are importing jvm objects into pyspark. Lines 11-25 create the table name, schema and partition spec. These
actions will be familiar to seasoned iceberg users and are wholly iceberg operations. Line 28 is where our initial 
iceberg metadata is finally written to disk and a commit takes place on Nessie.

Now that we have created an Iceberg table in nessie we can write to it. The iceberg `DataSourceV2` allows for either
`overwrite` or `append` mode in a standard `spark.write`.

=== "Java"
    ``` java
    regionDf = spark.read().load("data/region.parquet")
    regionDf.write().format("iceberg").mode("overwrite").save("testing.region")
    ```
=== "Python"
    ``` python
    region_df = spark.read.load("data/region.parquet")
    region_df.write.format("iceberg").mode("overwrite").save("testing.region")
    ```

Here we simply read a file from the default filesystem and write it to an existing nessie iceberg table. This will
trigger a commit on current context's branch.

For the examples above we have performed commits on the branch specified when we set our spark configuration. Had we not
specified the context in our spark configuration all operations would have defaulted to the default branch defined by
the server. This is a strong pattern for a spark job which is for example writing data as part of a wider ETL job. It
will only ever need one context or branch to write to. If however you are running an interactive session and would like
to write to a specific branch without changing context the following works as well.

=== "Java"
    ``` java
    regionDf = spark.read().load("data/region.parquet")
    regionDf.write().format("iceberg").option("nessie.ref", "dev").mode("overwrite").save("testing.region")
    ```
=== "Python"
    ``` python
    region_df = spark.read.load("data/region.parquet")
    region_df.write.format("iceberg").option("nessie.ref", "dev").mode("overwrite").save("testing.region")
    ```

Note the extra `option` clause in the write command. This will ensure the commit happens on the `dev` branch rather than
the default branch. It is also possible to specify a hash via options (eg `option("nessie.hash", <hash>)`), this forces
full serializable isolation. See [Isolation Levels](../features/transactions.md#isolation-levels) for more details.

#### Spark3

The write path for Spark3 is slightly different and easier to work with. These changes haven't made it to pyspark yet so
writing datafames looks much the same there, including having to create the table. Spark3 table creation/insertion is as
follows:

=== "Java"
    ``` java
    regionDf = spark.read().load('data/region.parquet')
    //create
    regionDf.writeTo("nessie.testing.region").create()
    //append
    regionDf.writeTo("nessie.testing.region").append()
    //overwrite partition
    regionDf.writeTo("nessie.testing.region").overwritePartitions()
    ```
=== "Python"
    ``` python
    # same code as the spark2 section above to create the testing.region table
    region_df = spark.read.load("data/region.parquet")
    region_df.write.format("iceberg").mode("overwrite").save("testing.region")
    ```
=== "SQL"
    ``` sql
    CREATE TABLE nessie.testing.city (C_CITYKEY BIGINT, C_NAME STRING, N_NATIONKEY BIGINT, C_COMMENT STRING) USING iceberg PARTITIONED BY (N_NATIONKEY)
    -- AS SELECT .. can be added to the sql statement to perform a CTAS
    INSERT INTO nessie.testing.city VALUES (1, 'a', 1, 'comment')
    ```

The full list of operations can be found [here](https://iceberg.apache.org/spark/#create-table). Everything that Iceberg
supports the Nessie Iceberg Catalog also supports.

### Reading

Reading is more straightforward between spark 2 and spark 3. We will look at both versions together in this section. To
read a Nessie table in iceberg simply:

=== "Java"
    ``` java
    regionDf = spark.read().format("iceberg").load("testing.region") // Spark2
    regionDf = spark.table("nessie.testing.region") // Spark3
    ```
=== "Python"
    ``` python
    # same code as above to create the testing.region table
    region_df = spark.read.format("iceberg").load("testing.region")
    ```
=== "SQL"
    ``` sql
    SELECT * FROM nessie.testing.city -- Spark3 only
    ```

The examples above all use the default branch defined on initialisation. There are several ways to reference specific
branches or hashes from within a read statement. We will take a look at a few now from pyspark3, the rules are the same
across all environments though. The general pattern is `<table>@<branch>#<sha2>`. Table must be present and either
branch and/or sha2 are optional. We will throw an error if branch or sha2 don't exist or if sha2 isn't on branch when
both are supplied. Branch or hash references in the table name will override passed `option`s and the settings in the
Spark/Hadoop configs.

``` python linenums="1"
spark.read().option("nessie.ref", "dev").format("iceberg").load("testing.region") # read from dev branch
spark.read().option("nessie.hash", "<sha2>").format("iceberg").load("testing.region") # read from arbitrary hash 
spark.read().option("nessie.ref", "dev").option("nessie.hash", "<sha2>").format("iceberg").load("testing.region") # read from branch at specific point in time
spark.read().format("iceberg").load("testing.region@dev") # read from branch dev
spark.read().format("iceberg").load("testing.region#<sha2>") # read from sha2
spark.read().format("iceberg").load("testing.region@dev#<sha2>") # read from branch dev, specifically using sha2
spark.sql("SELECT * FROM nessie.testing.`region@dev`")
spark.sql("SELECT * FROM nessie.testing.`region#<sha2>`")
```

Notice in the SQL statements the `table@branch` must be escaped separately from namespace or catalog arguments.

Future versions may add the ability to specify a timestamp to query the data at a specific point in time
(time-travel). In the meantime the history can be viewed on the command line or via the python client and a specific
hash based on commit time can be extracted for use in the spark catalog. It is recommended to use the time-travel
features of Nessie over the Iceberg features as Nessie history is consistent across the entire database

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
