# Spark

Nessie can be used with Spark in several different ways. These include:

## Iceberg client

Nessie works seamlessly with Iceberg in Spark2 and Spark3. Nessie is implemented as a custom iceberg 
[catalog](http://iceberg.apache.org/custom-catalog/) and therefore supports all features available to any Iceberg
client. This includes Spark structured streaming, Presto and Flink. See the [iceberg docs](https://iceberg.apache.org)
for more info. We update soon with instructions to use these technologies with Nessie.

!!! note
  You can follow along interactively in a Jupyter notebook by following the instructions 
  [here](https://github.com/projectnessie/nessie/tree/main/python/demo).

To access Nessie from a spark cluster make sure the `spark.jars` spark option is set to include 
the [Spark 2](https://repo.maven.apache.org/maven2/org/projectnessie/nessie-iceberg-spark2/{{ versions.java }}/nessie-iceberg-spark2-{{ versions.java }}.jar) 
or [Spark 3](https://repo.maven.apache.org/maven2/org/projectnessie/nessie-iceberg-spark3/{{ versions.java }}/nessie-iceberg-spark3-{{ versions.java }}.jar) Nessie plugin jar. This fat jar contains 
all Nessie **and** Apache Iceberg libraries required for operation. 

!!! note
    The Nessie team is working to incorporate Nessie support directly into the Iceberg project moving forward.

In pyspark this would look like

``` python
SparkSession.builder
            .config('spark.jars', 'path/to/nessie-iceberg-spark3-{{ versions.java }}.jar')
            ... rest of spark config
            .getOrCreate()
```

The docs for the [Java api](https://iceberg.apache.org/java-api-quickstart) in Iceberg explain how to use a `Catalog`. 
The only change is that a Nessie catalog should be instantiated

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
    still have to go through the jvm objects. See the [demo](https://github.com/projectnessie/nessie/tree/main/python/demo) 
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
        .set("spark.hadoop.nessie.auth_type", authType)
        .set("spark.sql.catalog.nessie", "com.dremio.nessie.iceberg.spark.NessieIcebergSparkCatalog");
    spark = SparkSession.builder()
                        .master("local[2]")
                        .config(conf)
                        .getOrCreate();
    ```
=== "Python"
    ``` python
    # here we are assuming NONE authorisation
    spark = SparkSession.builder \
            .config("spark.jars", "../../clients/iceberg-spark3/target/nessie-iceberg-spark3-{{ versions.java }}.jar") \
            .config("spark.hadoop.nessie.url", "http://localhost:19120/api/v1") \
            .config("spark.hadoop.nessie.ref", "main") \
            .config("spark.sql.catalog.nessie", "com.dremio.nessie.iceberg.spark.NessieIcebergSparkCatalog") \
            .getOrCreate()
    ```
Note above we specified the option `spark.hadoop.nessie.ref`. This value sets the default branch that the iceberg
catalog will use. This can be changed by changing the `hadoopConfiguration` however best practice would be to use a
single write context (branch) for the duration of the spark session. Read context can be changed dynamically as shown
below.

We have also specified `spark.sql.catalog.nessie` to point to our `NessieIcebergSparkCatalog`. This is a Spark3 only
option to set the catalog `nessie` to be managed by Nessie's Catalog implementation.

### Writing

Spark support is constantly evolving and the differences in Spark3 vs Spark2.4 are considerable. See the 
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

When looking at the Python code above, lines 1-10 are importing jvm objects into pyspark. Lines 11-25 create the table name, schema and partition spec. These
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
the default branch.

#### Spark3

The write path for Spark3 is slightly different and easier to work with. These changes haven't made it to pyspark yet so
writing dataframes looks much the same there, including having to create the table. Spark3 table creation/insertion is as
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
across all environments though. The general pattern is `<table>@<branch>`. Table must be present and either
branch and/or hash are optional. We will throw an error if branch or hash don't exist.
Branch or hash references in the table name will override passed `option`s and the settings in the
Spark/Hadoop configs.

``` python linenums="1"
spark.read().option("nessie.ref", "dev").format("iceberg").load("testing.region") # read from dev branch
spark.read().option("nessie.ref", "<hash>").format("iceberg").load("testing.region") # read from branch at specific point in time
spark.read().format("iceberg").load("testing.region@dev") # read from branch dev
spark.read().format("iceberg").load("testing.region@<hash>") # read specifically from hash
spark.sql("SELECT * FROM nessie.testing.`region@dev`")
spark.sql("SELECT * FROM nessie.testing.`region@<hash>`")
```

Notice in the SQL statements the `table@branch` must be escaped separately from namespace or catalog arguments.

Future versions may add the ability to specify a timestamp to query the data at a specific point in time
(time-travel). In the meantime the history can be viewed on the command line or via the python client and a specific
hash based on commit time can be extracted for use in the spark catalog. It is recommended to use the time-travel
features of Nessie over the Iceberg features as Nessie history is consistent across the entire database.

## Delta Lake

!!! note
    You can follow along interactively in a Jupyter notebook by following the instructions 
    [here](https://github.com/projectnessie/nessie/tree/main/python/demo).

Delta Lake support in Nessie requires some minor modifications to the core Delta libraries. This patch is still ongoing,
in the meantime Nessie will not work on Databricks and must be used with the open source Delta. Nessie is able to interact with Delta Lake by implementing a
custom version of Delta's LogStore [interface](https://github.com/delta-io/delta/blob/master/src/main/scala/org/apache/spark/sql/delta/storage/LogStore.scala).
This ensures that all filesystem changes are recorded by Nessie as commits. The benefit of this approach is the core
ACID primitives are handled by Nessie. The limitations around [concurrency](https://docs.delta.io/latest/delta-storage.html)
that Delta would normally have are removed, any number of readers and writers can simultaneously interact with a Nessie 
managed Delta Lake table.
 
To access Nessie from a spark cluster make sure the `spark.jars` spark option is set to include 
the [Spark 2](https://repo.maven.apache.org/maven2/org/projectnessie/nessie-deltalake-spark2/{{ versions.java}}/nessie-deltalake-spark2-{{ versions.java}}.jar) 
or [Spark 3](https://repo.maven.apache.org/maven2/org/projectnessie/nessie-deltalake-spark3/{{ versions.java}}/nessie-deltalake-spark3-{{ versions.java}}.jar) 
jar. These jars contain all Nessie **and** Delta Lake libraries required for operation. 

!!! note
    the `spark3` jar is for Delta versions >7.0 on Spark3 and the `spark2` jar is for Delta versions 6.x on Spark2.4

In pyspark this would look like

``` python
SparkSession.builder
            .config('spark.jars', 'path/to/nessie-deltalake-spark2-{{ versions.java}}.jar')
            ... rest of spark config
            .getOrCreate()
```

The Nessie LogStore needs the following parameters set in the Spark/Hadoop config.

```
nessie.url = full url to nessie
nessie.username = username if using basic auth, omitted otherwise
nessie.password = password if using basic auth, omitted otherwise
nessie.auth.type = authentication type (BASIC, NONE or AWS)
spark.delta.logFileHandler.class=com.dremio.nessie.deltalake.NessieLogFileMetaParser
spark.delta.logStore.class=com.dremio.nessie.deltalake.NessieLogStore
```

These are set as follows in code (or through other methods as described [here](https://spark.apache.org/docs/latest/configuration.html))

=== "Java"``
    ``` java
    //for a local spark instance
    conf.set("spark.hadoop.nessie.url", url)
        .set("spark.hadoop.nessie.ref", branch)
        .set("spark.hadoop.nessie.auth_type", authType)
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .set("spark.delta.logStore.class", "com.dremio.nessie.deltalake.NessieLogStore")
        .set("spark.delta.logFileHandler.class", "com.dremio.nessie.deltalake.NessieLogFileMetaParser")
    spark = SparkSession.builder()
                        .master("local[2]")
                        .config(conf)
                        .getOrCreate();
    ```
=== "Python"
    ``` python
    # here we are assuming NONE authorisation
    spark = SparkSession.builder \
            .config("spark.jars", "../../clients/deltalake-spark3/target/nessie-deltalake-spark3-{{ versions.java}}.jar") \
            .config("spark.hadoop.nessie.url", "http://localhost:19120/api/v1") \
            .config("spark.hadoop.nessie.ref", "main") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.delta.logFileHandler.class", "com.dremio.nessie.deltalake.NessieLogFileMetaParser") \
            .config("spark.delta.logStore.class", "com.dremio.nessie.deltalake.NessieLogStore") \
            .getOrCreate()
    ```
Note above we specified the option `spark.hadoop.nessie.ref`. This value sets the default branch that the delta
catalog will use. This can be changed by changing the `hadoopConfiguration` however best practice would be to use a
single write context (branch) for the duration of the spark session. 

The key to enabling Nessie is to instruct Delta to use the Nessie specific `LogStore` and `LogFileHandler`. With these
enabled the Delta core library will delegate transaction handling to Nessie.

Finally, note we have explicitly enabled Delta's SQL extensions which enable Delta specific SQL in Spark3.

!!! warning
    Currently Delta metadata operations like `VACUUM` are descructive to Nessie managed Delta tables. **Do not** run 
    these operations. Future versions of Nessie will disable these commands when Nessie is activated.

### Writing

Spark support is constantly evolving and the differences in Spark3 vs Spark2.4 is considerable. See the 
[delta](https://docs.delta.io/latest/delta-batch.html) docs for an up to date support table.

#### Spark2

Spark2.4 supports reads, appends, overwrites in Delta via data frames. Spark 3 additionally supports SQL syntax.
Nessie tables in delta can be written via the Nessi enabled Delta client. The Delta writer allows for either `overwrite`
or `append` mode in a standard `spark.write`.

=== "Java"
    ``` java
    regionDf = spark.read().load("data/region.parquet")
    regionDf.write().format("delta").mode("overwrite").save("/location/to/delta/testing/region")
    ```
=== "Python"
    ``` python
    region_df = spark.read.load("data/region.parquet")
    region_df.write.format("delta").mode("overwrite").save("/location/to/delta/testing/region")
    ```
 === "SQL"
    ``` sql
    CREATE TABLE nessie.testing.city (C_CITYKEY BIGINT, C_NAME STRING, N_NATIONKEY BIGINT, C_COMMENT STRING) USING delta PARTITIONED BY (N_NATIONKEY) LOCATION 'path/to/delta/testing/city'
    -- SELECT .. can be added to the sql statement to perform a CTAS
    INSERT [OVERWRITE] INTO nessie.testing.city VALUES (1, 'a', 1, 'comment')
    ```

Here we simply read a file from the default filesystem and write it to a new nessie Delta table. This will
trigger a commit on current context's branch.

For the examples above we have performed commits on the branch specified when we set our spark configuration. Had we not
specified the context in our spark configuration all operations would have defaulted to the default branch defined by
the server. This is a strong pattern for a spark job which is for example writing data as part of a wider ETL job. It
will only ever need one context or branch to write to. If however you are running an interactive session and would like
to write to a specific branch without changing context the following should be used to change the context.

=== "Java"
    ``` java
    spark.sparkContext().hadoopConfiguration().set("nessie.ref", "dev")
    regionDf = spark.read().load("data/region.parquet")
    regionDf.write().format("delta").mode("overwrite").save("/location/to/delta/testing/region")
    ```
=== "Python"
    ``` python
    spark.sparkContext._jsc.hadoopConfiguration().set("nessie.ref", "dev")
    region_df = spark.read.load("data/region.parquet")
    region_df.write.format("delta").mode("overwrite").save("/location/to/delta/testing/region")
    ```
=== "SQL"
    ``` sql
    -- change hadoop configuration externally using the Java or Python syntax
    CREATE TABLE nessie.testing.city (C_CITYKEY BIGINT, C_NAME STRING, N_NATIONKEY BIGINT, C_COMMENT STRING) USING iceberg PARTITIONED BY (N_NATIONKEY)
    -- AS SELECT .. can be added to the sql statement to perform a CTAS
    INSERT INTO nessie.testing.city VALUES (1, 'a', 1, 'comment')
    ```

We have to manually change the `hadoopConfiguration` for the `SparkContext` for a Delta table to be initialised with the
correct reference. This will change in the near future when it will be possible to use the same `branch@ref` syntax as 
[Iceberg](/tools/spark/#writing) inside of delta. Currently it isn't possible to change the ref from SQL directly. This
should be fixed in an upcomming release.

!!! note
    Delta by default caches tables internally. If an action has to happen on the same table but a different branch the
    cache first should be cleared. `DeltaLog.clearCache()`.


### Reading

Reading is similar between Spark2 and Spark3. We will look at both versions together in this section. To
read a Nessie table in Delta Lake simply:

=== "Java"
    ``` java
    regionDf = spark.read().format("delta").load("/path/to/delta/testing/region") 
    ```
=== "Python"
    ``` python
    region_df = spark.read.format("delta").load("/path/to/delta/testing/region")
    ```
=== "SQL"
    ``` sql
    SELECT * FROM '/path/to/delta/testing/region' 
    ```

The examples above all use the default branch defined on initialisation. Future versions will add the ability to specify
a branch and timestamp similar to Iceberg. Currently to switch branches a similar technique as writing is required
(manually changing the hadoopConfiguration). History can be viewed on the command line or via the python client and a specific
hash based on commit time can be extracted for use in the spark config. It is recommended to use the time-travel
features of Nessie over the Delta features as Nessie history is consistent across the entire database.

