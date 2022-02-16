# Spark via Iceberg

!!! note    
    Detailed steps on how to set up Pyspark + Iceberg + Nessie with Python is available on [Binder](https://mybinder.org/v2/gh/projectnessie/nessie-demos/main?filepath=notebooks/nessie-iceberg-demo-nba.ipynb)

To access Nessie from a spark cluster make sure the `spark.jars` spark option is set to include
the [Spark 2](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark/{{ versions.iceberg }}/iceberg-spark-{{ versions.iceberg }}.jar)
or [Spark 3](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark3/{{ versions.iceberg }}/iceberg-spark3-{{ versions.iceberg }}.jar) Nessie plugin jar. This fat jar
is distributed by the Apache Iceberg project and contains all Apache Iceberg libraries required for operation, including the built-in Nessie Catalog.

In pyspark this would look like

``` python
SparkSession.builder
    .config('spark.jars.packages',
            'org.apache.iceberg:iceberg-spark3-runtime:{{ versions.iceberg }}')
    ... rest of spark config
    .getOrCreate()
```

!!! note
    The Spark config parameter `spark.jars.packages` uses Maven coordinates to pull the given
    dependencies and all transitively required dependencies as well. Dependencies are resolved
    via the local Ivy cache, the local Maven repo and then against Maven Central.
    The config parameter `spark.jars` only takes a list of jar files and does not resolve
    transitive dependencies.

The docs for the [Java API](https://iceberg.apache.org/java-api-quickstart) in Iceberg explain how to use a `Catalog`.
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
    still have to go through the jvm objects. See the [demo](https://mybinder.org/v2/gh/projectnessie/nessie-demos/main?filepath=notebooks/nessie-iceberg-demo-nba.ipynb)
    directory for details.

## Configuration

The Nessie Catalog needs the following parameters set in the Spark/Hadoop config.

These are set as follows in code (or through other methods as described [here](https://spark.apache.org/docs/latest/configuration.html))

=== "Java"
    ``` java
    // Full url of the Nessie API endpoint to nessie
    String url = "http://localhost:19120/api/v1";
    // Where to store nessie tables
    String fullPathToWarehouse = ...;
    // The ref or context that nessie will operate on
    // (if different from default branch).
    // Can be the name of a Nessie branch or tag name. 
    String ref = "main";
    // Nessie authentication type (BASIC, NONE or AWS)
    String authType = "NONE";

    //for a local spark instance
    conf.set("spark.jars.packages",
            "org.apache.iceberg:iceberg-spark3-runtime:{{ versions.iceberg }},org.projectnessie:nessie-spark-extensions:{{ versions.java }}")
        .set("spark.sql.extensions", 
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        .set("spark.sql.catalog.nessie.uri", url)
        .set("spark.sql.catalog.nessie.ref", ref)
        .set("spark.sql.catalog.nessie.authentication.type", authType)
        .set("spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie.warehouse", fullPathToWarehouse)
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog");
    spark = SparkSession.builder()
                        .master("local[2]")
                        .config(conf)
                        .getOrCreate();
    ```
=== "Python"
    ``` python
    # Full url of the Nessie API endpoint to nessie
    url = "http://localhost:19120/api/v1"
    # Where to store nessie tables
    full_path_to_warehouse = ...
    # The ref or context that nessie will operate on (if different from default branch).
    # Can be the name of a Nessie branch or tag name.
    ref = "main"
    # Nessie authentication type (BASIC, NONE or AWS)
    auth_type = "NONE"

    # here we are assuming NONE authorisation
    spark = SparkSession.builder \
            .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark3-runtime:{{ versions.iceberg }},org.projectnessie:nessie-spark-extensions:{{ versions.java }}") \
            .config("spark.sql.extensions", 
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
            .config("spark.sql.catalog.nessie.uri", url) \
            .config("spark.sql.catalog.nessie.ref", ref) \
            .config("spark.sql.catalog.nessie.authentication.type", auth_type) \
            .config("spark.sql.catalog.nessie.catalog-impl", 
                "org.apache.iceberg.nessie.NessieCatalog") \
            .config("spark.sql.catalog.nessie.warehouse", full_path_to_warehouse) \
            .config("spark.sql.catalog.nessie",
                "org.apache.iceberg.spark.SparkCatalog") \
            .getOrCreate()
    ```

All configuration for the Nessie catalog exists below this `spark.sql.catalog.nessie` configuration namespace. The catalog name is not important, it is important that the
required options are all given below the catalog name.

The following properties are **required** in Spark when creating the Nessie Catalog (replace `<catalog_name>` with the name of your catalog):

- `spark.sql.catalog.<catalog_name>.uri` : The location of the Nessie server.
- `spark.sql.catalog.<catalog_name>.ref` : The default Nessie branch that the iceberg
  catalog will use.
- `spark.sql.catalog.<catalog_name>.authentication.type` : The authentication type to be used, set to `NONE` by default. Please refer to the [authentication docs](../auth_config.md) for more info.
- `spark.sql.catalog.<catalog_name>.catalog-impl` : This **must** be `org.apache.iceberg.nessie.NessieCatalog` in order to tell Spark to use Nessie catalog implementation.
- `spark.sql.catalog.<catalog_name>.warehouse` : The location where to store Iceberg tables managed by Nessie catalog.
- `spark.sql.catalog.<catalog_name>` : This **must** be `org.apache.iceberg.spark.SparkCatalog`. This is a Spark
  option to set the catalog `<catalog_name>` to be managed by Nessie's Catalog implementation.


## Writing

Spark support is constantly evolving and the differences in Spark3 vs Spark2.4 are considerable. See the
[iceberg](https://iceberg.apache.org/spark-writes/) docs for an up-to-date support table.

### Spark2

Spark2.4 supports reads, appends, overwrites in Iceberg. Nessie tables in iceberg can be written via the Nessie Iceberg
Catalog instantiated above. Iceberg in Spark2.4 has no ability to create tables so before a table can be appended to or
overwritten the table must be first created via an Iceberg Catalog. This is straightforward in Java but requires
addressing jvm objects directly in Python (until the python library for iceberg is released).

=== "Java"
    ``` java linenums="11"
    // first instantiate the catalog
    NessieCatalog catalog = new NessieCatalog();
    catalog.setConf(sc.hadoopConfiguration());
    catalog.initialize("nessie", ImmutableMap.of(
        "ref", ref,
        "url", url,
        "warehouse", pathToWarehouse));

    // Creating table by first creating a table name with namespace
    TableIdentifier region_name = TableIdentifier.parse("testing.region");

    // next create the schema
    Schema region_schema = Schema([
      Types.NestedField.optional(1, "R_REGIONKEY", Types.LongType.get()),
      Types.NestedField.optional(2, "R_NAME", Types.StringType.get()),
      Types.NestedField.optional(3, "R_COMMENT", Types.StringType.get()),
    ]);

    // and the partition
    PartitionSpec region_spec = PartitionSpec.unpartitioned();

    // finally create the table
    catalog.createTable(region_name, region_schema, region_spec);

    ```
=== "Python"
    ``` python linenums="1"
    sc = spark.sparkContext
    jvm = sc._gateway.jvm

    # import jvm libraries for iceberg catalogs and schemas
    java_import(jvm, "org.projectnessie.iceberg.NessieCatalog")
    java_import(jvm, "org.apache.iceberg.catalog.TableIdentifier")
    java_import(jvm, "org.apache.iceberg.Schema")
    java_import(jvm, "org.apache.iceberg.types.Types")
    java_import(jvm, "org.apache.iceberg.PartitionSpec")

    # first instantiate the catalog
    catalog = jvm.NessieCatalog()
    catalog.setConf(sc._jsc.hadoopConfiguration())
    catalog.initialize("nessie", {"ref": ref,
        "url": url,
        "warehouse": pathToWarehouse})

    # Creating table by first creating a table name with namespace
    region_name = jvm.TableIdentifier.parse("testing.region")

    # next create the schema
    region_schema = jvm.Schema([
      jvm.Types.NestedField.optional(
        1, "R_REGIONKEY", jvm.Types.LongType.get()
      ),
      jvm.Types.NestedField.optional(
        2, "R_NAME", jvm.Types.StringType.get()
      ),
      jvm.Types.NestedField.optional(
        3, "R_COMMENT", jvm.Types.StringType.get()
      ),
    ])

    # and the partition
    region_spec = jvm.PartitionSpec.unpartitioned()

    # finally create the table
    region_table = catalog.createTable(region_name, region_schema, region_spec)
    ```

When looking at the Python code above, lines 1-11 are importing jvm objects into pyspark. Lines 12-25 create the table name, schema and partition spec. These
actions will be familiar to seasoned iceberg users and are wholly iceberg operations. Line 29 is where our initial
iceberg metadata is finally written to disk and a commit takes place on Nessie.

Now that we have created an Iceberg table in nessie we can write to it. The iceberg `DataSourceV2` allows for either
`overwrite` or `append` mode in a standard `spark.write`.

=== "Java"
    ``` java
    regionDf = spark.read().load("data/region.parquet");
    regionDf.write().format("iceberg").mode("overwrite")
        .save("nessie.testing.region");
    ```
=== "Python"
    ``` python
    region_df = spark.read.load("data/region.parquet")
    region_df.write.format("iceberg").mode("overwrite") \
        .save("nessie.testing.region")
    ```

Here we simply read a file from the default filesystem and write it to an existing nessie iceberg table. This will
trigger a commit on current context's branch.

For the examples above we have performed commits on the branch specified when we set our spark configuration. Had we not
specified the context in our spark configuration all operations would have defaulted to the default branch defined by
the server. This is a strong pattern for a spark job which is for example writing data as part of a wider ETL job. It
will only ever need one context or branch to write to. If however you are running an interactive session and would like
to write to a specific branch you would have to create a new Spark Conf.

=== "Java"
    ``` java
    sparkDev = spark.newSession();
    sparkDev.conf.set("spark.sql.catalog.nessie.ref", "dev");
    regionDf = sparkDev.read().load("data/region.parquet");
    regionDf.write().format("iceberg").mode("overwrite")
        .save("nessie.testing.region");
    ```
=== "Python"
    ``` python
    spark_dev = spark.newSession()
    spark_dev.conf.set("spark.sql.catalog.nessie.ref", "dev")
    region_df = spark_dev.read.load("data/region.parquet")
    region_df.write.format("iceberg").mode("overwrite") \
        .save("nessie.testing.region")
    ```

Note the extra `option` clause in the write command. This will ensure the commit happens on the `dev` branch rather than
the default branch.

### Spark3

The write path for Spark3 is slightly different and easier to work with. These changes haven't made it to pyspark yet so
writing dataframes looks much the same there, including having to create the table. Spark3 table creation/insertion is as
follows:

=== "Java"
    ``` java
    regionDf = spark.read().load('data/region.parquet');
    //create
    regionDf.writeTo("nessie.testing.region").create();
    //append
    regionDf.writeTo("nessie.testing.region").append();
    //overwrite partition
    regionDf.writeTo("nessie.testing.region").overwritePartitions();
    ```
=== "Python"
    ``` python
    # same code as the spark2 section above to create the testing.region table
    region_df = spark.read.load("data/region.parquet")
    region_df.write.format("iceberg").mode("overwrite") \
        .save("nessie.testing.region")
    ```
=== "SQL"
    ``` sql
    CREATE TABLE nessie.testing.city (
        C_CITYKEY BIGINT, C_NAME STRING, N_NATIONKEY BIGINT, C_COMMENT STRING
    ) USING iceberg PARTITIONED BY (N_NATIONKEY)
    -- AS SELECT .. can be added to the sql statement to perform a CTAS

    INSERT INTO nessie.testing.city VALUES (1, 'a', 1, 'comment')
    ```

The full list of operations can be found [here](https://iceberg.apache.org/spark-writes/). Everything that Iceberg
supports the Nessie Iceberg Catalog also supports.

## Reading

Reading is more straightforward between spark 2 and spark 3. We will look at both versions together in this section. To
read a Nessie table in iceberg simply:

=== "Java"
    ``` java
    // Spark2:
    regionDf = spark.read().format("iceberg")
        .load("nessie.testing.region");

    // Spark3:
    regionDf = spark.table("nessie.testing.region");
    ```
=== "Python"
    ``` python
    # same code as above to create the testing.region table
    region_df = spark.read.format("iceberg").load("nessie.testing.region")
    ```
=== "SQL"
    ``` sql
    -- Spark3 only
    SELECT * FROM nessie.testing.city
    ```
    ``` sql
    -- Spark3 only, read from the `etl` branch
    SELECT * FROM nessie.testing.`city@etl`
    ```

The examples above all use the default branch defined on initialisation. There are several ways to reference specific
branches or hashes from within a read statement. We will take a look at a few now from pyspark3, the rules are the same
across all environments though. The general pattern is `<table>@<branch>`. Table must be present and either
branch and/or hash are optional. We will throw an error if branch or hash don't exist.
Branch or hash references in the table name will override passed `option`s and the settings in the
Spark/Hadoop configs.

``` python linenums="1"
# read from branch dev
spark.read().format("iceberg").load("testing.region@dev")
# read specifically from hash
spark.read().format("iceberg").load("testing.region@<hash>")

spark.sql("SELECT * FROM nessie.testing.`region@dev`")
spark.sql("SELECT * FROM nessie.testing.`region@<hash>`")
```

Notice in the SQL statements the `table@branch` must be escaped separately from namespace or catalog arguments.

Future versions may add the ability to specify a timestamp to query the data at a specific point in time
(time-travel). In the meantime the history can be viewed on the command line or via the python client and a specific
hash based on commit time can be extracted for use in the spark catalog. It is recommended to use the time-travel
features of Nessie over the Iceberg features as Nessie history is consistent across the entire database.
