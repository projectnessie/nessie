# Spark via Delta Lake

!!! note
    Detailed steps on how to set up Pyspark + Delta Lake + Nessie with Python is available on [Binder](https://mybinder.org/v2/gh/projectnessie/nessie-demos/main?filepath=notebooks/nessie-delta-demo-nba.ipynb).

To access Nessie from a spark cluster make sure the `spark.jars` spark option is set to include
the [Spark 2](https://repo.maven.apache.org/maven2/org/projectnessie/nessie-deltalake-spark2/{{ versions.java}}/nessie-deltalake-spark2-{{ versions.java}}.jar)
or [Spark 3](https://repo.maven.apache.org/maven2/org/projectnessie/nessie-deltalake-spark3/{{ versions.java}}/nessie-deltalake-spark3-{{ versions.java}}.jar)
jar. These jars contain all Nessie **and** Delta Lake libraries required for operation.

!!! note
    the `spark3` jar is for Delta versions >0.7.0 on Spark3 and the `spark2` jar is for Delta versions 0.6.x on Spark2.4

In pyspark this would look like

``` python
SparkSession.builder
    .config('spark.jars.packages',
            'org.projectnessie:nessie-deltalake-spark3:{{ versions.java}}')
    ... rest of spark config
    .getOrCreate()
```

The Nessie LogStore needs the following parameters set in the Spark/Hadoop config.

```
nessie.url = full url to nessie
nessie.authentication.type = authentication type 
spark.delta.logFileHandler.class=org.projectnessie.deltalake.NessieLogFileMetaParser
spark.delta.logStore.class=org.projectnessie.deltalake.NessieLogStore
```

These are set as follows in code (or through other methods as described [here](https://spark.apache.org/docs/latest/configuration.html))

=== "Java"
    ``` java
    //for a local spark instance
    conf.set("spark.jars.packages",
            "org.projectnessie:nessie-deltalake-spark3:{{ versions.java}}")
        .set("spark.hadoop.nessie.url", url)
        .set("spark.hadoop.nessie.ref", branch)
        .set("spark.hadoop.nessie.authentication.type", authType)
        .set("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .set("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.delta.logStore.class",
            "org.projectnessie.deltalake.NessieLogStore")
        .set("spark.delta.logFileHandler.class",
            "org.projectnessie.deltalake.NessieLogFileMetaParser");
    spark = SparkSession.builder()
                        .master("local[2]")
                        .config(conf)
                        .getOrCreate();
    ```
=== "Python"
    ``` python
    # here we are assuming NONE authorisation
    spark = SparkSession.builder \
            .config("spark.jars.packages",
                "org.projectnessie:nessie-deltalake-spark3:{{ versions.java}}") \
            .config("spark.hadoop.nessie.url",
                "http://localhost:19120/api/v1") \
            .config("spark.hadoop.nessie.ref", "main") \
            .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.delta.logFileHandler.class",
                "org.projectnessie.deltalake.NessieLogFileMetaParser") \
            .config("spark.delta.logStore.class",
                "org.projectnessie.deltalake.NessieLogStore") \
            .getOrCreate()
    ```

Additional authentication settings are documented in the [Athentication in Spark](../spark_auth_config.md) section.

Note above we specified the option `spark.hadoop.nessie.ref`. This value sets the default branch that the delta
catalog will use. This can be changed by changing the `hadoopConfiguration` however best practice would be to use a
single write context (branch) for the duration of the spark session.

The key to enabling Nessie is to instruct Delta to use the Nessie specific `LogStore` and `LogFileHandler`. With these
enabled the Delta core library will delegate transaction handling to Nessie.

Finally, note we have explicitly enabled Delta's SQL extensions which enable Delta specific SQL in Spark3.

!!! warning
    Currently Delta metadata operations like `VACUUM` are destructive to Nessie managed Delta tables. **Do not** run
    these operations. Future versions of Nessie will disable these commands when Nessie is activated.

## Writing

Spark support is constantly evolving and the differences in Spark3 vs Spark2.4 is considerable. See the
[delta](https://docs.delta.io/latest/delta-batch.html) docs for an up-to-date support table.

### Spark2

Spark2.4 supports reads, appends, overwrites in Delta via data frames. Spark 3 additionally supports SQL syntax.
Nessie tables in delta can be written via the Nessi enabled Delta client. The Delta writer allows for either `overwrite`
or `append` mode in a standard `spark.write`.

=== "Java"
    ``` java
    regionDf = spark.read().load("data/region.parquet");
    regionDf.write().format("delta").mode("overwrite")
        .save("/location/to/delta/testing/region");
    ```
=== "Python"
    ``` python
    region_df = spark.read.load("data/region.parquet")
    region_df.write.format("delta").mode("overwrite") \
        .save("/location/to/delta/testing/region")
    ```
=== "SQL"
    ``` sql
    CREATE TABLE nessie.testing.city (
        C_CITYKEY BIGINT, C_NAME STRING, N_NATIONKEY BIGINT, C_COMMENT STRING
    ) USING delta PARTITIONED BY (N_NATIONKEY) LOCATION 'path/to/delta/testing/city'
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
    spark.sparkContext().hadoopConfiguration().set("nessie.ref", "dev");
    regionDf = spark.read().load("data/region.parquet");
    regionDf.write().format("delta").mode("overwrite")
        .save("/location/to/delta/testing/region");
    ```
=== "Python"
    ``` python
    spark.sparkContext._jsc.hadoopConfiguration().set("nessie.ref", "dev")
    region_df = spark.read.load("data/region.parquet")
    region_df.write.format("delta").mode("overwrite") \
        .save("/location/to/delta/testing/region")
    ```
=== "SQL"
    ``` sql
    -- change hadoop configuration externally using the Java or Python syntax
    CREATE TABLE nessie.testing.city (
        C_CITYKEY BIGINT, C_NAME STRING, N_NATIONKEY BIGINT, C_COMMENT STRING
    ) USING iceberg PARTITIONED BY (N_NATIONKEY)
    -- AS SELECT .. can be added to the sql statement to perform a CTAS
    
    INSERT INTO nessie.testing.city VALUES (1, 'a', 1, 'comment')
    ```

We have to manually change the `hadoopConfiguration` for the `SparkContext` for a Delta table to be initialised with the
correct reference. This will change in the near future when it will be possible to use the same `branch@ref` syntax as
[Iceberg](/tools/spark/#writing) inside of delta. Currently, it isn't possible to change the ref from SQL directly. This
should be fixed in an upcomming release.

!!! note
    Delta by default caches tables internally. If an action has to happen on the same table but a different branch the
    cache first should be cleared. `DeltaLog.clearCache()`.


## Reading

Reading is similar between Spark2 and Spark3. We will look at both versions together in this section. To
read a Nessie table in Delta Lake simply:

=== "Java"
    ``` java
    regionDf = spark.read().format("delta")
        .load("/path/to/delta/testing/region");
    ```
=== "Python"
    ``` python
    region_df = spark.read.format("delta") \
        .load("/path/to/delta/testing/region")
    ```
=== "SQL"
    ``` sql
    SELECT * FROM '/path/to/delta/testing/region'
    ```

The examples above all use the default branch defined on initialisation. Future versions will add the ability to specify
a branch and timestamp similar to Iceberg. Currently, to switch branches a similar technique as writing is required
(manually changing the hadoopConfiguration). History can be viewed on the command line or via the python client and a specific
hash based on commit time can be extracted for use in the spark config. It is recommended to use the time-travel
features of Nessie over the Delta features as Nessie history is consistent across the entire database.
