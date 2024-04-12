---
title: "Nessie + Iceberg + Flink"
---

# Flink via Iceberg

!!! note    
    Detailed steps on how to set up Pyspark + Iceberg + Flink + Nessie with Python is available on [Binder](https://mybinder.org/v2/gh/projectnessie/nessie-demos/main?filepath=notebooks/nessie-iceberg-flink-demo-nba.ipynb)

In order to use Flink with Python API, you will need to make sure `pyflink` have access to all Hadoop JARs as mentioned in these [docs](https://iceberg.apache.org/docs/latest/flink/#flinks-python-api). After that, you will need to make sure `iceberg-flink-runtime` is added to Flink. This can be done by adding the iceberg JAR to `pyflink` via `env.add_jar`, e.g: `env.add_jars("file://path/to/jar/iceberg-flink-runtime-{{ versions.iceberg }}.jar")`. This can be shown below:

```python
import os

from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
iceberg_flink_runtime_jar = os.path.join(os.getcwd(), "iceberg-flink-runtime-{{ versions.iceberg }}.jar")

env.add_jars("file://{}".format(iceberg_flink_runtime_jar))
```

Once we have added `iceberg-flink-runtime` JAR to `pyflink`, we can then create `StreamTableEnvironment` and execute Flink SQL statements. This can be shown in the following example:

```python
from pyflink.table import StreamTableEnvironment

table_env = StreamTableEnvironment.create(env)

table_env.execute_sql(
        """CREATE CATALOG <catalog_name> WITH (
        'type'='iceberg',
        'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',
        'uri'='http://localhost:19120/api/v1',
        'ref'='main',
        'warehouse' = '/path/to/flink/warehouse')"""
    )
```

With the above statement, we have created a Nessie catalog (via Iceberg) that Flink will use to manage the tables.

For more general information about Flink and Iceberg, refer to [Iceberg and Flink documentation](https://iceberg.apache.org/docs/latest/flink/).


## Configuration 

To use Nessie Catalog in Flink via Iceberg, we will need to create a catalog in Flink through `CREATE CATALOG` SQL statement (replace `<catalog_name>` with the name of your catalog), example:

```python
table_env.execute_sql(
        """CREATE CATALOG <catalog_name> WITH (
        'type'='iceberg',
        'catalog-impl'='org.apache.iceberg.nessie.NessieCatalog',
        'uri'='http://localhost:19120/api/v1',
        'ref'='main',
        'warehouse' = '/path/to/flink/warehouse')"""
    )
```

The following properties are **required** in Flink when creating the Nessie Catalog:

- `type`: This **must** be `iceberg` for iceberg table format.
- `catalog-impl`: This **must** be `org.apache.iceberg.nessie.NessieCatalog` in order to tell Flink to use Nessie catalog implementation.
- `uri`: The location of the Nessie server.
- `ref`: The Nessie ref/branch we want to use.
- `warehouse`: The location where to store Iceberg tables managed by Nessie catalog.
- `authentication.type`: The authentication type to be used, please refer to the [authentication docs](../nessie-latest/client_config.md) for more info.


## Create tables

To create tables in Flink that are managed by Nessie/Iceberg, you will need to specify the catalog name in addition to the database whenever you issue `CREATE TABLE` statement, e.g:

```sql
CREATE DATABASE `<catalog_name>`.`<database_name>`;

CREATE TABLE `<catalog_name>`.`<database_name>`.`<table_name>` (
    id BIGINT COMMENT 'unique id',
    data STRING
);
```

## Reading tables

To read tables in Flink, this can be done with a typical SQL `SELECT` statement, however as the same with creating tables, you will need to make sure to specify the catalog name in addition to the database. e.g:

```sql
SELECT * FROM `<catalog_name>`.`<database_name>`.`<table_name>`;
```

As well, similar to [Spark](spark.md#reading), you can read tables from specific
branches or hashes from within a `SELECT` statement. The general pattern is `<table_name>@<branch>` or `<table>#<hash>` or `<table>@<branch>#<hash>` (e.g: `salaries@main`):

```sql
SELECT * FROM `<catalog_name>`.`<database_name>`.`<table_name>@<branch>`;
SELECT * FROM `<catalog_name>`.`<database_name>`.`<table_name>#<hash>`;
SELECT * FROM `<catalog_name>`.`<database_name>`.`<table_name>@<branch>#<hash>`;
```

## Other DDL statements

To read and write into tables that are managed by Iceberg and Nessie, typical Flink SQL queries can be used. Refer to this documentation [here](https://iceberg.apache.org/docs/latest/flink-ddl/) for more information.
