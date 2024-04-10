---
title: "Nessie + Iceberg + Hive"
---

# Hive via Iceberg

!!! note    
    Detailed steps on how to set up Pyspark + Iceberg + Hive + Nessie with Python is available on [Binder](https://mybinder.org/v2/gh/projectnessie/nessie-demos/main?filepath=notebooks/nessie-iceberg-hive-demo-nba.ipynb)

To access Hive via Iceberg, you will need to make sure `iceberg-hive-runtime` is added to Hive. This can be done either by adding the JAR file to `auxlib` folder in Hive home directory, by adding the JAR file to `hive-site.xml` file or via Hive shell, e.g: `add jar /path/to/iceberg-hive-runtime.jar;`. Nessie's Iceberg module is already included with `iceberg-hive-runtime` JAR distribution.

For more general information about Hive and Iceberg, refer to [Iceberg and Hive documentation](https://iceberg.apache.org/docs/latest/hive/).

## Configuration 

To configure a Nessie Catalog in Hive, first it needs to be [registered in Hive](https://iceberg.apache.org/docs/latest/hive/#custom-iceberg-catalogs), this can be done by configuring the following properties in Hive (Replace `<catalog_name>` with the name of your catalog):

```
SET iceberg.catalog.<catalog_name>.catalog-impl=org.apache.iceberg.nessie.NessieCatalog
SET iceberg.catalog.<catalog_name>.<nessie_config_property>=<config>
``` 

To use Nessie Catalog in Hive via Iceberg, the following properties are **required** within Hive:

- `iceberg.catalog.<catalog_name>.warehouse` : The location where to store Iceberg tables managed by Nessie catalog. This will be the same location that is used to create an Iceberg table as it shown below.

- `iceberg.catalog.<catalog_name>.ref` : The _current_ Nessie branch. Note that Hive doesn't support the notation of `table@branch`, therefore everytime you want to execute against a specific branch, you will need to set this property to point to the working branch, e.g: `SET iceberg.catalog.<catalog_name>.ref=main`.

- `iceberg.catalog.<catalog_name>.uri`: The location of the Nessie server.

- `iceberg.catalog.<catalog_name>.authentication.type`: The authentication type to be used, please refer to the [authentication docs](../nessie-latest/client_config.md) for more info.

For example:

```
SET iceberg.catalog.<catalog_name>.warehouse=/home/user/notebooks/nessie_warehouse;
SET iceberg.catalog.<catalog_name>.ref=dev;
SET iceberg.catalog.<catalog_name>.catalog-impl=org.apache.iceberg.nessie.NessieCatalog;
SET iceberg.catalog.<catalog_name>.uri=http://localhost:19120/api/v1;
```

## Create tables

Whenever Hive creates an Iceberg table, it will create it as external table that is managed by Iceberg catalog (in this case Nessie Catalog), thus, some properties need to be provided in order to create an Iceberg tables in Hive:

```
CREATE TABLE database_a.table_a (
  id bigint, name string
) PARTITIONED BY (
  dept string
) STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'
  LOCATION '/path_nessie_warehouse/database_a/salaries
  TBLPROPERTIES ('iceberg.catalog'='<catalog_name>', 'write.format.default'='parquet');
```
Whereby the above properties are explained as below:

- `STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler'` : Since Hive doesn't use a custom global catalog, we tell Hive here that individual table will be managed by Iceberg's catalog through `org.apache.iceberg.mr.hive.HiveIcebergStorageHandler`.

- `LOCATION '/path_nessie_warehouse/database_a/salaries` : As mentioned before, Hive will create Iceberg tables as external tables and thus, location of the data files needs to be provided. This location is the same location that is provided in `iceberg.catalog.<catalog_name>.warehouse` in addition to the database and table names.

- `'iceberg.catalog'='<catalog_name>'` : The custom Iceberg catalog to be used to manage this table.

- `'write.format.default'='parquet'` : The format that is used to store the data, this could be anything that is supported by Iceberg, e.g: ORC.


## Writing and reading tables

To read and write into tables that are managed by Iceberg and Nessie, typical Hive SQL queries can be used. Refer to this documentation [here](https://iceberg.apache.org/docs/latest/hive/#dml-commands) for more information.

**Note**: Hive doesn't support the notation of `<table>@<branch>`, therefore everytime you want to execute against a specific branch, you will need to set this property to point to the working branch, e.g: `SET iceberg.catalog.<catalog_name>.ref=main`. E.g:
```
SET iceberg.catalog.<catalog_name>.ref=dev

SELECT * FROM database_a.table_a;
```
