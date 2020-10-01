# Overview

Nessie is focused on working with the widest range of tools possible. If a tool creates 
or reads data, Nessie seeks to work with it. Current Nessie integrations/tools include 
the following:

| |[Spark 2](spark.md)[^1]|[Spark 3](spark.md)[^2]|[Hive](hive.md)[^3]|AWS Athena[^4]|[Nessie CLI](cli.md)|
| - | - | - | - | - | - |
|Read Default Branch|:material-check:| :material-check: |:material-check:|:material-check:| |
|Read Any Branch/Tag/Hash|:material-check:| :material-check: |:material-check:|:material-check:| |
|Write Default Branch|:material-check:| :material-check: |:material-check:| | |
|Write Any Branch/Tag/Hash|:material-check:| :material-check: |:material-check:| | |
|Create Branch| :material-check: | :material-check: | :material-check: | |:material-check:|
|Create Tag| :material-check: | :material-check: | :material-check: | |:material-check:|
|Iceberg Tables|:material-check:|:material-check:|:material-check:| | |
|Delta Lake Tables|:material-check:|:material-check:| | | |
|Hive Tables (via HMS Bridge)|:material-check:|:material-check:|:material-check:|:material-check:|:material-check:|

[^1]: Spark 2 currently only supports access via the Dataframe API due to weak generic 
catalog support.
[^2]: Spark 3 supports both SQL and dataframe access. Consumption can be done via existing 
Iceberg and Delta Lake catalogs with Nessie extensions or through the Nessie Catalog, 
which currently exposes both of these formats.
[^3]: Hive support is provided via the HMS Bridge Service
[^4]: Athena access is made possible via the AWS Athena Lambda for Hive Metastore and 
the HMS Bridge Service.