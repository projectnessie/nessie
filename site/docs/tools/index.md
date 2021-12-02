# Overview

Nessie is focused on working with the widest range of tools possible. If a tool creates 
or reads data, Nessie seeks to work with it. Current Nessie integrations/tools include 
the following:

- [Iceberg Integration](iceberg/index.md)
    - [Spark via Iceberg](iceberg/spark.md)
    - [Flink via Iceberg](iceberg/flink.md)
    - [Hive via Iceberg](iceberg/hive.md)

- [Delta Lake Integration](deltalake/index.md)
    - [Spark via Delta Lake](deltalake/spark.md)

- [Nessie CLI](cli.md)
- [Nessie Web UI](ui.md)
- [Authentication in Tools](auth_config.md)
- [Nessie Spark SQL Extensions](sql.md)


## Feature Matrix

| |Spark 2 [^1]|Spark 3[^2]|[Nessie CLI](cli.md)|Flink|
| --- | --- | --- | --- | --- |
|Read Default Branch|:material-check:| :material-check: | |:material-check:|
|Read Any Branch/Tag/Hash|:material-check:| :material-check: | |:material-check:|
|Write Default Branch|:material-check:| :material-check: | |:material-check:|
|Write Any Branch/Tag/Hash|:material-check:| :material-check: | |:material-check:|
|Create Branch| :material-check: | :material-check: | :material-check:|:material-check:|
|Create Tag| :material-check: | :material-check: |:material-check:|:material-check:|
|Iceberg Tables|:material-check:|:material-check:| |:material-check:|
|Delta Lake Tables||:material-check:| | |

[^1]: Spark 2 currently only supports access via the Dataframe API due to weak generic 
catalog support.
[^2]: Spark 3 supports both SQL and dataframe access. Consumption can be done via existing 
Iceberg and Delta Lake catalogs with Nessie extensions or through the Nessie Catalog, 
which currently exposes both of these formats.

## Demos

The [Nessie Demos](https://github.com/projectnessie/nessie-demos) GitHub repository contains a set of demos that help users understand how Nessie works.
