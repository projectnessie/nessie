# Overview

Nessie works seamlessly with Iceberg in Spark3. Nessie is implemented as a custom Iceberg
[catalog](https://iceberg.apache.org/docs/latest/custom-catalog/) and therefore supports all features available to any Iceberg
client. This includes Spark structured streaming, Presto, Trino, Flink and Hive. See the [Iceberg docs](https://iceberg.apache.org/docs/latest/)
for more info. Current Nessie Iceberg integration includes 
the following:

- [Spark via Iceberg](spark.md) 
- [Flink via Iceberg](flink.md)
- [Hive via Iceberg](hive.md)
