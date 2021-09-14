# Overview

Nessie works seamlessly with Iceberg in Spark2 and Spark3. Nessie is implemented as a custom iceberg
[catalog](http://iceberg.apache.org/custom-catalog/) and therefore supports all features available to any Iceberg
client. This includes Spark structured streaming, Presto, Flink and Hive. See the [iceberg docs](https://iceberg.apache.org)
for more info. Current Nessie Iceberg integration include 
the following:

- [Spark via Iceberg](spark.md). 