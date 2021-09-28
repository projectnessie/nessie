# Overview

Delta Lake support in Nessie requires some minor modifications to the core Delta libraries. This patch is still ongoing,
in the meantime Nessie will not work on Databricks and must be used with the open source Delta. Nessie is able to interact with Delta Lake by implementing a
custom version of Delta's LogStore [interface](https://github.com/delta-io/delta/blob/master/core/src/main/scala/io/delta/storage/LogStore.java).
This ensures that all filesystem changes are recorded by Nessie as commits. The benefit of this approach is the core
ACID primitives are handled by Nessie. The limitations around [concurrency](https://docs.delta.io/latest/delta-storage.html)
that Delta would normally have are removed, any number of readers and writers can simultaneously interact with a Nessie
managed Delta Lake table. Current Nessie Delta Lake integration include 
the following:

- [Spark via Delta Lake](spark.md)
