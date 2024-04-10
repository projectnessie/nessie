---
title: "Iceberg Views"
---

# Apache Iceberg Views

Nessie supports versioning Iceberg views. A view is composed of the following properties:

* Metadata Location
* Version ID
* Schema ID
* SQL Text
* Dialect (such as Hive, Spark, Dremio, Presto) 

This enables Iceberg views to be versioned along with underlying datasets to provide a 
complete place for logical and physical experimentation. Because SQL dialects differ 
by system, Nessie does not parse or understand SQL. It relies on the creator of SQL statements 
to validate the provided SQL before being stored in Nessie.

Additional information about Iceberg Views can be found in the [View Spec](https://iceberg.apache.org/view-spec/)
