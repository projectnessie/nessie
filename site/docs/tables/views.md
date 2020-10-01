# SQL Views

Nessie supports versioning SQL views. A view is composed of the following properties:

* SQL Text
* Schema (representation to be finalized, likely Avro or Arrow)
* Dialect (currently includes Hive, Spark, Dremio, Presto) 

This enables SQL views to be versioned along with underlying datasets to provide a 
complete place for logical and physical experimentation. Because SQL dialects differ 
by system, Nessie does not parse or understand SQL. It relies on the creator of SQL statements 
to validate the provided SQL before being stored in Nessie.

!!! info
    While Nessie can already store SQL Views, further work needs to be done in existing 
    systems to fully expose this functionality.

