# SQL Views

Nessie supports version SQL views. A view is composed of the following properties:

* SQL Text
* Schema (based on Apache Arrow's schema representation)
* Dialect Hint (currently includes Hive, Spark, Dremio, Presto) 

This enables SQL views to be versioned along with underlying datasets to provide a 
complete place for logical and physical experiementation. Because SQL dialects differ 
by system, 

Nessie does not parse or understand SQL. It relies on the creator of SQL statements 
to validate the provided SQL.