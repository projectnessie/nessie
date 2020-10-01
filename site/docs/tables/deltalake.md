# Delta Lake

Delta Lake is a table format open-sourced under the Apache License. It provides several benefits including:

* Single table ACID transactions
* Scalable metadata
* Appends, deletes, updates and merges via file re-statements

Delta Lake is relatively Spark-centric. It does expose tables via manifests for tools 
that are not Delta Lake enabled and there 
are [libraries for other tools](https://github.com/delta-io/connectors)[^1] but the 
core library relies heavily on Spark.

When using Nessie as the backing store for Delta Lake there are no longer restrictions on which types of filesystems/blob stores can be
written to. When using Nessie you can write to Delta Lake regardless of the filesystem or number 
of writers.

## Client Integration Points
Nessie provides a custom LogStore implementation for Delta Lake. Additionally, Nessie 
currently requires a small change to core Delta Lake code to enable use of Nessie. 
Without Nessie, Delta Lake normally maintains a single consistent version history through 
the use of a custom naming scheme within a known directory. While this works for one 
version history, with multiple additional work is required. As such, Nessie introduces 
a new abstraction that allows multiple file naming schemes thus enabling
 multiple version of the same dataset (with separate histories) to exist simultaneously. 
This is done by adding an extension point to the `LogFileHandler` interface.  
  
## Server Integration Points
There is a [plan](https://github.com/projectnessie/nessie/issues/123) for Nessie to run table 
management tasks related to Delta Lake for manifest generation. This would expose manifests 
for selected branches and or tags that are maintained as references in HMS. 
This would target situations where the consumption tool doesn't have Delta Lake and Nessie libraries. 
For example, this would enable AWS Athena to consume Nessie-versioned Delta Lake tables via AWS Glue.

[^1]: These libraries look to be unmaintained and leverage old versions of Delta Lake.

