# Delta Lake

Delta Lake is a table format open-sourced under the Apache License. It provides several benefits including:

* Single table ACID transactions
* Scalable metadata
* Appends, deletes, updates and merges via file re-statements

Delta Lake is very Spark centric. While there is some work for exposing snapshots of 
Delta Lake to other tools through both manifests and 

When using Nessie as the backing store for Delta Lake there are no longer restrictions on which types of filesystems/blob stores can be
written to. When using Nessie you can write to Delta Lake regardless of the filesystem or number 
of writers.

## Client Integration Points
Nessie provide a custom LogStore implementation for Delta Lake. Additionally, Nessie 
currently requires a [small change](https://github.com/delta-io/delta/compare/v0.6.1...rymurr:hash-names?expand=1) to core Delta Lake code to enable use of Nessie. 
Without Nessie, Delta Lake normally maintains a single consistent version history through 
the use of a custom naming scheme within a known directory. While this works for one 
version history, with multiple additional work is required. As such, Nessie introduces 
a new abstraction that allows multiple file naming schemes thus enabling
 multiple version of the same dataset ()with separate histories) to exist simultaneously. 
This is done by adding an extension point to the `LogFileHandler` interface.  
  
## Server Integration Points
Nessie can be scheduled to provide branch and/or tag manifest snapshots to HMS based 
on commit operations this work is [in progress](https://github.com/projectnessie/nessie/issues/123). 
This is useful when the consumption tool doesn't have Delta Lake and Nessie libraries. 
For example, this enables AWS Athena to consume Delta Lake tables via AWS Glue.


