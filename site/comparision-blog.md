# Iceberg vs Delta Lake vs Hudi blog

## Intro

## ACID transactions on Data Lake and why its important

* all are tightly coupled to Hadoop and more so Spark
* in a lot of cases it removes the need for a DW, simplifies Architecture.
* started w/ things like kudu and hive acid but too monolithic, not suitable for cloud data lakes
Earlier attempts to do ACID transactions on a data lake
required a heavy weight and expensive metastore (eg Transactional Hive),
no extra infra needed for any
discuss the dimensions on which we are comparing

## Architecture and usage of Delta Lake

Delta Lake was introduced in #todo by Databricks. It was open sourced under the Linux foundation in 2019. As per [its
website](https://delta.io) its 'Delta Lake is an open source storage layer that brings reliability to data lakes. Delta
Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. Delta Lake
runs on top of your existing data lake and is fully compatible with Apache Spark APIs'. Its primary attraction is its
deep integration in Apache Spark and Databricks. It is the newest project but already is relatively mature and very
actively used. 

### Architecture

The two key architectural points in Delta are its delta based transaction log and its copy-on-write mechanism for
inserts. Like the other technologies Delta stores all metadata along side the data, this simplifies metadata storage and
reading and means a central metadata repository is not required. The delta based transaction log means that each commit
is stored in an immutable file in a subdirectory of the table on the Data Lake (typically `_delta_store`). These files
are 20 character long 0 padded sequential integers (eg #todo). They are all json documents and store the actions
performed in a given transaction. The sequential format appears restrictive and indeed if a file in the sequence is
missing or corrupt the table would be unreadable. However, Delta goes to great lengths to ensure this doesn't happen and
as discussed below the metadata format is what guarantees atomic and ordered transactions. The log deltas are
periodically compacted (by default every 10 commits) into a 'checkpoint' file. Like the snapshot files these are
write-once and immutable. They collapse the entire history of a table down to a single file to avoid having to scan
thousands of small transaction log files.

The copy on write mechanism that Delta uses stipulates that any individual file affected by an update or row level
deletes is completely re-written at commit time. This keeps all data files immutable and simplifies work for the reader
at the cost of more storage being used and data files that are no longer accessible/valid being left hanging around.
Delta requires regular cleaning via the `VACUUM` command to delete any data files that are no longer referenced. For
example if we perform an insert to create file A then update a few rows of that same file we will write a new file
called A' while leaving A. At some point we will have to delete file A via a `VACUUM` operation as it no longer has up
to date data.

Finally, Delta is tightly bound to Spark. This has advantages and disadvantages: the spark reader and writer are
extremely mature and well integrated into Spark, which makes for an ergonomic user experience. The disadvantage however
is that it makes it hard to read and write Delta tables from other tools. The current [Delta documentation](#todo spec)
refers to it as a reference implementation, hopefully this means an implementation separate to Spark can be built by the
community.

### Concurrency

As mentioned previously how a library handles concurrency is the single most important aspect to it being able to handle
ACID transactions. The Delta library handles it by performing atomic `rename`s on metadata files. First Delta will write
all data eagerly to the Data Lake. Its copy on write semantics and its metadata format ensures that readers do not see
any altered data at this stage. Next it will write a metadata file to a staging name. Finally the commit happens when an
atomic rename occurs to the final metadata file name, which is the next integer in the sequence as described above. If
the rename fails, which usually happens because the metadata file already exists the writer aborts the commit. This
doesn't directly mean the commit fails, the writer can try to read new metadata and reapply the commit to try again. If
this still fails the transaction fails.

This model allows for multiple concurrent writers who can perform transactions (updates, inserts, deletes and schema
changes) in parallel on a Delta table. The caveat however is that the underlying Data Lake or filesystem supports strong
consistency and atomic renames. Thankfully this is the case for standard local filesystems, HDFS (#todo link) and Azure
(#todo link). Unfortunately AWS S3 is in most cases eventually consistent(#todo link), this means that AWS is strictly
limited to a single concurrent writer, multiple concurrent writers could result in inadvertent data loss.

The story for Delta readers is similar: a Delta reader on most file systems will immediately see any commited updates
but a reader reading from S3 can in theory not see the most recent commit. The Delta protocol ensures that the reader
will still see a consistent view of data and not partial updates and recent studies (#todo) have shown that while S3 is
theoretically eventually consistent it doesn't manifest itself regularly in practice.

### Writing

As mentioned Delta can support multiple writers in most cases. This is particularly useful if a table is being written
to by more than one Spark cluster, especially for streaming use cases. The Spark writer is a DatasourceV2(#link) writer
that works for both batch and structured streaming use cases. This gives a fair bit of flexibility to the Data Engineer
for how to add data to Delta. An interesting feature of Delta's streaming component is the checkpoint action that Delta
can write to the commit log. This records the information required for a Spark Streaming job to ensure exactly-once
processing, even in the face of multiple writers.

Delta supports the following actions when writing: (#todo links)

* insert - add data to a Delta table via batch processing
* append - append data via stream processing
* overwrite - overwrite all data in a table via batch processing
* complete - re-write the entire table every batch for stream processing. Primarily useful if computing summary
  statistics
* update - batch only operation which modifies individual records based on a predicate.
* merge - a batch only operation that mixes insert and update. Effectively an 'upsert'
* delete - removes records from a table (batch only). This does not immediately delete the data on disk. The data is
  still accessibel via 'time-travel' and will only be deleted when the old versions are vacuumed.

The copy on write semantics of Delta means that for updates, merges and deletes entire new data files are written. The
trade off here is that the writing Spark instance will have to fetch all data involved and rewrite entire
files/partitions. More data is left on disk until the next vacuum operation and writes potentially take longer. This
means that all reads are optimised and only have to read the requested data without merges or filtering.

Delta also allows schema migration commands on write. Currently only adding new columns or re-ordering columns is
supported. To do more sophisitcated schema updates: re-partition, change column types etc the entire table has to be
re-written. 

Finally, a bonus feature is adding metadata to a commit. This allows storage of commit provenance, commit owner etc to
be stored alongside the rest of the table metadata.

### Reading

The primary way to read Delta tables is via its Spark DataSource V2 reader. This exposes the table to Spark SQL and to
the dataframe based PySpark and Scala readers. This provides a flexible and clean interface for reading from spark. The
serializable isolation guarantee that Delta provides ensures that any reads through this method will be consistent.
Spark based Delta readers also have the option to 'time-travel'. This allows a client to read a different snapshot based
on time or snapshot id. This can fix results in time so all reads are identical and it can also provide interesting
opportunities to eg compare what changed between dates or versions. By default seven days of history is kept however
this can be configured by the user.

With respect to performance, when reading the Delta libraries make use of partitions and table stats to aggressively
filter the files which need to be read. The actual reading is delegated to Spark so read speeds will be similar to what
Spark can do on an optimised table. 

While Spark integration is excellent for Delta, readers from different applications have to rely on Hive's symlink
format (#todo link). This updates the Hive metastore with a list of files that should be read for a specific Delta
snapshot. This action has to be done whenever an external reader wants to read new Delta data. Once the manifest symlink
is available in the metastore Hive, Presto, Snowflake, Redshift and any other process that can read symlink manifests
from the Hive metastore can read the given Delta table.

### Maintenance & Usability

The primary maintenance concern for Delta is the `VACUUM` command. This needs to be run regularly to ensure a table
doesn't have too many hanging files lying around. This doesn't affect read speed but does leave a lot of unused files on
the Data Lake. There are some important [caveats](#todo) around running vacuum to regularly. The only other
consideration with the regular operation of a Delta table is to ensure compaction jobs are run. These reduce the number
of small files and optimise the table for reading. Compactions are run as [Spark jobs](#todo). As previously mentioned
the Delta libraries are rather Spark-centric so all maintenance and utilities have to be run directly in Spark. 

### Overall

The Delta library is an good choice if you are already a heavy Spark user or use Databricks. Despite being the newest
library available it already has a strong user base and is relatively mature. Hopefully as it matures it will become
more usable from a broader toolset, especially for the maintenance jobs.

## Architecture and useage of Hudi

Hudi has been around since (#todo) and is the oldest of the libraries discussed today. It originally was developed at
Uber to solve some very specific performance issues on their very large data lake. Since it has been donated to the
Apache Foundation and is now a full-fledged apache project. From their website 'Hudi brings stream processing to big
data, providing fresh data while being an order of magnitude efficient over traditional batch processing'. Hudi really
excels at upserts from streaming use cases and reading transaction logs from operational databases. This is in
comparison to the other libraries which we are likely to see in predominantly append-only use cases. As the most mature
library being discussed its also has the largest feature set and nubmer of integrations.

### Architecture

As previously stated the Hudi architecture is primarily driven by its desire to deliver high performance for streaming,
update heavy workloads. Given its relative age its also the most hadoop-oriented of the libraries, this can be seen in
how it is coupled to the hdfs IO libraries and its tighter integration with Hive. Unlike Iceberg or Delta, Hudi offers
two table types: copy-on-write tables and merge-on-read tables. It is also the only library to offer indexing in the
style of traditional RDBMSs.

The copy on write table is similar to Delta in that it reads the current data for a table (or partition) into memory
performs updates to that table in memory. The data is then written back out as an entirely new file. All data files 
written by Hudi are immutable and the freshly written files will not clash with the previous data files. Writes are
consequently longer but this makes reads very efficient. Just as for Delta this requires regular cleaning.

Merge on read tables are different, they write any updates to a Avro based row-oriented log file on write. At read time
the records from the log file are combined with the original, immutable data files. This allows for very fast writes at
the cost of longer reads. Merge on read tables have two versions in Hudi: the read optimised view, which ignores any
updates in the log and presents the user with read-optimised data but is out of date. It also has the full view which
shows all data with slower reads. Regular compaction is required to flatten the row based log file and consequently
produce a new read-optimised view.

Both table types support time-travel in a similar fashion as Delta, however the Hudi libraries can also present
incremental views which show what has changed since a time `t`. This is particularly useful in streaming ETL jobs.

These table types and read functions are all supported by a delta based commit log. This log is stored next to the data
and contains a record of all changes to the Hudi table. The commits are stored as 1 file per action and the filenames
are the timestamp of the start of the transaction. As we will see below this has interesting ramifications on Hudi's
concurrency guarantees.

Finally, a unique feature of Hudi is its in-built index. The index stores a mapping of the Hudi key to a filename. The
Hudi key is a tuple of the partition id and a record key. The advantage of this system is it can be used to drastically
reduce the number of files needed on a read. The downside though is that a table has to have a user supplied unique
(per-partition), monotonically increasing index field. The index can be stored on disk or in HBase if you are in a
Hadoop environment.

### Concurrency

As stated above the Hudi transaction log is a set of time ordered files which define which transactions happened and in
what order. This transaction log relies on the atomicity of the underlying filesystem. As such we can guarantee that
local filesystems, ADLS and HDFS will all have atomic, instantly visible transactions. Similar to Delta the caveat that
S3 is eventually consistent requires extra handling on the part of Hudi. Unlike Delta and Iceberg, Hudi is lilmited to a
single concurrent writer. This restriction is because the commit log is time based and there is no way to ensure that
the timestamp currently visible to the writer is actually the most up to date timestamp across many potential writers.

The Hudi library may not see the most recent commit if being used on S3, another important reason to restrict Hudi to
single writers. It also means a reader may not see the most up to date version of the data, however it is not likely to
produce any major problems under normal workloads (#todo see Delta and links)

### Writing

The Hudi library should only be used in a single writer configuration. When this is true the transactions are guaranteed
to be ACID in nature. Unlike the other libraries Hudi has some interesting tools to assist in writing new data. Data can
be written from Spark using a DataSourceV1 writer or the `DeltaStreamer` tool can be used. DeltaStreamer is able to
connect to many different sources: Kafka (optionally with schema registry support), the Data lake, transaction logs from
other databases amongst others. For streaming sources it has an excatly-once guarantee. This process handles
checkpoints, rollback, optimal file size and a number of other rather useful options to simplify ingesting data into the
Data Lake. In reality the `DeltaStreamer` is a wrapper around spark-submit to create a Spark job which ingests the data
using the Hudi spark writer. 

Hudi supports the following write actions:

* insert - simple insert. This needs to see all data being written and optimises for file size and read patterns
* upsert - similar to insert but performs updates on relevant records. This requires that each record in a partition has
  a unique record id
* bulk-update - useful for bootstrapping a table. This ignores data size and partition patterns and writes large data
  sets quickly. However does not try ot optimise for reads. It is likely that data may need repartitioning or compaction
  after a bulk update

These actions apply to both table types and to streaming or batch updates. The Hudi library really prioritises the
upsert functionality and boasts high performance writes in streaming use cases. Its merge-on-read semantics and ability
to query only what has changed (effectively only the update log file) make it particularly well suited to streaming data
with late or out of order arrivals.


### Reading

The Hudi library can automatically expose a table to Hive making it available for querying in Hive or to any system that
can read via the Hive metastore (eg Snowflake, Presto etc). It will always have the most up to date data available to
Hive's metastore. Aside from that the Spark connector is the primary way to read Hudi data. The various types of tables
and the variety of query types gives the user a lot of flexibility for how and what they want to read. The user can read
the current metadata for copy-on-write tables and merge-on-read tables or it can read the read-optimised part of a
merge-on-read table. It can also read past table versions at specific points in time or it can read what has changed
since a given timestamp. As previously mentioned the 'what happened since' statements are quite powerful for streaming
ETL and finding what has changed in the table since a given time.

Regarding performance, Hudi limits the number of files that need to be read by using the built in index and taking
advantage of partition information. This will ensure read performance, especially for copy on write tables, is
performant.


### Maintenance and Usability

As expected the oldest of the libraries has the best and most mature tools for maintenance and usability. It has a
fairly complete CLI from which one can start compaction jobs, partition migrations and several other tools. These jobs
all get translated eventually into Spark jobs so while the CLI is super convenient it still requires Spark. It is also
the only library with automated tooling to ingest from Kafka clusters and external tools like Sqoop.

The merge-on-delete table type especially requires regular compaction to remove the Avro based update log and the
copy-on-write tables require vacuuming to remove stale files form time to time. Both these jobs can be started from the
Hudi CLI.

### Overall

The primary use case for Hudi is for streaming upserts. The library has been heavily optimised to support this use case.
As of this writing Hudi does not yet support Spark 3.0 or Hadoop 3.0 and it appears to move slower than the new projects
with upgrading to these new libraries. It is also the only library that doesn't support multiple writers. Despite these
limitations it still has the most advanced ecosystem and is by far the most battle tested of the libraries.

## Architecture and usage of Iceberg

### Architecture

* snapshot based metadata files
* strong Schema evolution and partition support

### Concurrency

* requires atomic FS or external store (Hive locking tables)

### Writing

* multiple writers
* merge on read

### Reading

* Hive, Spark, Presto
* time travel
* stats stored in metadata + partitioning allows for aggressive pruning
* generic Arrow based reader means less reliance on readers for Hive/Spark
* a python reader/writer w/o spark/hive

### Maintenance and Usability

* fewer utilities and less mature
* work being done in this area

## Future of ACID on DL

* multi table


## Important features

* atomicity of transactions and r/w concurrency
* updates/row level transactions
* streaming
* read vs write
* Schema evolution
* maintenance
* optimisations
* performance
