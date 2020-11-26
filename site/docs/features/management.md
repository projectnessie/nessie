# Management Services

Nessie can and needs to manage several operations within your data lake. Each management 
service can be scheduled and Nessie reports the outcome of each scheduled operation. 
Scheduled operations require that Nessie have access to a Spark cluster to complete 
those operations and many of them are distributed compute operations. 

!!! info
    Table management services are currently in progress and are not yet included in a released version of Nessie.

## Garbage Collection

Since Nessie is maintaining many different versions of metadata and data-pointers simultaneously, 
you must rely on Nessie to clean up old data. Nessie calls this garbage collection. 

Garbage collection policies are configurable and can be either destructive or instructive. 
To configure garbage collection, you must set the following settings:

### Collection Types

|Collection|Modes|Notes|
|-|-|-|
|Unreferenced Data|Instructive or Destructive|Deletes any objects that haven't been **directly** referenced by a tag or branch for 30 days.|
|Unreferenced Commit|Deletes internal Nessie objects that are no longer referenced|Destructive|
|Unreferenced Metadata|Deletes metadata objects that are no longer referenced[^1]|
|Referenced Commit & Metadata|Deletes historical commit information that is in a valid commit tree but older than a certain date|

!!! danger
    Because of the nature of data sizes, Nessie treats any historical commit as "unreferenced" 
    if it is not **directly** referenced by a tag or branch. **This is very different than traditional Git**. 
    If you want to also maintain historical versions of data beyond your garbage collection time, 
    you should leverage Time-based AutoTagging (below).

## Time-based AutoTagging

Nessie works against data based on a commit timeline. In many situations, it is useful 
to capture historical versions of data for analysis or comparison purposes. As such, 
you can configure Nessie to AutoTag (and auto-delete) using a timestamp based naming scheme. 
When enabled, Nessie will automatically generate and maintain tags based on time 
so that users can refer to historical data using timestamps as opposed to commits. 
This also works hand-in-hand with the Nessie garbage collection process by ensuring 
that older data is "referenced" and thus available for historical analysis.

Currently there is one AutoTagging policy. By default, it creates the following tags:

* Hourly tags for the last 25 hours
* Daily tags for the last 8 days
* Weekly tags for the last 6 weeks
* Monthly tags for the last 13 months
* Yearly tags for the last 3 years

Tags are automatically named using a `date/` prefix and a zero-extended underscore based naming scheme. 
For example: `date/2019_09_07_15_50` would be a tag for August 7, 2019 at 3:50pm. 

!!! warning
    AutoTags are automatically deleted once the policy rolls-over. As such, if retention is desired post roll-over, manual tags should be created.

AutoTagging is currently done based on the UTC roll-over of each item.

## Manifest Reorganization

Rewrites the manifests associated with a table so that manifest files are organized 
around partitions. This extends on the ideas in the Iceberg [`RewriteManifestsAction`](http://iceberg.apache.org/javadoc/0.8.0-incubating/org/apache/iceberg/actions/RewriteManifestsAction.html). 

!!! note
    Manifest reorganization will show up as a commit, like any other table operation.

Key configuration parameters:

|Name|Default|Meaning|
|-|-|-|
|effort|medium|How much rewriting is allowed to achieve the goals|
|target manifest size|8mb|What is the target|
|partition priority|medium|How important achieving partition-oriented manifests.|
 
## Compaction

Because operations against table formats are done at the file level, a table can start 
to generate many small files. These small files will slow consumption. As such, Nessie 
can automatically run jobs to compact tables to ensure a consistent level of performance.
 
|Name|Default|Meaning|
|-|-|-|
|Maximum Small Files|10.0|Maximum number of small files as a ratio to large files|
|Maximum Delete Files|10.0|Maximum number of delete tombstones as a ratio to other files before merging the tombstones into a consolidated file|
|Small File Size|100mb|Size of file before it is considered small|
|Target Rewrite Size|256mb|The target size for splittable units when rewriting data.|

!!! note
    Compaction will show up as a commit, like any other table operation.

[^1]: This operation is similar to the [Remove Orphan Files](http://iceberg.apache.org/javadoc/0.8.0-incubating/org/apache/iceberg/actions/RemoveOrphanFilesAction.html) 
iceberg operation.
