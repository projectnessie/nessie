# About Nessie

Nessie is to Data Lakes what git is to source code repositories. Therefore,
Nessie uses many terms from both git and data lakes.

This page explains how Nessie makes working with data in data lakes much easier
without requiring much prior knowledge of either git or data lakes.

Nessie is designed to give users an always-consistent view of their data
across all involved data sets (tables). Changes to your data, for example
from batch jobs, happen independently and are completely isolated. Users will
not see any incomplete changes. Once all the changes are done, all the changes
can be atomically and consistently applied and become visible to your users.

Nessie completely eliminates the hard and often manual work required to keep track
of the individual data files. Nessie knows which data files are being used
and which data files can safely be deleted.

Production, staging and development environments can use the same data lake without risking
the consistent state of production data.

Nessie does not copy your data, instead it references the existing data, which
works fine, because data files[^1] are immutable.

## Nessie 101

* Changes to the contents of the data lake are
  [recorded in Nessie](#working-with-data-in-nessie) as *commits* without copying
  the actual data.
* Add [meanings to the changes](#commit-messages-and-more) to your data lake.
* [Always-consistent](#branches) view of all the data 
* Sets of changes, like the whole [work of a distributed Spark job](#working-branches-for-analytics-jobs).
  or [experiments of data engineers](#working-branches-for-humans) are
  isolated in Nessie via *branches*. Failed jobs do not add additional harm to the data.
* Known, fixed versions of all data can be [tagged](#tags).
* Automatic removal of unused data files ([garbage collection](#garbage-collection)).

## Data Lake 101

> *"A data lake is a system or repository of data stored in its natural/raw format,
usually object blobs or files."* (cite from [Wikipedia](https://en.wikipedia.org/wiki/Data_lake))

Data is stored in immutable data files[^1]. Each data file defines the schema of the
data (i.e. names and types of the columns) and contains the data. A single,
logical table (for example a `customers` or a `bank_account_transactions` table)
consists of many data files.

New data files can be added to the set of files for a particular table.
Data files can also contain updates to and deletions of existing data.

Since data lakes contain the history of all that data, it is possible to
perform queries against the state of the data as it was at a point in time
in the past.

The amount of data held in data lakes is rather huge (GBs, TBs, PBs), and so
is the number of tables and data files (100s of thousands, millions).

Managing that amount of data and data files while keeping track of schema
changes, for example adding or removing a column, changing a column's type,
renaming a column in a table ("PDS", physical datasets) and views ("VDS",
virtual datasets), is one of the things that Nessie tackles.

Data in a data lake is usually consumed and written using tools like
Apache [Hive](https://hive.apache.org)[^2] or Apache
[Spark](https://spark.apache.org)[^2]. Your existing jobs can easily integrate
Nessie without any production code changes, it's a simple configuration change.

## git 101

> *"Git is a free and open source distributed version control system designed to 
handle everything from small to very large projects with speed and efficiency"*
(cite from [git-scm.com](https://git-scm.com/))

git maintains the history or all changes of a software project from the very
first *commit* until the current state.

git is used by humans, i.e. developers.

Many of the concepts of git for source code are implemented by Nessie for all
the data your data lake. It would be rather confusing to explain all git
concepts here and then outline the differences in the next chapter.

## Working with data in Nessie

Each individual state in Nessie is defined by a *Nessie commit*.
Each *commit* in Nessie, except the very first one, has references to its
predecessors, the previous versions of the data.

Each *Nessie commit* also "knows" about the data files in your data lake, which
represent the state of all data in all tables.

The following example illustrates that our *current commit* adds a 3rd data file.
The other two data files 1+2 have been added by *previous commit*.
```
 +-------------------+       +-------------------------+
 |  previous commit  | --<-- |     current commit      |
 +-------------------+       +-------------------------+
     |         |                 |        |        |
   (add)     (add)               |        |      (add)
     |         |                 |        |        |
  +------+  +------+          +------+ +------+ +------+
  | data |  | data |          | data | | data | | data |
  | file |  | file |          | file | | file | | file |
  | #1   |  | #2   |          | #1   | | #2   | | #3   |
  |     _|  |     _|          |     _| |     _| |     _|
  |  __/    |  __/            |  __/   |  __/   |  __/  
  |_/       |_/               |_/      |_/      |_/     
```
In "relational SQL" you can think of the following sequence of SQL statements:
```SQL
BEGIN TRANSACTION;
  -- The data for data file #1
  INSERT INTO table_one (...) VALUES (...);
  -- The data for data file #2
  INSERT INTO other_table (...) VALUES (...);
-- creates our "previous commit"
COMMIT TRANSACTION;

BEGIN TRANSACTION;
  -- Data added to 'table_one' will "land" in a new data file #3, because
  -- data files are immutable.
  INSERT INTO table_one (...) VALUES (...);
-- Creates our "current commit"
COMMIT TRANSACTION; 
```

Each commit is identified by a sequence of hexadecimal characters like
`2898591840e992ec5a7d5c811c58c8b42a8e0d0914f86a37badbeedeadaffe`, which is not
easy to read and remember for us humans.

### Branches

Nessie uses the concept of "branches" to always reference the *latest* version
in a chain of commits. Our example branch is named "master" and has just
a single commit:
```
 +-------------+
 |  commit #1  |
 +-------------+
        ^
        |
        |
     "master"
      branch
```
When we add changes to our "master" branch, a new `commit #2` will be created:

* the new `commit #2` will reference `commit #1` as its predecessor and
* the *named reference* "master" will be updated to point to our new `commit #2`

```
 +-------------+       +-------------+
 |  commit #1  | --<-- |  commit #2  |
 +-------------+       +-------------+
                              ^
                              |
                              |
                           "master"
                            branch
```
This behavior ensures that the *named reference* "master" always points to the
very latest version of our data.

### Working-branches for analytics jobs

The above example with a single branch works well, if all changes to all tables
can be grouped into a single commit. In a distributed world, computational work
is distributed across many machines running many processes. All these individual
tasks generate commits, but only the "sum" of all commits from all the tasks
represents a consistent state.

If all the tasks of a job would directly commit onto our "master" branch, the
"master" branch would be *inconsistent* at least until not all tasks have finished.
Further, if the whole job fails, it would be hard to rollback the changes, especially
if other jobs are running. Last but not least, the "master" branch would contain a
lot of commits (for example `job#213, task#47346, add 1234 rows to table x`), which
do not make a lot of sense on their own, but a single commit (for example
`aggregate-financial-stuff 2020/12/24`) would.

To get around that issue, jobs can create a new "work"-branch when they start.
The results from all tasks of a job are recorded as individual commits into that
"work"-branch. Once the job has finished, all changes are then merged into the 
"master" branch at once.
```
    "work"
    branch
      |
      |
      v
+-----------+ 
| commit #1 | 
+-----------+ 
      ^
      |
      |
   "master"
    branch
```
Our example Spark job has two tasks, each generates a separate commit, which are only
visible on our "work"-branch:
```
          task#1         task#2   "work"
          result         result   branch
            |                |     |
            v                v     v
      +-----------+       +-----------+
      | commit #2 | --<-- | commit #3 |
      +-----------+       +-----------+
         |
         v
         |
+-----------+ 
| commit #1 | 
+-----------+ 
      ^
      |
      |
   "master"
    branch
```
When the job has finished, you can merge the now consistent result back
into the "master"-branch.
```
          task#1         task#2   "work"
          result         result   branch
            |                |     |
            v                v     v
      +-----------+       +-----------+
      | commit #2 | --<-- | commit #3 |
      +-----------+       +-----------+
         |                          |        
         v                          ^
         |                          |
+-----------+                     +-----------+
| commit #1 | --------<---------- | commit #4 |  
+-----------+                     +-----------+
                                      ^
                                      |
                                      |
                                   "master"
                                    branch
```
Note that the merge-`commit #4` has references to two commits:

* the previous commit on the "master" branch and
* the merged commit from the "work"-branch

It would be nice to give that `commit #4` a meaningful commit message, like
`aggregate-financial-stuff 2020/12/24`, though, so people that look through the
history of the data can grasp what that commit changes and why it's there.

Before you actually merge the data from a working-branch into the "master" branch,
it is possible to review all the changes made by your job - either programmatically,
like running some process that validates the state of the branch, or asking a human
to take a look.

### Working branches for "humans"

You can also use "developer" branches to run experiments against your data, test
changes of your jobs etc.

Production, staging and development environments can use the same data lake without risking
the consistent state of production data.

### Squashing commits

Nessie can also help to reduce the amount of data files in a data lake.

You can for example merge all data files produces by all tasks of an analytics job
before the whole change is actually merged into the "master" branch. Nessie will
[figure out](#garbage-collection) that only the merged files need to survive and
remove the unused files.

### Tags

Another type of named references are *tags*. Nessie *tags* are named references
to specific commits but unlike branches do not get automatically "moved".

*Tags* are useful to reference specific commits, for example a tag named
`financial-data-of-FY2021` could reference all sources of financial data relevant
used for some financial year report.

### Commit messages and more

As briefly mentioned above, every commit in Nessie has a set of attributes.
Some of the more important ones are "summary" and "description", which are exactly
that - meaningful summaries and detailed descriptions that explain what has been
changed and why it has been changed. This gives data engineers the ability to know
why something has changed.

In addition to "summary" and "description", there are a bunch of additional attributes:

| Attribute | Meaning
| --- | ---
| commit timestamp | the timestamp when the commit was recorded in Nessie
| committer | the one (human user, system id) that actually recorded the change in Nessie
| author timestamp | the timestamp when a change has been implemented (can be different from the commit timestamp)
| author | the one (human user, system id) that authored the change, can be different if someone else actually commits the change to Nessie
| summary | one-line meaningful summary of the changes
| description | potentially long description of the changes
| ... | there are potentially way more attributes, just too many to mention here

## Garbage collection

Data lakes contain a lot of data. The amount of data has a direct relation to
the cost of ownership of a data lake. Keeping all data forever is probably going
to be just too expensive, practically not useful and can also collide with data
privacy regulations (for example GDPR or CCPA).

Nessie keeps track of unused data files and collects the garbage for you.

## Terms summary

| Term | Meaning in Nessie
| --- | ---
| Commit | An atomic change to a set of data files.
| (Multi-table) transaction | Since a Nessie commit can group data data files from many tables, you can think of a Nessie commit as a (multi-table) transaction.
| Branch | Named reference to a commit. A new commit to a branch updates the branch to the new commit.
| Tag | Named reference to a commit. Not automatically changed.
| Merge | Combination of two commits. Usually applies the changes of one source-branch onto another target-branch. Creates a merge-commit.

## Footnotes

[^1]: Common data file formats are [Apache Iceberg Tables](../tables/iceberg.md),
  [Delta Lake Tables](../tables/deltalake.md), [Hive Metastore Tables](../tables/hive.md)

[^2]: Apache, Hive, Spark, Iceberg, Parquet are trademarks of The Apache Software Foundation.
