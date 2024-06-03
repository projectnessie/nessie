# Management Services

Nessie can and needs to manage several operations within your data lake.

Each management service can be scheduled and Nessie reports the outcome of each operation.

## Garbage Collection

Since Nessie is maintaining many versions of metadata and data-pointers simultaneously, you must
rely on Nessie to clean up old data. Users should run Nessie GC regularly.

Nessie GC needs to know which content versions need to be retained. To identify this so called
"live" content, Nessie GC uses some rules which are applied on each named reference. Those rules
are described below.

Nessie GC is composed of multiple phases:
1. **Identify** (or "mark") phase: Inspects the Nessie repository to identify all commits and
   content version (in Iceberg terms: a table's snapshot). These so-called "content references" are
   stored as a live-content-set, ideally in a separate database (see below for compatible 
   databases). This phase requires access to the Nessie repository, but does not require access to 
   the data lake.
2. **Expire** ("sweep") phase: Uses the actual table format (e.g. Iceberg) to map the content
   references from a live-content-set to a set of file-references, which are then matched against
   a recursive listing of all files for the respective tables. Files that are not contained in the
   set of file-references are going to be deleted. Deletion either happens immediately or is
   persisted in the live-content-set as a set of orphan files.
3. The **delete** phase can be split out of the **expire** phase, it basically means that orphan 
   files are first collected, so these can be inspected, and then explicitly deleted. 

All relevant operations required for Nessie GC can be run via the `nessie-gc.jar` tool, which can be
downloaded from the [release page on GitHub](https://github.com/projectnessie/nessie/releases).

!!! info
    Currently the GC algorithm only works for Iceberg tables. A supported JDBC database is 
    recommended as the storage for the live-content-sets.

!!! info
    Information about Nessie GC can be found [here](../nessie-latest/gc.md).

## Nessie GC tool

It is recommended to run all Nessie GC phases via the Nessie GC command line tool `nessie-gc.jar`,
which can be downloaded from the
[release page on GitHub](https://github.com/projectnessie/nessie/releases).

The Nessie GC tool comes as an uber-jar packaged with everything you need to run Nessie GC against
a data lake using Iceberg.

!!! note
    Use `java -jar nessie-gc.jar help` to get a list of commands supported by the Nessie GC tool.

### Setting up the database for Nessie GC

!!! note
    The Nessie GC tool is compatible with the following databases: PostgreSQL (production-ready),
    MariaDB and MySQL (experimental at the moment).

You can create the tables in two ways:

Manually: use the DDL statements emitted by `java -jar nessie-gc.jar show-sql-create-schema-script`
as a template that can be enriched with database specific optimizations.

Or alternatively, let the Nessie GC tool create the schema in your existing database, for example
like this for PostgreSQL:

```bash
java -jar nessie-gc.jar create-sql-schema \
  --jdbc-url jdbc:postgresql://127.0.0.1:5432/nessie_gc \
  --jdbc-user pguser \
  --jdbc-password mysecretpassword
```

!!! note
    Instead of specifying the JDBC parameters, especially the password, everytime on the command
    line, most command line option values can be specified via environment variables. The naming
    scheme follows this Java pseudo-code:
    `"NESSIE_GC_" + optionName.substring(2).replace('-', '_').toUpperCase()`. For example, the
    `--jdbc-password` command line option's value is taken from the environment variable
    `NESSIE_GC_JDBC_PASSWORD`.

!!! note
    The availability of the database for Nessie GC is not critical for Nessie itself. Nessie does
    not require anything from Nessie GC to continue to work.

!!! note
    For small, experimental Nessie repositories, that do not access any production data lake
    information, you can experiment with the `java -jar nessie-gc.jar gc` command, which also 
    accepts the `--inmemory` command line option, which does not require an external database for
    live-content-set persistence. In fact, the `--inmemory` option does not persist anything and
    keeps the live-content-set information in memory. The `gc` command runs the _identity_,
    _expire_ and _delete_ phases sequentially.

### Live content sets

All Nessie GC operations work on exactly one so-called "live content set". Each live content set
is composed of:

* **Unique ID** each live-content-set is identified by a UUID. The `java -jar nessie-gc.jar 
  mark-live` command emits the ID of the live-content-set to the console, but it's recommended to 
  write the new live-content-set ID to a file using the `--write-live-set-id-to` option. Other 
  commands that work on a live-content-set allow reading the ID of the live-content-set using the 
  command line option `--read-live-set-id-from`.
* **Status** tracks the state and/or progress of a live-content-set and is used to know whether
  the _identify_ and _sweep_ phases started resp. ended and whether those finished successfully
  or with an error. If, for example, the identify phase did not finish successfully, the sweep
  phase cannot be started. A summary of the error message is stored with the live-content-set.
* Timestamps of when the identify and expire phases started and completed.
* **collection of content-IDs** as the result of the _identify_ phase
* **set of content-references for each content-ID** as the result of the _identify_ phase
* **set of base-table-locations** as the result of the _sweep_ phase
* **set of file-references to be deleted** as the result of the _identify_ phase, if Nessie GC
  was told to defer deletes using the `--defer-deletes` command line option.

A couple of `nessie-gc.jar` commands allow the listing of all and inspection of individual
live-content-sets. Those are:

* `list` lists all live-content-sets, starting with the most recent live-set.
* `show` shows information about one live-content-set, optionally with details about the content
  references or base-locations or deferred deletes.
* `list-deferred` to show the file-references from a _sweep_ phase with the `--defer-deletes`
  option.
* `deferred-deletes` to delete the files referenced by file-references collected during a _sweep_
  phase with the `--defer-deletes` option.
* `delete` deletes a live-content-set.

### Running the _mark_ (or _identify_) phase: Identifying live content references

The _mark_ or _identify_ phase is run via the `mark-live` (or `identify` as an alias)
`nessie-gc.jar` command.

```shell
java -jar nessie-gc.jar mark-live \
  --jdbc... # JDBC settings omitted in this example
```

It will walk the commits in all named references, and collect all content-references from the
visited Nessie commits. So called "cut off policies" define, when the _mark_ phase should stop
walking the commit log for a named reference. The default "cut off policy" is `NONE`, which means
that _all_ Nessie commits and therefore all contents in the named references using the `NONE`
policy are considered live.

!!! note
    Since the _mark_ phase requires access to Nessie, make sure to use the `--uri` command line
    option to configure the Nessie endpoint and the `--nessie-option` command line option to
    configure additional Nessie client parameters, for example a bearer token. The Nessie
    repository is never modified by Nessie GC.

!!! note
    The _mark_ phase does not access the data lake nor does it use Iceberg.

#### Cut off policies

Nessie GC supports three types of cut off policies:

* `NONE`, not explicitly selectable via the CLI, it is the implicit default when a named reference
  has no matching policy. It means, there is no cut-off time, everything in the named reference is
  considered "live".
* by **number of commits**: The given number of most recent commits are considered live.
* by **cut off timestamp**: All commits that are younger than the configured timestamp are
  considered live.
* by **cut off duration**: Similar to _cut off timestamp_, all commits younger `now - duration` are
  considered live. In the Nessie GC tool, a duration is always converted to a timestamp using a
  common reference timestamp.

Relevant command line options for `java -jar nessie-gc.jar mark-live` (alias `java -jar 
nessie-gc.jar identify`):

* `--cutoff reference-name-regex=cut-off-policy` the specified `cut-off-policy` is applied to all
  named references that match the given reference name regular expression.
* `--cutoff-ref-time` Defaults to "now", but can also be configured to another timestamp, if
  necessary.

Cut-off policies are parsed using the following logic and precedence:

1. An integer number is translated to the cut-off-policy using **number of commits**.
2. The string representation of a `java.time.Duration` is translated to a duration. Java durations
   string representation starts with `P` followed by the duration value. Examples:
    * `P10D` means 10 days
    * `PT10H` means 10 hours
3. The string representation of a `java.time.format.DateTimeFormatter.ISO_INSTANT` is translated
   to an exact cut-off-timestamp. Example using UTC: `2011-12-03T10:15:30Z`

!!! note
    Nessie GC's _mark_ phase processes up to 4 named references in parallel. This setting can be
    changed using the `--identify-parallelism` command line option.

### Running the _sweep_ (or _expire_) phase: Identifying live content references

Nessie GC's sweep phase uses the actual table format, for example Iceberg, to map the collected
live content references to live file references. The _sweep_ phase operates on each content-ID. So
it collects the live file references for each content ID. Those file references refer to Iceberg
assets:

* metadata files
* manifest lists
* manifest files
* data files

After the _expire_ phase identified the live file references for a content-ID, it collects all files
in the base table locations. While traversing the base locations, it collects the files that are
definitely _not_ live file references. Those non-live file references are then deleted, aka
immediate orphan files deletion.

As an alternative, the _expire_ phase can just record the orphan files instead of immediately
deleting those. This is called _deferred deletion_ in Nessie GC.

Configuration options for Iceberg and Hadoop can be specified using the `--iceberg` and `--hadoop`
options. Examples: `--iceberg s3.access-key-id=S3_ACCESS_KEY` and
`--hadoop fs.s3a.access.key=S3_ACCESS_KEY`.

Example of running the _expire_ command follows.

```shell
java -jar nessie-gc.jar expire --live-set-id 0baaa1ff-90db-4ee5-b6d2-b60aea148c76 \
  --jdbc... # JDBC settings omitted in this example
```

Example of running an _expire_ with _deferred deletion_:

```shell
java -jar nessie-gc.jar expire --live-set-id 0baaa1ff-90db-4ee5-b6d2-b60aea148c76 \
  --defer-deletes \
  --jdbc... # JDBC settings omitted in this example

# You can inspect the files to be deleted this way ...
java -jar nessie-gc.jar list-deferred --live-set-id 0baaa1ff-90db-4ee5-b6d2-b60aea148c76 \
  --jdbc... # JDBC settings omitted in this example

# ... or this way
java -jar nessie-gc.jar show --live-set-id 0baaa1ff-90db-4ee5-b6d2-b60aea148c76 \
  --with-deferred-deletes \
  --jdbc... # JDBC settings omitted in this example

# Now perform the file deletions
java -jar nessie-gc.jar deferred-deletes --live-set-id 0baaa1ff-90db-4ee5-b6d2-b60aea148c76 \
  --jdbc... # JDBC settings omitted in this example
```

!!! note
The _sweep_ phase does not access Nessie. It does use Iceberg and accesses the data lake.
If _deferred deletion_ is requested, no files will be deleted.

!!! note
Since data lakes can easily contain a huge amount of files, the _expire_ phase does not remember
every live data file (see the Iceberg assets above) individually, but uses a probabilistic data
structure (bloom filter). The default settings expect, for each content ID, 1,000,000 files and
uses a false-positive-probability of 0.0001 (those defaults may change, but can be inspected
with `java -jar nessie-gc.jar help expire`). The _expire_ phase will abort, if it hits a content-ID 
that _massively_ exceeds the configured false-positive-probability, because it hits way more live
file references.

!!! note
Nessie GC's _expire_ phase processes up to 4 content-IDs in parallel. This setting can be
changed using the `--expiry-parallelism` command line option.

### Recommended production setup for Nessie GC

It is highly recommended to use one of the supported databases to persist the live-content-sets.
Running the different Nessie GC phases separately is only supported with such a database.

Make yourself familiar with all the commands offered by `nessie-gc.jar` and the available command
line options. It is safe to run `java -jar nessie-gc.jar mark-live`, because it is non-destructive.
Use `java -jar nessie-gc.jar show --with-content-references` to inspect the collected live content
references.

Use _deferred deletion_ and inspect the files to be deleted **before** running
`java -jar nessie-gc.jar deferred-deletes`.

Use separate invocations for the _mark_, the _sweep_ and the _deferred deletion_ phases.

### All-in-one

As briefly mentioned above, the `java -jar nessie-gc.jar gc` command can be used to combine the
_mark_ and _sweep_ phases, optionally using the `--inmemory` option.

`gc` is equivalent to first running `identify` and then `expire`, and it takes the same set of
command line options. 

### Troubleshooting

Nessie GC tool emits the log output at `INFO` level. The default log level for the console can be
overridden using the Java system property `log.level.console`, for example using the following
command:

```shell
java -Dlog.level.console=DEBUG -jar nessie-gc.jar
```
