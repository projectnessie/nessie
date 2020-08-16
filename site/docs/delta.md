# Delta Lake

When using Nessie as the backing store for Delta Lake the restrictions on which types of filesystems/blob stores can be
written to are no longer valid. When using Nessie you can write to Delta Lake regardless of the filesystem or number
writers.

Similarly to Iceberg the Delta Lake library should be added to the `spark.jars` parameter. This fat jar has all the
required libraries for Nessie operation. The Nessie functionality is implemented as a `LogStore` and can be activated by
setting `spark.delta.logStore.class=com.dremio.nessie.deltalate.DeltaLake`. Now Delta will use Nessie to facilitate
atomic transactions. Similar to iceberg the following must be set on the hadoop configuration:

```
nessie.url = full url to nessie
nessie.username = username if using basic auth, omitted otherwise
nessie.password = password if using basic auth, omitted otherwise
nessie.auth.type = authentication type (BASIC or AWS)
nessie.view-branch = branch name. Optional, if not set the server default branch will be used
```

Once the log store is set all other operations are the same as any other Delta Lake log store. See [Getting
Started](https://docs.delta.io/latest/quick-start.html) to learn more.

!!! warning
    Spark 3 is not yet supported for Delta Lake and Nessie. Delta version must be `<=0.6.1`

!!! warning
    Currently branch operations (eg merge and create) are not supported directly from the Delta Lake connector. Use
    either the command line tool or the python library.
