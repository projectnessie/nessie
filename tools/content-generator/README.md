# Simple tool to generate (nonsense) Nessie content

When testing for example UI functionality, it is nice to have some content in Nessie, but we
do not have a tool for this available. This tool creates (nonsense) commits in 1 or more branches. 
Functionality supported:
* Generate N commits (defaults to 100)
* Commit to N branches (default: just the default branch)
* Generate tags (default probability: 0)
* Generate expected number of commits over a specified time duration - in other words: sleeps
  for "expected duration" divided by "number of commits" (default: no sleep)
* Generate content for N tables (default: 1 table)
* Generate content for any content type (default: Iceberg-table)

Uses the picocli library.

```
$ java -jar tools/content-generator/target/nessie-content-generator-0.12.2-SNAPSHOT.jar help generate

Usage: nessie-content-generator generate [-hV] [-b=<branchCount>]
       [-d=<runtimeDuration>] [-n=<numCommits>] [-t=<numTables>]
       [-T=<newTagProbability>] [--type=<contentType>] [-u=<uri>]
Generate commits
  -b, --num-branches=<branchCount>
                             Number of branches to use.
  -d, --duration=<runtimeDuration>
                             Runtime duration, equally distributed among the
                               number of commits to create. See java.time.
                               Duration for argument format details.
  -h, --help                 Show this help message and exit.
  -n, --num-commits=<numCommits>
                             Number of commits to create.
  -t, --num-tables=<numTables>
                             Number of table names, each commit chooses a
                               random table (content-key).
  -T, --tag-probability=<newTagProbability>
                             Probability to create a new tag off the last
                               commit.
      --type=<contentType>   Content-types to generate. Defaults to
                               ICEBERG_TABLE. Possible values: ICEBERG_TABLE,
                               DELTA_LAKE_TABLE, VIEW
  -u, --uri=<uri>            Nessie API endpoint URI, defaults to http:
                               //localhost:19120/api/v1.
  -V, --version              Print version information and exit.
```
