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
* Read references, commits and content objects
* Delete content objects
* Create missing namespaces

```
$ java -jar nessie-content-generator-0.58.1.jar -h
Usage: nessie-content-generator [-hV] [-u=<uri>] [COMMAND]
  -h, --help        Show this help message and exit.
  -u, --uri=<uri>   Nessie API endpoint URI, defaults to http://localhost:
                      19120/api/v2.
  -V, --version     Print version information and exit.
Commands:
  generate                   Generate commits
  commits                    Read commits
  refs                       Read references
  content                    Read content objects
  content-refresh            Get and Put content objects without changes to
                               refresh their storage model
  delete                     Delete content objects
  create-missing-namespaces  Creates missing namespaces for content keys at
                               branch HEADs.
  help                       Display help information about the specified
                               command.
```


```
$ java -jar nessie-content-generator-0.58.1.jar help generate 
Usage: nessie-content-generator generate [-hvV] [--continue-on-error]
       [-b=<branchCount>] [-d=<runtimeDuration>] [-D=<defaultBranchName>]
       [--key-pattern=<keyPattern>] [-n=<numCommits>]
       [--puts-per-commit=<putsPerCommit>] [-t=<numTables>]
       [-T=<newTagProbability>] [--type=<contentType>] [-u=<uri>]
Generate commits
  -b, --num-branches=<branchCount>
                             Number of branches to use.
      --continue-on-error
  -d, --duration=<runtimeDuration>
                             Runtime duration, equally distributed among the
                               number of commits to create. See java.time.
                               Duration for argument format details.
  -D, --default-branch=<defaultBranchName>
                             Name of the default branch, uses the server's
                               default branch if not specified.
  -h, --help                 Show this help message and exit.
      --key-pattern=<keyPattern>

  -n, --num-commits=<numCommits>
                             Number of commits to create.
      --puts-per-commit=<putsPerCommit>

  -t, --num-tables=<numTables>
                             Number of table names, each commit chooses a
                               random table (content-key).
  -T, --tag-probability=<newTagProbability>
                             Probability to create a new tag off the last
                               commit.
      --type=<contentType>   Content-types to generate. Defaults to
                               ICEBERG_TABLE. Possible values: ICEBERG_TABLE,
                               ICEBERG_VIEW, DELTA_LAKE_TABLE
  -u, --uri=<uri>            Nessie API endpoint URI, defaults to http:
                               //localhost:19120/api/v2.
  -v, --verbose              Produce verbose output (if possible)
  -V, --version              Print version information and exit.
```
