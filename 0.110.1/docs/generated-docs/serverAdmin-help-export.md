```
Usage: nessie-server-admin-tool-runner.jar export [-hV] [--full-scan]
       [-C=<expectedCommitCount>] [--commit-batch-size=<commitBatchSize>]
       [--content-batch-size=<number>] [--export-version=<exportVersion>]
       [-F=<output-format>] [--max-file-size=<maxFileSize>]
       [--output-buffer-size=<outputBufferSize>] -p=<export-to>
       [--single-branch-current-content=<branch-name>]
       [--object-resolvers=<genericObjectResolvers>]...
Exports a Nessie repository to the local file system.
  -C, --expected-commit-count=<expectedCommitCount>
                           Expected number of commits in the repository,
                             defaults to 1000000.
      --commit-batch-size=<commitBatchSize>
                           Batch size when reading commits and their associated
                             contents, defaults to 20.
      --content-batch-size=<number>
                           Group the specified number of content objects into
                             each commit at export time. This option is ignored
                             unless --single-branch-current-content is set. The
                             default value is 100.
      --export-version=<exportVersion>
                           The export version, defaults to 3.
  -F, --output-format=<output-format>
                           Explicitly define the output format to use to the
                             export.
                           If not specified, the implementation chooses the ZIP
                             export, if --path ends in .zip, otherwise will use
                             the directory output format.
                           Possible values: ZIP, DIRECTORY
      --full-scan          Export all commits, including those that are no
                             longer reachable any named reference.Using this
                             option is _not_ recommended.
  -h, --help               Show this help message and exit.
      --max-file-size=<maxFileSize>
                           Maximum size of a file in bytes inside the export.
      --object-resolvers=<genericObjectResolvers>
                           Additional jars that provide
                             `TransferRelatedObjects` implementations.
                           Jars can be provided as file paths or as URLs.
      --output-buffer-size=<outputBufferSize>
                           Output buffer size, defaults to 32768.
  -p, --path=<export-to>   The ZIP file or directory to create with the export
                             contents.
      --single-branch-current-content=<branch-name>
                           Export only the most recent contents from the
                             specified branch.
  -V, --version            Print version information and exit.

```
