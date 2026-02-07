---
search:
  exclude: true
---
<!--start-->

```
Usage: nessie-server-admin-tool-runner.jar import [-ehV]
       [--commit-batch-size=<commitBatchSize>]
       [--input-buffer-size=<inputBufferSize>] -p=<import-from>
Imports a Nessie repository from the local file system.
      --commit-batch-size=<commitBatchSize>
                             Batch size when writing commits, defaults to 20.
  -e, --erase-before-import  Erase an existing repository before the import is
                               started.
                             This will delete all previously existing Nessie
                               data.
                             Using this option has no effect, if the Nessie
                               repository does not already exist.
  -h, --help                 Show this help message and exit.
      --input-buffer-size=<inputBufferSize>
                             Input buffer size, defaults to 32768.
  -p, --path=<import-from>   The ZIP file or directory to read the export from.
                             If this parameter refers to a file, the import
                               will assume that it is a ZIP file, otherwise a
                               directory.
  -V, --version              Print version information and exit.

```
