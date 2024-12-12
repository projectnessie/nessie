```
Usage: nessie-server-admin-tool-runner.jar [-hV] [COMMAND]
Nessie Server Admin Tool
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  info                  Nessie repository information
  help                  Display help information about the specified command.
  cleanup-repository    Cleanup unreferenced data from Nessie's repository.
  check-content         Check content readability of active keys.
  delete-catalog-tasks  Delete persisted state of Iceberg snapshot loading
                          tasks previously executed by the Nessie Catalog.
  erase-repository      Erase current Nessie repository (all data will be lost)
                          and optionally re-initialize it.
  export                Exports a Nessie repository to the local file system.
  import                Imports a Nessie repository from the local file system.
  show-licenses         Show 3rd party license information.

```
