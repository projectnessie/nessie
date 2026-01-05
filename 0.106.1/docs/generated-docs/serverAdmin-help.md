```
2026-01-05 12:35:46,878 WARN  [org.hibernate.validator.internal.metadata.aggregated.CascadingMetaDataBuilder] (main) HV000271: Using `@Valid` on a container (java.util.List) is deprecated. You should apply the annotation on the type argument(s). Affected element: ContentService#getMultipleContents(String, String, List, boolean, RequestMeta)
2026-01-05 12:35:47,057 WARN  [org.hibernate.validator.internal.metadata.aggregated.CascadingMetaDataBuilder] (main) HV000271: Using `@Valid` on a container (java.util.List) is deprecated. You should apply the annotation on the type argument(s). Affected element: ContentService#getMultipleContents(String, String, List, boolean, RequestMeta)
Usage: nessie-server-admin-tool-runner.jar [-hV] [COMMAND]
Nessie Server Admin Tool
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  info                  Nessie repository information
  help                  Display help information about the specified command.
  cleanup-repository    Cleanup unreferenced data from Nessie's repository.
  cut-history           Advanced commit log manipulation command that removes
                          parents from the specified commit. Read the full help
                          message before using!
  check-content         Check content readability of active keys.
  delete-catalog-tasks  Delete persisted state of Iceberg snapshot loading
                          tasks previously executed by the Nessie Catalog.
  erase-repository      Erase current Nessie repository (all data will be lost)
                          and optionally re-initialize it.
  export                Exports a Nessie repository to the local file system.
  import                Imports a Nessie repository from the local file system.
  show-licenses         Show 3rd party license information.

```
