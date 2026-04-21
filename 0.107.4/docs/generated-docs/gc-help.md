---
search:
  exclude: true
---
<!--start-->

```
Usage: nessie-gc.jar [-hV] [COMMAND]
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  help                           Display help information about the specified command.
  mark-live, identify, mark      Run identify-live-content phase of Nessie GC, must not be used
                                   with the in-memory contents-storage.
  sweep, expire                  Run expire-files + delete-orphan-files phase of Nessie GC using a
                                   live-contents-set stored by a previous run of the mark-live
                                   command, must not be used with the in-memory contents-storage.
  gc                             Run identify-live-content and expire-files + delete-orphan-files.
  list                           List existing live-sets, must not be used with the in-memory
                                   contents-storage.
  delete                         Delete a live-set, must not be used with the in-memory
                                   contents-storage.
  list-deferred                  List files collected as deferred deletes, must not be used with
                                   the in-memory contents-storage.
  deferred-deletes               Delete files collected as deferred deletes, must not be used with
                                   the in-memory contents-storage.
  show                           Show information of a live-content-set, must not be used with the
                                   in-memory contents-storage.
  show-sql-create-schema-script  Print DDL statements to create the schema.
  create-sql-schema              JDBC schema creation.
  completion-script              Extracts the command-line completion script.
  show-licenses                  Show 3rd party license information.

```
