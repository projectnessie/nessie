```
Usage: nessie-server-admin-tool-runner.jar erase-repository [-hV]
       [--confirmation-code=<confirmationCode>] [-r=<newDefaultBranch>]
Erase current Nessie repository (all data will be lost) and optionally
re-initialize it.
      --confirmation-code=<confirmationCode>
                  Confirmation code for erasing the repository (will be emitted
                    by this command if not set).
  -h, --help      Show this help message and exit.
  -r, --re-initialize=<newDefaultBranch>
                  Re-initialize the repository after erasure. If set, provides
                    the default branch name for the new repository.
  -V, --version   Print version information and exit.

```
