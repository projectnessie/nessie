---
search:
  exclude: true
---
<!--start-->

```
Usage: nessie-cli.jar [-hHqV] [--no-up-to-date-check] [--non-ansi] [-j=<historyFile>] [[-K] [-E]
                      [-s=<scriptFile> | -c[=<commands>...] [-c[=<commands>...]]...]] [-u=<uri>
                      [--client-name=<clientName>] [-o[=<String=String>[,
                      <String=String>...]...]]... [-r=<initialReference>]]

The Nessie CLI
See https://projectnessie.org/nessie-latest/cli/ for documentation.

  -h, --help                Show this help message and exit.
  -H, --[no-]history        Allows disabling the command history file in the REPL.
                            Default is to save the command history.
  -j, --history-file=<historyFile>
                            Specify an alternative history file for the REPL.
                              Default: ~/.nessie/nessie-cli.history
      --no-up-to-date-check Optionally disable the up-to-date check.
                            Only effective if --quiet is not specified.
      --non-ansi            Allows disabling the (default) ANSI mode. Disabling ANSI support can be
                              useful in non-interactive scripts.
  -q, --quiet               Quiet option - omit the welcome and exit output.
  -V, --version             Print version information and exit.

Statements to execute before or without running the REPL
========================================================

  -c, --command[=<commands>...]
                            Nessie CLI commands to run. Each value represents one command.
                            The process will exit once all specified commands have been executed.
                              To keep the REPL running in case of errors, specify the
                              --keep-running option.
  -E, --continue-on-error   When running commands via the --command or --run-script option the
                              process will stop/exit when a command could not be parsed or ran into
                              an error.
                            Specifying this option lets the REPL continue executing the remaining
                              commands after parse or runtime errors.
  -K, --keep-running        When running commands via the --command or --run-script option the
                              process will exit once the commands have been executed.
                            To keep the REPL running, specify this option.See the
                              --continue-on-error option.
  -s, --run-script=<scriptFile>
                            Run the commands in the Nessie CLI script referenced by this option.
                            Possible values are either a file path or use the minus character ('-')
                              to read the script from stdin.

Connect options
===============

      --client-name=<clientName>
                            Name of the client implementation to use, defaults to HTTP suitable for
                              Nessie REST API.
                            See https://projectnessie.org/nessie-latest/client_config/ for the
                              'nessie.client-builder-name' option.
  -o, --client-option[=<String=String>[,<String=String>...]...]
                            Parameters to configure the REST client.
                            See https://projectnessie.org/nessie-latest/client_config/
  -r, --initial-reference=<initialReference>
                            Name of the Nessie reference to use.
  -u, --uri=<uri>           REST API endpoint URI to connect to.
                            See 'HELP CONNECT' in the REPL.

```
