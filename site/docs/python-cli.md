# Command Line Interface

The Dremio Client uses the [Click](https://click.palletsprojects.com) library to generate its command line interface.
This is installed as `nessie_client` by `pip`.

All of the REST API calls are exposed via the command line interface. To see a list of what is available run:

``` bash
$ nessie_client --help
Usage: cli.py [OPTIONS] COMMAND [ARGS]...
  Nessie cli tool.

  Interact with Nessie branches and tables via the command line

Options:
  --config DIRECTORY   Custom config file.
  -e, --endpoint TEXT  Endpoint if different from config file
  -u, --username TEXT  username if different from config file
  --password TEXT      password if different from config file
  --skip-verify        skip verificatoin of ssl cert
  --version
  --help               Show this message and exit.

Commands:
  create-branch  Create a branch and optionally fork from base-branch.
  delete-branch  Delete a specific branch.
  list-branches  List all known branches.
  list-tables    List tables from BRANCH.
  merge-branch   Merge FROM-BRANCH into TO-BRANCH.
  show-branch    Show a specific branch.
  show-table     List tables from BRANCH.
```  

The config directory and a number of other settings can be configured from the command line and overwrite the default
config locations.

The output is currently in json format and is designed to be used effectively with `jq`. For example:

``` bash
$ nessie_client list-branches | jq .
```
