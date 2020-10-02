# Nessie CLI

The Nessie CLI is an easy way to get started with Nessie. It supports multiple branch 
and tag management capabilities. This is installed as `pynessie` by `pip`.

## Installation

```
# python 3 required
pip install pynessie
```

## Usage 
All of the REST API calls are exposed via the command line interface. To see a list of what is available run:

``` bash
$ nessie
``` 

``` bash
Usage: nessie [OPTIONS] COMMAND [ARGS]...

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
  assign-branch    Assign from one ref to another.
  create-branch    Create a branch and optionally fork from ref.
  delete-branch    Delete a specific branch.
  list-references  List all known references.
  list-tables      List tables from BRANCH.
  show-reference   Show a specific reference.
  show-table       List tables from ref.
```

!!! caution
    Please note that we're actively redefining the cli to better match Git syntax. You should expect that this syntax will change shortly.


## Configuration

You can configure the Nessie CLI by creating a configuration file as described below:

* macOS: `~/.config/nessie` and `~/Library/Application Support/nessie`
* Other Unix: `~/.config/nessie` and `/etc/nessie`
* Windows: `%APPDATA%\nessie` where the `APPDATA` environment variable falls
  back to `%HOME%\AppData\Roaming` if undefined
* Via the environment variable `DREMIO_CLIENTDIR`

The default config file is as follows:

``` yaml
auth:
    # Type can be either basic or aws
    type: basic

    # Username and password required if using basic auth
    username: <username>
    password: <password>
    timeout: 10

# Nessie endpoint
endpoint: http://localhost/api/v1

# whether to skip SSL cert verification
verify: true 
```

When configuring authentication type `aws`, the client delegates to the [Boto](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) 
library. You can configure credentials using any of the standard [Boto AWS methods](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials).

The [command line interface](../tools/cli.md) can be configured with most of the above parameters via flags or by setting
a config directory. The relevant configs can also be set via environment variables. These take precedence. The
environment variable format is to append `NESSIE_` to a config parameter and nested configs are separated by a *_*. For
example: `NESSIE_AUTH_TIMEOUT` maps to `auth.timeout` in the default configuration file above.


## Working with JSON

The Nessie CLI returns data in json format and is designed to be used effectively with [`jq`](https://stedolan.github.io/jq/). For example:

``` bash
$ nessie list-references | jq .
```

The Nessie CLI is built on the great Python [Click](https://click.palletsprojects.com) library. It requires Python 3.x.
