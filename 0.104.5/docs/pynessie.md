---
title: "Python based CLI"
---

# Python based CLI

!!! warning
    The Python based CLI has been superseded by the new [Nessie CLI](./cli.md). This Python CLI
    is currently no longer developed and does not support Nessie REST API v2.

The Nessie CLI is an easy way to get started with Nessie. It supports multiple branch 
and tag management capabilities. This is installed as `pynessie` via `pip install pynessie`.
Additional information about `pynessie` and release notes can be found at the [PyPI](https://pypi.org/project/pynessie/) site. 

## Installation

```
# python 3 required
pip install pynessie
```

## Usage 
All the REST API calls are exposed via the command line interface. To see a list of what is available run:

``` bash
$ nessie --help
``` 

All docs of the CLI can be found [here](https://nessie.readthedocs.io/en/latest/cli.html).

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
    # Authentication type can be: none, bearer or aws
    type: none
    
    # OpenID token for the "bearer" authentication type
    # token: <OpenID token>
    
    timeout: 10

# Nessie endpoint
endpoint: http://localhost/api/v1

# whether to skip SSL cert verification
verify: true 
```

Possible values for the `auth.type` property are:

* `none` (default)
* `bearer`
* `aws`

When configuring authentication type `bearer`, the `auth.token` parameter should be set to a valid
[OpenID token](https://openid.net/specs/openid-connect-core-1_0.html). The token can be set in the Nessie
configuration file, as an environment variable (details below), or by the `--auth-token <TOKEN>` command
line option (for each command).

When configuring authentication type `aws`, the client delegates to the [Boto](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) 
library. You can configure credentials using any of the standard [Boto AWS methods](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials).
Additionally, the Nessie `auth.region` parameter should be set to the relevant AWS region.

The command line interface can be configured with most of the above parameters via flags or by setting
a config directory. The relevant configs can also be set via environment variables. These take precedence. The
environment variable format is to append `NESSIE_` to a config parameter and nested configs are separated by a *_*. For
example: `NESSIE_AUTH_TIMEOUT` maps to `auth.timeout` in the default configuration file above.


## Working with JSON

The Nessie CLI can return data in json format and can be used effectively with [`jq`](https://stedolan.github.io/jq/). For example:

``` bash
$ nessie --json branch -l | jq .
```

The Nessie CLI is built on the great Python [Click](https://click.palletsprojects.com) library. It requires Python 3.x.
