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
$ nessie show --help
usage: nessie show [<object>]

$ nessie log --help
usage nessie log [<revision_range>] [[--] <path>...]

$ nessie branch --help
usage: nessie branch -l
   or: nessie branch [-f] <branch> [<commit> | <object>]
   or: nessie branch -d <branch>
   
$ nessie tag --help
usage: nessie tag -l
       nessie tag [-f] <tagname> [<commit> | <object>]
       nessie tag -d <tagname>
       
$ nessie push <endpoint> --help
usage: nessie push <endpoint> [<commit>|<object>]:<object>]
```  

## Configuration

You can configure the Nessie CLI by creating a configuration file as described below:

* macOS: `~/.config/pynessie` and `~/Library/Application Support/pynessie`
* Other Unix: `~/.config/pynessie` and `/etc/pynessie`
* Windows: `%APPDATA%\pynessie` where the `APPDATA` environment variable falls
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
endpoint: http://localhost/api/v1
verify: true # whether to skip SSL cert verification
```

When configuring authentication type `aws`, the client delegates to the great Boto 
library. You can configure credentials using any of the standard [AWS methods](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials).


## Working with JSON

The Nessie CLI returns data in json format and is designed to be used effectively with [`jq`](https://stedolan.github.io/jq/). For example:

``` bash
$ nessie branch -l | jq .
```

The Nessie CLI is built on the great Python [Click](https://click.palletsprojects.com) library. It requires Python 3.x.
