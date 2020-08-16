# Python Client

See [Installation](/pydoc/installation). Or simply use pip `pip install nessie_client`. 

## Configuration


The Dremio Client is configured using the [confuse](https://github.com/beetbox/confuse) yaml based configuration
library. This looks for a configuration file called `config.yaml` in:

* macOS: `~/.config/nessie_client` and `~/Library/Application Support/nessie_client`
* Other Unix: `~/.config/nessie_client` and `/etc/nessie_client`
* Windows: `%APPDATA%\nessie_client` where the `APPDATA` environment variable falls
  back to `%HOME%\AppData\Roaming` if undefined
* Via the environment variable `DREMIO_CLIENTDIR`

The default config file is as follows:

``` yaml
auth:
    type: basic #  currently only basic or aws are supported
    username: nessie
    password: nessie123
    timeout: 10
endpoint: http://localhost/api/v1
verify: true # whether to skip SSL cert verification
```

The [command line interface](python-cli.md) can be configured with most of the above parameters via flags or by setting
a config directory. The relevant configs can also be set via environment variables. These take precedence. The
environment variable format is to append `NESSIE_` to a config parameter and nested configs are separated by a *_*. For
example: `NESSIE_AUTH_TIMEOUT` maps to `auth.timeout` in the default configuration file above.


## Usage

To instantiate a client simply run

``` python
from nessie_client import init
client = init() # this will look for the client config as per above
branches = client.list_branches()
print(branches)
```

All endpoint options are available from this client. See [Client API](/pydocs/nessie_client.html) for full options.
