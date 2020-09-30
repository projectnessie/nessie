# Python

```
# using python 3
pip install pynessie
``` 

## Configuration

The Nessie Python client is configured using the [confuse](https://github.com/beetbox/confuse) yaml based configuration
library. This looks for a configuration file called `config.yaml` in:

* macOS: `~/.config/pynessie` and `~/Library/Application Support/pynessie`
* Other Unix: `~/.config/pynessie` and `/etc/pynessie`
* Windows: `%APPDATA%\pynessie` where the `APPDATA` environment variable falls
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

The [command line interface](/tools/cli.md) can be configured with most of the above parameters via flags or by setting
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
