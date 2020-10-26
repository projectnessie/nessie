# Python

```
# using python 3
pip install pynessie
``` 

## Configuration

When you install pynessie, you get the Python client along with a Python CLI. Configuration 
for both is covered in our reference for the [command line interface](../tools/cli.md). 

## Usage

To instantiate a client simply run

``` python
from pynessie import init
client = init() # this will look for the client config as per above
branches = client.list_branches()
print(branches)
```

All endpoint options are available from this client.


## Spark usage from Python

A common way to interact with Nessie is via Spark. You can read more about working 
with Nessie and Spark together on our [Spark](../tools/spark.md) docs page.

## API Documentation

API docs are hosted on [readthedocs](https://nessie.readthedocs.io/en/latest/)
