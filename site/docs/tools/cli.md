# Nessie CLI

The Nessie CLI is an easy way to get started with Nessie. It supports multiple branch 
and tag management capabilities. This is installed as `pynessie` by `pip`.

```
pip install pynessie
```

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

The config directory and a number of other settings can be configured from the command line and overwrite the default
config locations.

The Nessie CLI returns data in json format and is designed to be used effectively with [`jq`](https://stedolan.github.io/jq/). For example:

``` bash
$ nessie branch -l | jq .
```

The Nessie CLI is built on the great Python [Click](https://click.palletsprojects.com) library. It requires Python 3.x.
