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
$ nessie --help
``` 

``` bash
Usage: nessie [OPTIONS] COMMAND [ARGS]...

  Nessie cli tool.

  Interact with Nessie branches and tables via the command line

Options:
  --json           write output in json format.
  -v, --verbose    Verbose output.
  --endpoint TEXT  Optional endpoint, if different from config file.
  --version
  --help           Show this message and exit.

Commands:
  branch       Branch operations.
  cherry-pick  Transplant HASHES onto current branch.
  config       Set and view config.
  contents     Contents operations.
  log          Show commit log.
  merge        Merge BRANCH into current branch.
  remote       Set and view remote endpoint.
  tag          Tag operations.
```

``` bash
$ nessie branch --help
``` 

``` bash
Usage: nessie branch [OPTIONS] [BRANCH] [NEW_BRANCH]

  Branch operations.

  BRANCH name of branch to list or create/assign

  NEW_BRANCH name of branch to assign from or rename to

  Examples:

      nessie branch -l -> list all branches

      nessie branch -l main -> list only main

      nessie branch -d main -> delete main

      nessie branch -> list all branches

      nessie branch main -> create branch main at current head

      nessie branch main test -> create branch main at head of test

      nessie branch -f main test -> assign main to head of test

Options:
  -l, --list            list branches
  -d, --delete          delete a branch
  -f, --force           force branch assignment
  -c, --condition TEXT  Conditional Hash. Only perform the action if branch
                        currently points to condition.

  --help                Show this message and exit.
```

``` bash
$ nessie cherry-pick --help
``` 

``` bash
Usage: nessie cherry-pick [OPTIONS] [HASHES]...

  Transplant HASHES onto current branch.

Options:
  -b, --branch TEXT     branch to cherry-pick onto. If not supplied the
                        default branch from config is used

  -f, --force           force branch assignment
  -c, --condition TEXT  Conditional Hash. Only perform the action if branch
                        currently points to condition.

  --help                Show this message and exit.
```

``` bash
$ nessie config --help
``` 

``` bash
Usage: nessie cherry-pick [OPTIONS] [HASHES]...

  Transplant HASHES onto current branch.

Options:
  -b, --branch TEXT     branch to cherry-pick onto. If not supplied the
                        default branch from config is used

  -f, --force           force branch assignment
  -c, --condition TEXT  Conditional Hash. Only perform the action if branch
                        currently points to condition.

  --help                Show this message and exit.
```

``` bash
$ nessie contents --help
``` 

``` bash
Usage: nessie contents [OPTIONS] [KEY]...

  Contents operations.

  KEY name of object to view, delete. If listing the key will limit by
  namespace what is included.

Options:
  -l, --list            list tables
  -d, --delete          delete a table
  -s, --set             modify a table
  -c, --condition TEXT  Conditional Hash. Only perform the action if branch
                        currently points to condition.

  -r, --ref TEXT        branch to list from. If not supplied the default
                        branch from config is used

  -m, --message TEXT    commit message
  --help                Show this message and exit.
!!! caution
    Please note that we're actively redefining the cli to better match Git syntax. You should expect that this syntax will change shortly.
```

``` bash
$ nessie log --help
``` 

``` bash
Usage: nessie log [OPTIONS] [REVISION_RANGE] [PATHS]...

  Show commit log.

  REVISION_RANGE optional branch, tag or hash to start viewing log from. If
  of the form <hash>..<hash> only show log for given range

  PATHS optional list of paths. If given, only show commits which affected
  the given paths

Options:
  -n, --number INTEGER    number of log entries to return
  --since, --after TEXT   Commits more recent than specific date
  --until, --before TEXT  Commits older than specific date
  --author, --committer   limit commits to specific committer
  --help                  Show this message and exit.
```

``` bash
$ nessie merge --help
``` 

``` bash
Usage: nessie merge [OPTIONS] [MERGE_BRANCH]

  Merge BRANCH into current branch. BRANCH can be a hash or branch.

Options:
  -b, --branch TEXT     branch to cherry-pick onto. If not supplied the
                        default branch from config is used

  -f, --force           force branch assignment
  -c, --condition TEXT  Conditional Hash. Only perform the action if branch
                        currently points to condition.

  --help                Show this message and exit.
```

``` bash
$ nessie remote --help
``` 

``` bash
Usage: nessie remote [OPTIONS] COMMAND [ARGS]...

  Set and view remote endpoint.

Options:
  --help  Show this message and exit.

Commands:
  add       Set current remote.
  set-head  Set current default branch.
  show      Show current remote.
```

``` bash
$ nessie tag --help
``` 

``` bash
Usage: nessie tag [OPTIONS] [TAG_NAME] [NEW_TAG]

  Tag operations.

  TAG_NAME name of branch to list or create/assign

  NEW_TAG name of branch to assign from or rename to

  Examples:

      nessie tag -l -> list all tags

      nessie tag -l main -> list only main

      nessie tag -d main -> delete main

      nessie tag -> list all tags

      nessie tag main -> create tag xxx at current head

      nessie tag main test -> create tag xxx at head of test

      nessie tag -f main test -> assign xxx to head of test

Options:
  -l, --list            list branches
  -d, --delete          delete a branches
  -f, --force           force branch assignment
  -c, --condition TEXT  Conditional Hash. Only perform the action if branch
                        currently points to condition.

  --help                Show this message and exit.
```

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
