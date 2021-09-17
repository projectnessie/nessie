# Nessie CLI

The Nessie CLI is an easy way to get started with Nessie. It supports multiple branch 
and tag management capabilities. This is installed as `pynessie` via `pip install pynessie`.
Additional information about `pynessie` and release notes can be found at the [PyPI](https://pypi.org/project/pynessie/) site. 

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
  --json             write output in json format.
  -v, --verbose      Verbose output.
  --endpoint TEXT    Optional endpoint, if different from config file.
  --auth-token TEXT  Optional bearer auth token, if different from config
                     file.
  --version
  --help             Show this message and exit.

Commands:
  branch       Branch operations.
  cherry-pick  Transplant HASHES onto current branch.
  config       Set and view config.
  contents     Contents operations.
  log          Show commit log.
  merge        Merge FROM_BRANCH into current branch.
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

      nessie branch -> list all branches

      nessie branch -l -> list all branches

      nessie branch -l main -> list only main

      nessie branch -d main -> delete main

      nessie branch new_branch -> create new branch named 'new_branch' at
      current HEAD of the default branch

      nessie branch new_branch test -> create new branch named 'new_branch' at
      head of reference named 'test'

      nessie branch -o 12345678abcdef new_branch test -> create new branch
      named 'new_branch' at hash 12345678abcdef on reference named 'test'

      nessie branch -f existing_branch test -> assign branch named
      'existing_branch' to head of reference named 'test'

      nessie branch -o 12345678abcdef -f existing_branch test -> assign branch
      named 'existing_branch' to hash 12345678abcdef on reference named 'test'

Options:
  -l, --list              list branches
  -d, --delete            delete a branch
  -f, --force             force branch assignment
  -o, --hash-on-ref TEXT  Hash on source-reference for 'create' and 'assign'
                          operations, if the branch shall not point to the
                          HEAD of the given source-reference.
  -c, --condition TEXT    Conditional Hash. Only perform the action if the
                          branch currently points to the hash specified by
                          this option.
  --help                  Show this message and exit.
```

``` bash
$ nessie cherry-pick --help
``` 

``` bash
Usage: nessie cherry-pick [OPTIONS] [HASHES]...

  Transplant HASHES onto current branch.

Options:
  -b, --branch TEXT      branch to cherry-pick onto. If not supplied the
                         default branch from config is used
  -f, --force            force branch assignment
  -c, --condition TEXT   Conditional Hash. Only perform the action if the
                         branch currently points to the hash specified by this
                         option.
  -s, --source-ref TEXT  Name of the reference used to read the hashes from.
                         [required]
  --help                 Show this message and exit.
```

``` bash
$ nessie config --help
``` 

``` bash
Usage: nessie config [OPTIONS] [KEY]

  Set and view config.

Options:
  --get TEXT    get config parameter
  --add TEXT    set config parameter
  -l, --list    list config parameters
  --unset TEXT  unset config parameter
  --type TEXT   type to interpret config value to set or get. Allowed options:
                bool, int
  -h, --help    Show this message and exit.
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
  -l, --list                      list tables
  -d, --delete                    delete a table
  -s, --set                       modify a table
  -c, --condition TEXT            Conditional Hash. Only perform the action if
                                  the branch currently points to the hash
                                  specified by this option.
  -r, --ref TEXT                  branch to list from. If not supplied the
                                  default branch from config is used
  -m, --message TEXT              commit message
  -t, --type TEXT                 entity types to filter on, if no entity
                                  types are passed then all types are returned
  --query, --query-expression TEXT
                                  Allows advanced filtering using the Common
                                  Expression Language (CEL). An intro to CEL
                                  can be found at
                                  https://github.com/google/cel-
                                  spec/blob/master/doc/intro.md. Some examples
                                  with usable variables 'entry.namespace'
                                  (string) & 'entry.contentType' (string) are:
                                  entry.namespace.startsWith('a.b.c')
                                  entry.contentType in
                                  ['ICEBERG_TABLE','DELTA_LAKE_TABLE'] entry.n
                                  amespace.startsWith('some.name.space') &&
                                  entry.contentType in
                                  ['ICEBERG_TABLE','DELTA_LAKE_TABLE']
  --author TEXT                   The author to use for the commit
  --help                          Show this message and exit.
```

``` bash
$ nessie log --help
``` 

``` bash
Usage: nessie log [OPTIONS] [REVISION_RANGE] [PATHS]...

  Show commit log.

  REVISION_RANGE optional hash to start viewing log from. If of the form
  <start_hash>..<end_hash> only show log for given range on the particular ref
  that was provided

  PATHS optional list of paths. If given, only show commits which affected the
  given paths

Options:
  -r, --ref TEXT                  branch to list from. If not supplied the
                                  default branch from config is used
  -n, --number INTEGER            number of log entries to return
  --since, --after TEXT           Only include commits newer than specific
                                  date
  --until, --before TEXT          Only include commits older than specific
                                  date
  --author TEXT                   Limit commits to a specific author (this is
                                  the original committer). Supports specifying
                                  multiple authors to filter by.
  --committer TEXT                Limit commits to a specific committer (this
                                  is the logged in user/account who performed
                                  the commit). Supports specifying multiple
                                  committers to filter by.
  --query, --query-expression TEXT
                                  Allows advanced filtering using the Common
                                  Expression Language (CEL). An intro to CEL
                                  can be found at
                                  https://github.com/google/cel-
                                  spec/blob/master/doc/intro.md. Some examples
                                  with usable variables 'commit.author'
                                  (string) / 'commit.committer' (string) /
                                  'commit.commitTime' (timestamp) /
                                  'commit.hash' (string) / 'commit.message'
                                  (string) / 'commit.properties' (map) are:
                                  commit.author=='nessie_author'
                                  commit.committer=='nessie_committer'
                                  timestamp(commit.commitTime) >
                                  timestamp('2021-06-21T10:39:17.977922Z')
  --help                          Show this message and exit.
```

``` bash
$ nessie merge --help
``` 

``` bash
Usage: nessie merge [OPTIONS] [FROM_BRANCH]

  Merge FROM_BRANCH into current branch. FROM_BRANCH can be a hash or branch.

Options:
  -b, --branch TEXT       branch to merge onto. If not supplied the default
                          branch from config is used
  -f, --force             force branch assignment
  -c, --condition TEXT    Conditional Hash. Only perform the action if the
                          branch currently points to the hash specified by
                          this option.
  -o, --hash-on-ref TEXT  Hash on merge-from-reference
  --help                  Show this message and exit.
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

      nessie tag -> list all tags

      nessie tag -l -> list all tags

      nessie tag -l main -> list only main

      nessie tag -d main -> delete main

      nessie tag new_tag -> create new tag named 'new_tag' at current HEAD of
      the default branch

      nessie tag new_tag test -> create new tag named 'new_tag' at head of
      reference named 'test'

      nessie tag -o 12345678abcdef new_tag test -> create new tag named
      'new_tag' at hash 12345678abcdef on reference named 'test'

      nessie tag -f existing_tag test -> assign tag named 'existing_tag' to
      head of reference named 'test'

      nessie tag -o 12345678abcdef -f existing_tag test -> assign tag named
      'existing_tag' to hash 12345678abcdef on reference named 'test'

Options:
  -l, --list              list branches
  -d, --delete            delete a branches
  -f, --force             force branch assignment
  -o, --hash-on-ref TEXT  Hash on source-reference for 'create' and 'assign'
                          operations, if the tag shall not point to the HEAD
                          of the given source-reference.
  -c, --condition TEXT    Conditional Hash. Only perform the action if the tag
                          currently points to the hash specified by this
                          option.
  --help                  Show this message and exit.
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
