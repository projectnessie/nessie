
UI Commands
===========

Available Commands
------------------

.. code-block:: bash

    Usage: nessie [OPTIONS] COMMAND [ARGS]...

      Nessie cli tool.

      Interact with Nessie branches and tables via the command line

    Options:
      --version
      --help     Show this message and exit.

    Commands:
      branch       Branch operations.
      cherry-pick  Transplant HASHES onto current branch.
      config       Set and view config.
      contents     Contents operations.
      log          Show commit log.
      merge        Merge BRANCH into current branch.
      remote       Set and view remote endpoint.
      tag          Tag operations.


Config Command
--------------
Used to set config parameters found in ``default_config.yaml`` and to set the default context. To set default context use
``nessie config --add ref main``, all operations that are ref specific will happen on that ref unless otherwise specified.

.. code-block:: bash

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


Branch Command
--------------
Perform operations on branches: create, delete, modify and reassign.

.. code-block:: bash

    Usage: nessie branch [OPTIONS] [BRANCH] [NEW_BRANCH]

      Branch operations.

      BRANCH name of branch to list or create/assign NEW_BRANCH name of branch
      to assign from or rename to

      Examples:

          nessie branch -l -> list all branches

          nessie branch -l main -> list only main

          nessie branch -d main -> delete main

          nessie branch -> list all branches

          nessie branch main -> create branch main at current head

          nessie branch main test -> create branch main at head of test

          nessie branch -f main test -> assign main to head of test

    Options:
      -l, --list     list branches
      -d, --delete   delete a branch
      -f, --force    force branch assignment
      --json         write output in json format.
      -v, --verbose  Verbose output.
      -c, --condition TEXT  Conditional Hash. Only perform the action if branch
                            currently points to condition.
      --help         Show this message and exit.


Tag Command
-----------
Perform operations on tags: create, delete, modify and reassign.

.. code-block:: bash

    Usage: cli nessie [OPTIONS] [TAG_NAME] [NEW_TAG]

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
      -l, --list     list branches
      -d, --delete   delete a branches
      -f, --force    force branch assignment
      --json         write output in json format.
      -v, --verbose  Verbose output.
      -c, --condition TEXT  Conditional Hash. Only perform the action if branch
                            currently points to condition.
      --help         Show this message and exit.



Remote Command
--------------
Set and view the remote. The ``add`` command is a shortcut to ``nessie config --set endpoint <endpoint>`` and the show
command functions similarly to the ``git remote show <remote>`` command to show the remote and remote refs.

.. code-block:: bash

    Usage: nessie remote [OPTIONS] COMMAND [ARGS]...

      Set and view remote endpoint.

    Options:
      --help  Show this message and exit.

    Commands:
      add   Set current remote.
      show  Show current remote.


Log Command
-----------

View the commit log. This operates similarly to ``git log`` and shows the log in the terminals pager. Revision range is
specified as <hash>..<hash> or <hash/ref>.

.. code-block:: bash

    Usage: nessie log [OPTIONS] [REVISION_RANGE] [PATHS]...

      Show commit log.

      REVISION_RANGE optional branch, tag or hash to start viewing log from. If of
      the form <hash>..<hash> only show log for given range\n PATHS optional list
      of paths. If given, only show commits which affected the given paths

    Options:
      -n, --number INTEGER    number of log entries to return
      --since, --after TEXT   Commits more recent than specific date
      --until, --before TEXT  Commits older than specific date
      --author, --committer   limit commits to specific committer
      --json                  write output in json format.
      --help                  Show this message and exit.

Merge Command
-------------

Perform a merge operation. This takes commits on ``MERGE_BRANCH`` which not present on ``branch`` and adds them to
branch.

.. code-block:: bash

    Usage: nessie merge [OPTIONS] [MERGE_BRANCH]

      Merge BRANCH into current branch. BRANCH can be a hash or branch.

    Options:
      -b, --branch TEXT  branch to cherry-pick onto. If not supplied the default
                         branch from config is used
      -f, --force    force merge. condition not required in this case
      -c, --condition TEXT  Conditional Hash. Only perform the action if branch
                            currently points to condition.

      --help             Show this message and exit.

Cherry-Pick Command
-------------------

Perform a cherry-pick operation. This takes the list of commits ``HASHES`` and adds them to ``branch``.

.. code-block:: bash

    Usage: nessie cherry-pick [OPTIONS] [HASHES]...

      Transplant HASHES onto current branch.

    Options:
      -b, --branch TEXT  branch to cherry-pick onto. If not supplied the default
                         branch from config is used
      -f, --force    force cherry-pick. condition not required in this case
      -c, --condition TEXT  Conditional Hash. Only perform the action if branch
                            currently points to condition.

      --help             Show this message and exit.

Contents Command
----------------

View and list contents.

.. code-block:: bash

    Usage: nessie contents [OPTIONS] [KEY]

      Contents operations.

      KEY name of object to view, delete. If listing the key will limit by
      namespace what is included.

    Options:
      -l, --list     list tables
      -d, --delete   delete a table
      --json         write output in json format.
      -v, --verbose  Verbose output.
      -r, --ref TEXT valid ref (hash, branch, tag) to on which the contents are viewed. If missing uses the default context.

      --help         Show this message and exit.
