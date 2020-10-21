
UI Commands
===========

Available Commands
------------------

.. include:: main.rst

Config Command
--------------
Used to set config parameters found in ``default_config.yaml`` and to set the default context. To set default context use
``nessie config --add default_branch main``, all operations that are ref specific will happen on that ref unless
otherwise specified.

.. include:: config.rst

Branch Command
--------------
Perform operations on branches: create, delete, modify and reassign.

.. include:: branch.rst

Tag Command
-----------
Perform operations on tags: create, delete, modify and reassign.

.. include:: tag.rst

Remote Command
--------------
Set and view the remote. The ``add`` command is a shortcut to ``nessie config --set endpoint <endpoint>`` and the show
command functions similarly to the ``git remote show <remote>`` command to show the remote and remote refs.

.. include:: remote.rst

Log Command
-----------

View the commit log. This operates similarly to ``git log`` and shows the log in the terminals pager. Revision range is
specified as <hash>..<hash> or <hash/ref>.

.. include:: log.rst

Merge Command
-------------

Perform a merge operation. This takes commits on ``MERGE_BRANCH`` which not present on ``branch`` and adds them to
branch.

.. include:: merge.rst

Cherry-Pick Command
-------------------

Perform a cherry-pick operation. This takes the list of commits ``HASHES`` and adds them to ``branch``.

.. include:: cherry-pick.rst

Contents Command
----------------

View and list contents.

.. include:: contents.rst
