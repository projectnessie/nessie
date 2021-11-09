.. code-block:: bash

   Usage: nessie cherry-pick [OPTIONS] [HASHES]...
   
     Cherry-pick HASHES onto another branch.
   
     HASHES commit hashes to be cherry-picked from the source reference.
   
     Examples:
   
         nessie cherry-pick -c 12345678abcdef -s dev 21345678abcdef 31245678abcdef
         -> cherry pick 2 commits with commit hash '21345678abcdef'
         '31245678abcdef' from dev branch to default branch with default branch's
         expected hash '12345678abcdef'
   
         nessie cherry-pick -b main -c 12345678abcdef -s dev 21345678abcdef
         31245678abcdef -> cherry pick 2 commits with commit hash '21345678abcdef'
         '31245678abcdef' from dev branch to a branch named main with main branch's
         expected hash '12345678abcdef'
   
   Options:
     -b, --branch TEXT      branch to cherry-pick onto. If not supplied the default
                            branch from config is used
     -f, --force            force branch assignment
     -c, --condition TEXT   Expected hash. Only perform the action if the branch
                            currently points to the hash specified by this option.
     -s, --source-ref TEXT  Name of the reference used to read the hashes from.
                            [required]
     --help                 Show this message and exit.
   
   

