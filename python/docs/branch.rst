.. code-block:: bash

   Usage: nessie branch [OPTIONS] [BRANCH] [BASE_REF]

     Branch operations.

     BRANCH name of branch to list or create/assign

     BASE_REF name of branch or tag from which to create/assign the new BRANCH

     Examples:

         nessie branch -> list all branches

         nessie branch -l -> list all branches

         nessie branch -l main -> list only main

         nessie branch -d main -> delete main

         nessie branch new_branch -> create new branch named 'new_branch' at
         current HEAD of the default branch

         nessie branch new_branch main -> create new branch named 'new_branch' at
         head of reference named 'main'

         nessie branch -o 12345678abcdef new_branch main -> create a branch named
         'new_branch' at hash 12345678abcdef on reference named 'main'

         nessie branch new_branch main@12345678abcdef -> create a branch named
         'new_branch' at hash 12345678abcdef on reference named 'main', alternate
         syntax for the above

         nessie branch -f existing_branch main -> assign branch named
         'existing_branch' to head of reference named 'main'

         nessie branch -o 12345678abcdef -f existing_branch main -> assign branch
         named 'existing_branch' to hash 12345678abcdef on reference named 'main'

   Options:
     -l, --list              list branches
     -d, --delete            delete a branch
     -f, --force             force branch assignment
     -o, --hash-on-ref TEXT  Hash on source-reference for 'create' and 'assign'
                             operations, if the branch shall not point to the HEAD
                             of the given source-reference.
     -c, --condition TEXT    Expected hash. Only perform the action if the branch
                             currently points to the hash specified by this option.
     -x, --extended          Retrieve additional metadata for a branch, such as
                             number of commits ahead/behind, info about the HEAD
                             commit, number of total commits, or the common
                             ancestor hash.
     --help                  Show this message and exit.


