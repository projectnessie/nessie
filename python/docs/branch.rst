.. code-block:: bash

	Usage: cli branch [OPTIONS] [BRANCH] [NEW_BRANCH]
	
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
	
	      nessie branch -o 12345678abcdef new_branch test -> create new branch named
	      'new_branch' at hash 12345678abcdef on reference named 'test'
	
	      nessie branch -f existing_branch test -> assign branch named
	      'existing_branch' to head of reference named 'test'
	
	      nessie branch -o 12345678abcdef -f existing_branch test -> assign branch
	      named 'existing_branch' to hash 12345678abcdef on reference named 'test'
	
	Options:
	  -l, --list              list branches
	  -d, --delete            delete a branch
	  -f, --force             force branch assignment
	  -o, --hash-on-ref TEXT  Hash on source-reference for 'create' and 'assign'
	                          operations, if the branch shall not point to the HEAD
	                          of the given source-reference.
	  -c, --condition TEXT    Conditional Hash. Only perform the action if the
	                          branch currently points to the hash specified by this
	                          option.
	  --help                  Show this message and exit.
	
	

