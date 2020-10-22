.. code-block:: bash

	Usage: cli branch [OPTIONS] [BRANCH] [NEW_BRANCH]
	
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
	
	

