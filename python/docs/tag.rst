.. code-block:: bash

	Usage: cli tag [OPTIONS] [TAG_NAME] [NEW_TAG]
	
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
	
	

