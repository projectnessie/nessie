.. code-block:: bash

	Usage: cli tag [OPTIONS] [TAG_NAME] [NEW_TAG]
	
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
	
	      nessie tag -f existing_tag test -> assign tag named 'existing_tag' to head
	      of reference named 'test'
	
	      nessie tag -o 12345678abcdef -f existing_tag test -> assign tag named
	      'existing_tag' to hash 12345678abcdef on reference named 'test'
	
	Options:
	  -l, --list              list branches
	  -d, --delete            delete a branches
	  -f, --force             force branch assignment
	  -o, --hash-on-ref TEXT  Hash on source-reference for 'create' and 'assign'
	                          operations, if the tag shall not point to the HEAD of
	                          the given source-reference.
	  -c, --condition TEXT    Conditional Hash. Only perform the action if the tag
	                          currently points to the hash specified by this option.
	  --help                  Show this message and exit.
	
	

