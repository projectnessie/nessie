.. code-block:: bash

	Usage: cli contents [OPTIONS] [KEY]...
	
	  Contents operations.
	
	  KEY name of object to view, delete. If listing the key will limit by
	  namespace what is included.
	
	Options:
	  -l, --list            list tables
	  -d, --delete          delete a table
	  -s, --set             modify a table
	  -c, --condition TEXT  Conditional Hash. Only perform the action if branch
	                        currently points to condition.
	
	  -r, --ref TEXT        branch to list from. If not supplied the default branch
	                        from config is used
	
	  -m, --message TEXT    commit message
	  --help                Show this message and exit.
	
	

