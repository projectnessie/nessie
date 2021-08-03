.. code-block:: bash

	Usage: cli merge [OPTIONS] [FROM_BRANCH]
	
	  Merge FROM_BRANCH into current branch. FROM_BRANCH can be a hash or branch.
	
	Options:
	  -b, --branch TEXT       branch to merge onto. If not supplied the default
	                          branch from config is used
	  -f, --force             force branch assignment
	  -c, --condition TEXT    Conditional Hash. Only perform the action if branch
	                          currently points to condition.
	  -o, --hash-on-ref TEXT  Hash on merge-from-reference
	  --help                  Show this message and exit.
	
	

