.. code-block:: bash

	Usage: cli merge [OPTIONS] [MERGE_BRANCH]
	
	  Merge BRANCH into current branch. BRANCH can be a hash or branch.
	
	Options:
	  -b, --branch TEXT     branch to cherry-pick onto. If not supplied the default
	                        branch from config is used
	
	  -f, --force           force branch assignment
	  -c, --condition TEXT  Conditional Hash. Only perform the action if branch
	                        currently points to condition.
	
	  --help                Show this message and exit.
	
	

