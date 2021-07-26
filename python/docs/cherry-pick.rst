.. code-block:: bash

	Usage: cli cherry-pick [OPTIONS] [HASHES]...
	
	  Transplant HASHES onto current branch.
	
	Options:
	  -b, --branch TEXT      branch to cherry-pick onto. If not supplied the default
	                         branch from config is used
	  -f, --force            force branch assignment
	  -c, --condition TEXT   Conditional Hash. Only perform the action if branch
	                         currently points to condition.
	  -s, --source-ref TEXT  Name of the reference used to read the hashes from.
	                         [required]
	  --help                 Show this message and exit.
	
	

