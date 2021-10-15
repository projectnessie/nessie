.. code-block:: bash

	Usage: cli contents [OPTIONS] [KEY]...
	
	  Contents operations.
	
	  KEY name of object to view, delete. If listing the key will limit by namespace
	  what is included.
	
	Options:
	  -l, --list                      list tables
	  -d, --delete                    delete a table
	  -s, --set                       modify a table
	  -i, --stdin                     read contents for --set from STDIN (separated
	                                  by Ctrl-D)
	  -S, --expect-same-contents      send the same contents both as the new and
	                                  expected (old contents) parameters for --set
	                                  operations
	  -c, --condition TEXT            Expected hash. Only perform the action if the
	                                  branch currently points to the hash specified
	                                  by this option.
	  -r, --ref TEXT                  branch to list from. If not supplied the
	                                  default branch from config is used
	  -m, --message TEXT              commit message
	  -t, --type TEXT                 entity types to filter on, if no entity types
	                                  are passed then all types are returned
	  --query, --query-expression TEXT
	                                  Allows advanced filtering using the Common
	                                  Expression Language (CEL). An intro to CEL can
	                                  be found at https://github.com/google/cel-
	                                  spec/blob/master/doc/intro.md. Some examples
	                                  with usable variables 'entry.namespace'
	                                  (string) & 'entry.contentType' (string) are:
	                                  entry.namespace.startsWith('a.b.c')
	                                  entry.contentType in
	                                  ['ICEBERG_TABLE','DELTA_LAKE_TABLE']
	                                  entry.namespace.startsWith('some.name.space')
	                                  && entry.contentType in
	                                  ['ICEBERG_TABLE','DELTA_LAKE_TABLE']
	  --author TEXT                   The author to use for the commit
	  --help                          Show this message and exit.
	
	

