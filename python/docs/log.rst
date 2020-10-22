.. code-block:: bash

	Usage: cli log [OPTIONS] [REVISION_RANGE] [PATHS]...
	
	  Show commit log.
	
	  REVISION_RANGE optional branch, tag or hash to start viewing log from. If of
	  the form <hash>..<hash> only show log for given range
	
	  PATHS optional list of paths. If given, only show commits which affected the
	  given paths
	
	Options:
	  -n, --number INTEGER    number of log entries to return
	  --since, --after TEXT   Commits more recent than specific date
	  --until, --before TEXT  Commits older than specific date
	  --author, --committer   limit commits to specific committer
	  --help                  Show this message and exit.
	
	

