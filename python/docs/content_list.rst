.. code-block:: bash

   Usage: nessie content list [OPTIONS]
   
     List content.
   
     Examples:
   
         nessie content list -r dev -> List all contents in 'dev' branch.
   
         nessie content list -r dev -t DELTA_LAKE_TABLE ->  List all contents in
         'dev' branch with type `DELTA_LAKE_TABLE`.
   
         nessie content list -r dev --query
         "entry.namespace.startsWith('some.name.space')" -> List all contents in
         'dev' branch that start with 'some.name.space'
   
   Options:
     -r, --ref TEXT                  Branch to list from. If not supplied the
                                     default branch from config is used
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
     --help                          Show this message and exit.
   
   

