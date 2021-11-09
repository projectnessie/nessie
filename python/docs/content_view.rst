.. code-block:: bash

   Usage: nessie content view [OPTIONS] KEY...
   
     View content.
   
         KEY is the content key that is associated with a specific content to view.
         This accepts as well multiple keys with space in between.
   
     Examples:
   
         nessie content view -r dev my_table -> View content details for content
         "my_table" in 'dev' branch.
   
   Options:
     -r, --ref TEXT  Branch to list from. If not supplied the default branch from
                     config is used.
     --help          Show this message and exit.
   
   

