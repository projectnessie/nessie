.. code-block:: bash

   Usage: nessie reflog [OPTIONS]
   
     Show reflog.
   
     Reflog entries from the current HEAD or from the specified revision_range.
   
     Examples:
   
         nessie reflog -> show reflog entries from the current HEAD
   
         nessie reflog -n 5 -> show reflog entries from the current HEAD limited by
         5 entries
   
         nessie reflog --revision-range 12345678abcdef..12345678efghj -> show
         reflog entries in range of hash '12345678abcdef' and '12345678efghj' (both
         inclusive)
   
   Options:
     -n, --number INTEGER       number of reflog entries to return
     -r, --revision-range TEXT  Hash to start viewing reflog from. If of the form
                                '<start_hash>'..'<end_hash>' only show reflog for
                                given range on the particular ref that was
                                provided. Both the '<end_hash>' and '<start_hash>'
                                is inclusive.
     --help                     Show this message and exit.
   
   

