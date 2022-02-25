.. code-block:: bash

   Usage: nessie diff [OPTIONS] FROM_REF TO_REF

     Show diff between two given references.

     'from_ref'/'to_ref': name of branch or tag to use to show the diff

     'from_ref' and 'to_ref' can be either:

         The name of a reference (branch or tag), resolving to the HEAD of the
         named reference.

         The name of a reference (branch or tag) with a commit-id on that named
         reference.

         A detached commit-id/hash. Example:

     Examples:

         nessie diff from_ref to_ref -> show diff between 'from_ref' and 'to_ref',
         using the HEADs of both references

         nessie diff main@1234567890abcdef my-branch -> compare main branch at
         commit-id 1234567890abcdef with the HEAD of my-branch

         nessie diff 1234567890abcdef main -> compare the main branch w/ commit-id
         1234567890abcdef

   Options:
     --help  Show this message and exit.


