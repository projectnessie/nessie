.. code-block:: bash

   Usage: nessie merge [OPTIONS] [FROM_REF]

     Merge FROM_REF into another branch.

     FROM_REF can be a hash or branch.

     Examples:

         nessie merge -c 12345678abcdef dev -> merge dev to default branch with
         default branch's expected hash '12345678abcdef'

         nessie merge -b main -c 12345678abcdef dev -> merge dev to a branch named
         main with main branch's expected hash '12345678abcdef'

         nessie merge -b main -o 56781234abcdef -c 12345678abcdef dev -> merge dev
         at hash-on-ref '56781234abcdef' to main branch with main branch's expected
         hash '12345678abcdef'

         nessie merge -f -b main dev -> forcefully merge dev to a branch named main

   Options:
     -b, --branch TEXT       branch to merge onto. If not supplied the default
                             branch from config is used
     -f, --force             force branch assignment
     -c, --condition TEXT    Expected hash. Only perform the action if the branch
                             currently points to the hash specified by this option.
     -o, --hash-on-ref TEXT  Hash on merge-from-reference
     --help                  Show this message and exit.


