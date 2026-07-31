```
Usage: nessie-server-admin-tool-runner.jar cleanup-repository [-hV]
       [--allow-duplicate-commit-traversal] [--dry-run]
       [--allowed-fpp=<allowedFalsePositiveProbability>]
       [--commit-rate=<resolveCommitRatePerSecond>]
       [--fpp=<falsePositiveProbability>] [--obj-count=<expectedObjCount>]
       [--obj-rate=<resolveObjRatePerSecond>]
       [--pending-objs-batch-size=<pendingObjsBatchSize>]
       [--purge-obj-rate=<purgeDeleteObjRatePerSecond>]
       [--recent-objs-ids-filter-size=<recentObjIdsFilterSize>]
       [--referenced-grace=<objReferencedGrace>]
       [--scan-obj-rate=<purgeScanObjRatePerSecond>]
Cleanup unreferenced data from Nessie's repository.
This is a two-phase implementation that first identifies the objects that are
referenced and the second phase scans the whole repository and deletes objects
that are unreferenced.
It is recommended to run this command regularly, but with appropriate rate
limits using the --commit-rate, --obj-rate, --scan-obj-rate, --purge-obj-rate
which does not overload your backend database system.
The implementation uses a bloom-filter to identify the IDs of referenced
objects. The default setting is to allow for 1000000 objects in the backend
database with an FPP of 1.0E-5. These values should serve most repositories.
However, if your repository is quite big, you should supply a higher expected
object count using the --obj-count option. If the implementation detected that
the bloom-filter would exceed the maximum allowed FPP, it would restart with a
higher number of expected objects.
In rare situations with an extremely huge amount of objects, the data
structures may require a lot of memory. The estimated heap pressure for the
contextual data structures is printed to the console.
If you are unsure whether this command works fine, specify the --dry-run option
to perform all operations except deleting objects.
      --allow-duplicate-commit-traversal
                  Allow traversal of the same commit more than once. This is
                    disabled by default.
      --allowed-fpp=<allowedFalsePositiveProbability>
                  Maximum allowed false-positive-probability to detect
                    referenced objects, defaults to 1.0E-4.
      --commit-rate=<resolveCommitRatePerSecond>
                  Allowed number of commits to process during the 'resolve'
                    phase per second. Default is unlimited.
      --dry-run   Perform all operations, but do not delete any object .
      --fpp=<falsePositiveProbability>
                  Default false-positive-probability to detect referenced
                    objects, defaults to 1.0E-5.
  -h, --help      Show this help message and exit.
      --obj-count=<expectedObjCount>
                  Number of expected objects, defaults to 1000000.
      --obj-rate=<resolveObjRatePerSecond>
                  Allowed number of objects to process during the 'resolve'
                    phase per second. Default is unlimited.
      --pending-objs-batch-size=<pendingObjsBatchSize>

      --purge-obj-rate=<purgeDeleteObjRatePerSecond>
                  Allowed number of objects to delete during the 'purge' phase
                    per second. Default is unlimited.
      --recent-objs-ids-filter-size=<recentObjIdsFilterSize>
                  Size of the filter to recognize recently processed objects.
                    This helps to reduce effort, but should be kept to a
                    reasonable number. Defaults to 100000.
      --referenced-grace=<objReferencedGrace>
                  Grace-time for newly created objects to not be deleted.
                    Default is just "now". Specified using the ISO-8601 format,
                    for example P1D (24 hours) or PT2H (2 hours) or P10D12H (10
                    * 24 + 10 hours).
      --scan-obj-rate=<purgeScanObjRatePerSecond>
                  Allowed number of objects to scan during the 'purge' phase
                    per second. Default is unlimited.
  -V, --version   Print version information and exit.

```
