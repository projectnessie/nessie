.. code-block:: bash

   Usage: nessie log [OPTIONS] [REF]

     Show commit log.

     REF name of branch or tag to use to show the commit logs

     Examples:

         nessie log -> show commit logs using the configured default branch

         nessie log dev -> show commit logs for 'dev' branch

         nessie log -n 5 dev -> show commit logs for 'dev' branch limited by 5
         commits

         nessie log --revision-range 12345678abcdef..12345678efghj dev -> show
         commit logs in range of hash '12345678abcdef' and '12345678efghj' in 'dev'
         branch

         nessie log --author nessie.user dev -> show commit logs for user
         'nessie.user' in 'dev' branch

         nessie log --filter "commit.author == 'nessie_user2' || commit.author ==
         'non_existing'" dev -> show commit logs using query in 'dev' branch

         nessie log --after "2019-01-01T00:00:00+00:00" --before
         "2021-01-01T00:00:00+00:00" dev -> show commit logs between
         "2019-01-01T00:00:00+00:00" and "2021-01-01T00:00:00+00:00" in 'dev'
         branch

   Options:
     -n, --number INTEGER            number of log entries to return
     --since, --after TEXT           Only include commits newer than specific date,
                                     such as '2001-01-01T00:00:00+00:00'
     --until, --before TEXT          Only include commits older than specific date,
                                     such as '2999-12-30T23:00:00+00:00'
     --author TEXT                   Limit commits to a specific author (this is
                                     the original committer). Supports specifying
                                     multiple authors to filter by.
     --committer TEXT                Limit commits to a specific committer (this is
                                     the logged in user/account who performed the
                                     commit). Supports specifying multiple
                                     committers to filter by.
     -r, --revision-range TEXT       Hash to start viewing log from. If of the form
                                     '<start_hash>'..'<end_hash>' only show log for
                                     given range on the particular ref that was
                                     provided, the '<end_hash>' is inclusive and
                                     '<start_hash>' is exclusive.
     --filter TEXT
                                     Allows advanced filtering using the Common
                                     Expression Language (CEL). An intro to CEL can
                                     be found at https://github.com/google/cel-
                                     spec/blob/master/doc/intro.md. Some examples
                                     with usable variables 'commit.author' (string)
                                     / 'commit.committer' (string) /
                                     'commit.commitTime' (timestamp) /
                                     'commit.hash' (string) / 'commit.message'
                                     (string) / 'commit.properties' (map) are:
                                     commit.author=='nessie_author'
                                     commit.committer=='nessie_committer'
                                     timestamp(commit.commitTime) >
                                     timestamp('2021-06-21T10:39:17.977922Z')
     -x, --extended                  Retrieve all available information for the
                                     commit entries. This option will also return
                                     the operations for each commit and the parent
                                     hash. The schema of the JSON output will then
                                     produce a list of LogEntrySchema, otherwise a
                                     list of CommitMetaSchema.
     --help                          Show this message and exit.



