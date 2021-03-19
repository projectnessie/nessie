# PyNessie tests

PyNessie uses [vcr.py](https://vcrpy.readthedocs.io/) to execute tests. All test data is stored in
`tests/cassetes/{module}/{test}.yaml`. The unit tests then don't use the network or need a Nessie engine running to run.

If you add more tests or the Nessie server is changed tests can be run against a live Nessie engine by running
`pytest --record-mode=rewrite tests/` and adding the newly generated test data to git.

**NOTE** run the quarkus server with `-Dnessie.server.send-stacktrace-to-client=false` to reduce the size of the vcr
cassettes and create less noise in updates related to REST api changes
