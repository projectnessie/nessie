# PyNessie tests

PyNessie uses [vcr.py](https://vcrpy.readthedocs.io/) to execute tests. All test data is stored in
`tests/cassetes/{module}/{test}.yaml`. The unit tests then don't use the network or need a Nessie engine running to run.

If you add more tests, change tests or the Nessie server, especially the REST API, is changed, tests
can be run against a live Nessie engine.

1. run the Nessie server to record new cassettes, it is sufficient to run
   `./mvnw quarkus:dev -am -pl :nessie-quarkus` locally.
   * Note: if the server is running in `dev` mode for a long time, it is necessary to reset its in-memory store
     every time the tests are re-recorded so that the "global" state from old test runs is erased and will not
     interfere with newer recordings. The reset can be performed by restarting the server or by pressing the `s` key
     in the Quarkus console (STDIN).
1. Run `pytest --record-mode=all tests/` (in a venv with `requirements_dev.txt` installed)
   to add test data for newly added tests and updated tests data.
   * Note: when running authentication tests (e.g. `test_nessie_cli_auth.py`) in recording more, the tests will most
     likely fail out-of-the box because the server will not recognize the auth tokens those tests use. In order to
     record cassettes for the authentication tests:
     1. Follow the instructions in the [Nessie Server README.md file](../../servers/quarkus-server/README.md) to start
        the server in test mode with a real authentication provider and generate a real auth token.
     1. Put real tokens into test sources
     1. Run tests in recording mode (e.g. `pytest --record-mode=all tests/test_nessie_cli_auth.py`)
     1. Manually replace real tokens with test tokens in the recorded cassettes and test sources
     1. Re-run the tests in validation mode (no recording) to make sure they work as expected
1. Be nice and only add the cassettes' yaml files, that have really changed.

Run the Quarkus server with `-Dnessie.server.send-stacktrace-to-client=false` (this is the default) to reduce
the size of the vcr cassettes and create less noise in updates related to REST api changes.
