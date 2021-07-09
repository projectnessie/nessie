# PyNessie tests

PyNessie uses [vcr.py](https://vcrpy.readthedocs.io/) to execute tests. All test data is stored in
`tests/cassetes/{module}/{test}.yaml`. The unit tests then don't use the network or need a Nessie engine running to run.

If you add more tests, change tests or the Nessie server, especially the REST API, is changed, tests
can be run against a live Nessie engine.

1. run the Nessie server to record new cassettes, it is sufficient to run
   `./mvnw quarkus:dev -am -pl :nessie-quarkus` locally.
1. Run `pytest --record-mode=all tests/` (in a venv with `requirements_dev.txt` installed)
   to add test data for newly added tests and updated tests data.
1. Be nice and only add the cassettes' yaml files, that have really changed.

Run the Quarkus server with `-Dnessie.server.send-stacktrace-to-client=false` (this is the default) to reduce
the size of the vcr cassettes and create less noise in updates related to REST api changes.
