# Performance testing Nessie with Gatling

Contains the code to run Gatling Simulations against Nessie.

It provides the plumbing of Nessie specifics like providing the `NessieClient` to the tested
actions (e.g. generate a Nessie commit) and wraps infrastructure code to optionally push metrics
to Prometheus via a Prometheus Push Gateway (in the future).

See the [Simulations README](../simulations/README.md).
