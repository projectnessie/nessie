# Nessie Perf-Test Measurement Pack

The pack of Docker containers (Grafana + Prometheus + Jaeger) that are used to collect metrics
for performance tests, which act as the backend when running a performance test when both the Nessie
server and load generators run on the host.

Containers:
* Grafana to display metrics. Pre-configured with three dashboards:
  * JVM-metrics (only useful when running Nessie not as a native image)
  * "Nessie" to view the metrics collected by Nessie
  * "Nessie-Benchmark" to view the metrics collected by the Gatling based benchmarks/perf-test
* Prometheus. Pre-configured to use `prometheus-data/data` in the current directory to store the
  metrics, so you can easily collect the metrics and transfer those to another machine for later
  inspection: just startup another "measurement-pack" for example on your laptop with the
  `prometheus-data/data` directory pre-populated.
* Prometheus Push-Gateway. This is used by the Gatling benchmark/perf-test to push the metrics.
  Note: Prometheus Push-Gateway is *not* meant for production use, only for one-off-ish scenarios.
* Jaeger. To collect traces. Both Nessie and the Nessie-Gatling stuff can emit trace inforrmation,
  but must be configured accordingly. For Nessie, use the Quarkus environment variables, for
  the benchmarks/perf-tests use the "raw" Jaeger environment variables (starting with `JAEGER_`).
* DynamoDB. A local DynamoDB, if running the tests on your local machine.
* Docker-Host. A Docker container to let the Docker container access services (= Nessie) running
  on your local machine.
* Cadvisor. To inspect the Docker containers.

## Starting the measurement-pack

1. Go to the `perftest/measurement-pack` directory 
1. Setup [docker compose](https://docs.docker.com/compose/install/)
1. Pre-create the Prometheus data directory:
  - `mkdir -p prometheus-data/data`
  - `chmod -R o+w prometheus-data`
1. Execute `docker-compose up -d`.
   This will start:
  - [Grafana](http://localhost:3000) - for plotting prometheus metrics
  - [Prometheus](http://localhost:9090) - collecting load, dynamo usage etc from server
  - [Jaeger](http://localhost:16686) - trace executions
  - Cadvisor - docker image and host metrics
  - Local-DynamoDB - for dynamodb and future AWS services

## Troubleshooting tip: If Prometheus cannot scrape your Nessie instance

This "pack of Docker containers for measurement" accesses Nessie running on your machine
from the Docker containers.

If Prometheus accesses time out, make sure your local firewall does not block TCP traffic from
the Docker containers to your machine. Your machine's blocking TCP traffic, if Prometheus complains
about "context timeout" or so in http://localhost:9090/classic/targets
