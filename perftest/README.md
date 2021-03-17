# Running Benchmarks against Nessie

Currently, Nessie performance testing consists of two main components:
* The "measurement pack", which is a collection of Docker images using Docker Compose that contains
  a local DynamoDB mock, Prometheus + Push-Gateway, Grafana, Jaeger and Docker-Host.
* The Gatling scenario and simulation to simulate commits against Nessie.

Nessie currently itself runs as a separate instance, so you have to explicitly start it in the
configuration you want run. This is convenient to try and test different configuration and code
changes locally.

For more information, look at the `README.md` files in the sub-modules `measurement-pack` and `gatling`.

## Quick start in a local machine

1. Start the measurement-pack:
  ```shell
  cd perftest/measurement-pack
  mkdir -p prometheus-data/data/
  chmor -R o+w prometheus-data
  docker-compose up 
  ```
  (Install [docker compose](https://docs.docker.com/compose/install/))
1. Build & Start Nessie
  ```shell
  export AWS_ACCESS_KEY_ID=xxx
  export AWS_SECRET_ACCESS_KEY=xxx
  export QUARKUS_DYNAMODB_AWS_REGION=us-west-2
  export QUARKUS_DYNAMODB_ENDPOINT_OVERRIDE=http://127.0.0.1:8000
  export NESSIE_SERVER_SEND_STACKTRACE_TO_CLIENT=FALSE
  export NESSIE_VERSION_STORE_TYPE=DYNAMO
  export NESSIE_VERSION_STORE_DYNAMO_INITIALIZE=true
  export QUARKUS_JAEGER_ENDPOINT=http://127.0.0.1:14268/api/traces 
  export QUARKUS_JAEGER_SAMPLER_TYPE=const
  export QUARKUS_JAEGER_SAMPLER_PARAM=0
  # Don't emit the HTTP access log
  export HTTP_ACCESS_LOG_LEVEL=WARN

  ./mvnw clean install -DskipTests

  java -Xms6g -Xmx6g -XX:+AlwaysPreTouch -jar servers/quarkus-server/target/quarkus-app/quarkus-run.jar
  ```
1. Start the Gatling based tests
  ```shell
  ./mvnw install gatling:test \
    -Dgatling.simulationClass=org.projectnessie.perftest.gatling.CommitToBranchSimulation \
    -Dsim.users=5 \
    -Dsim.commits=0 \
    -Dsim.prometheus=127.0.0.1:9091 \
    -Dsim.duration.seconds=60 \
    -Dsim.rate=5 \
    -Dsim.branchMode=SINGLE_BRANCH_TABLE_PER_USER \
    -Dhttp.maxConnections=100 \
    -pl :nessie-perftest-gatling
  ```
1. Inspect the metrics, open [Grafana](https://localhost:3000/)
  * Dashboard for [JVM metrics](http://localhost:3000/d/Y0ObmOsMz/jvm-micrometer)
  * Dashboard for [Nessie Server](http://localhost:3000/d/itt84dyMz/nessie)
  * Dashboard for [Nessie Benchmark](http://localhost:3000/d/itt84dyMy/nessie-benchmark)
1. Play around & run more tests

## Provision the load-driver EC2 instance

Provision the machine running the "measurement pack" and Nessie and Gatling tests. Some examples
that need to be inspected and adopted to your own environments are in the 'ec2-sample-scripts'
subfolder.

In our examples, we were running Ubuntu Server 20.04.

### Start the "measurement pack"
```shell

# cd to the Nessie source directory

cd perftest/measurement-pack

mkdir -p prometheus-data/data
chmod -R o+w prometheus-data

docker-compose up
```

### Start Nessie
```shell

# TODO replace with your credentials! See notes below!
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx

# Start the Nessie server
```

### Start the benchmark
```shell

# cd to the Nessie source directory

# See perftest/gatling/README.md for the perftest parameters.

##############################
## Example tests runs 
##############################

./mvnw install gatling:test -Dgatling.simulationClass=org.projectnessie.perftest.gatling.CommitToBranchSimulation \
  -Dsim.users=5 \
  -Dsim.commits=0 \
  -Dsim.mode=BRANCH_PER_USER \
  -Dsim.duration.seconds=600 \
  -Dsim.rate=10 \
  -Dhttp.maxConnections=100 \
  -pl :nessie-perftest-gatling ; sleep 60 ; \
\
./mvnw install gatling:test -Dgatling.simulationClass=org.projectnessie.perftest.gatling.CommitToBranchSimulation \
  -Dsim.users=20 \
  -Dsim.commits=0 \
  -Dsim.mode=BRANCH_PER_USER \
  -Dsim.duration.seconds=600 \
  -Dsim.rate=10 \
  -Dhttp.maxConnections=100 \
  -pl :nessie-perftest-gatling ; sleep 60 ; \
\
./mvnw install gatling:test -Dgatling.simulationClass=org.projectnessie.perftest.gatling.CommitToBranchSimulation \
  -Dsim.users=5 \
  -Dsim.commits=0 \
  -Dsim.mode=SINGLE_BRANCH_TABLE_PER_USER \
  -Dsim.duration.seconds=600 \
  -Dsim.rate=10 \
  -Dhttp.maxConnections=100 \
  -pl :nessie-perftest-gatling
```

### Stop

After running the benchmarks, stop the Docker(-compose) containers

### Pull Prometheus data onto your local machine
```shell
# GO TO YOUR LOCAL NESSIE CLONE
cd perftest/measurement-pack
rm -rf prometheus-data
# TODO check the server path here
scp -r -i ~/.ssh/${YOUR_PRIVATE_KEY_FILE} ubuntu@${INSTANCE_IP}:nvm/nessie/perftest/measurement-pack/prometheus-data .
chmod -R o+w prometheus-data
docker-compose up
case $(uname -s) in Linux) xdg-open http://127.0.0.1:3000/ ;; Darwin) open http://127.0.0.1:3000/ ;; esac
```

# Hints

## ssh into the EC2 instance

This sample ssh command includes port redirections for Grafana, Prometheus and Nessie from
your local machine to the compute-instance. With these redirections, you do not need to open the
TCP ports for these services.

```shell
ssh -L 9090:127.0.0.1:9090 -L 3000:127.0.0.1:3000 -L 19120:127.0.0.1:19120 -i ~/.ssh/<YOUR-SSH-PRIVATE-KEY> ubuntu@<INSTANCE-IP>
```
