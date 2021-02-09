# Running Benchmarks against Nessie

Currently, Nessie performance testing consists of two main components:
* The "measurement pack", which is a collection of Docker images using Docker Compose that contains
  a local DynamoDB mock, Prometheus, Grafana and some more.
* The Gatling scenario and simulation to simulate commits against Nessie.

Nessie currently itself runs as a separate instance, so you have to explicitly start it in the
configuration you want run. This is convenient to try and test different configuration and code
changes locally.

For more information, look at the `README.md` files in the sub-modules `measurement-pack` and `gatling`.

## Quick start

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
  export NESSIE_VERSION_STORE_DYNAMO_CACHE_ENABLED=false
  export NESSIE_VERSION_STORE_DYNAMO_COMMIT_BACKOFF_RETRY_SLEEP=PT0S
  export NESSIE_VERSION_STORE_DYNAMO_P2_COMMIT_BACKOFF_RETRY_SLEEP=PT0S
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
1. Inspect the metrics, open [Grafana](https://127.0.0.1:3000/)
  * Dashboard for [JVM metrics](http://localhost:3000/d/Y0ObmOsMz/jvm-micrometer)
  * Dashboard for [Nessie Server](http://localhost:3000/d/itt84dyMz/nessie)
  * Dashboard for [Nessie Benchmark](http://localhost:3000/d/itt84dyMy/nessie-benchmark)
1. Play around & run more tests

## Example against AWS / EC2 / DynamoDB

Contents of `~/.aws/config`:
```
[default]
region=us-west-2
output=json
```

```shell
# Assumes AWS CLI credentials are in ~/.aws/credentials or the necessary env vars are configured.

#       m5d.8xlarge   32 vCPU,    128 G RAM,    2x 600 G NVMe SSD
#       m5d.4xlarge   16          64            2x 300
# -->   m5d.12xlarge  48          192           2x 900
#       m5d.16xlarge  64          256           4x 600
#       m5d.24xlarge  96          384           4x 900
#       m5d.metal     96          384           4x 900
#       m5dn...
```

## Provision the load-driver EC2 instance

`ssh` into the provisioned EC2 instance (the following assumes Ubuntu 20.04 Server).

### Initial provisioning scripts

#### Initial provisioning script when building Nessie on the EC2 instance
```shell

# Mandatory stuff (Docker, compiler, etc)
sudo apt-get update
sudo apt-get install -y wget git gcc g++ zlib1g-dev docker.io docker-compose
sudo usermod -a -G docker ubuntu

# Provision the NVMe SSD, mount in /home/ubuntu/nvm
mkdir nvm
sudo mkfs.ext4 -E nodiscard /dev/nvme1n1
sudo mount /dev/nvme1n1 /home/ubuntu/nvm
sudo chown ubuntu: nvm

cd nvm

# Put the Docker work directory on the NVMe SSD
sudo mkdir -p var-lib-docker
sudo ln -s $(pwd)/var-lib-docker /var/lib/docker

# Download and install GrallVM CE
wget https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-21.0.0.2/graalvm-ce-java11-linux-amd64-21.0.0.2.tar.gz
tar xfz graalvm-ce-java11-linux-*.tar.gz
ln -s $(find . -name "graalvm-ce-java11*" -type d -maxdepth 1) jdk

# Update environment
export JAVA_HOME=$(pwd)/jdk
export PATH=$JAVA_HOME/bin:$PATH

cat >> ~/.bashrc <<!

export JAVA_HOME=$JAVA_HOME
export PATH=\$JAVA_HOME/bin:\$PATH
!

# Install the GraalVM 'native-image' extension
gu install native-image

# Clone Nessie (this is from my personal branch!)
git clone -o snazy -b trace-commit https://github.com/snazy/nessie.git nessie
cd nessie

# Prepare the directory receiving the Prometheus data (so we can download it)
sudo mkdir perftest/measurement-pack/prometheus-data
sudo chmod o+w perftest/measurement-pack/prometheus-data

cat >> ~/.bashrc <<!

# Environment for the nessie server
export NESSIE_VERSION_STORE_TYPE=DYNAMO
export NESSIE_VERSION_STORE_DYNAMO_INITIALIZE=false
export NESSIE_VERSION_STORE_DYNAMO_CACHE_ENABLED=true
export NESSIE_VERSION_STORE_DYNAMO_COMMIT_BACKOFF_RETRY_SLEEP=PT0S
export NESSIE_VERSION_STORE_DYNAMO_P2_COMMIT_BACKOFF_RETRY_SLEEP=PT0S
export QUARKUS_DYNAMODB_AWS_REGION=us-west-2
export QUARKUS_DYNAMODB_ENDPOINT_OVERRIDE=http://dynamodb.${QUARKUS_DYNAMODB_AWS_REGION}.amazonaws.com
export QUARKUS_LOG_FILE_ENABLE=true
export QUARKUS_JAEGER_ENDPOINT=http://127.0.0.1:14268/api/traces 
export QUARKUS_JAEGER_SAMPLER_TYPE=const
export QUARKUS_JAEGER_SAMPLER_PARAM=0

# Environment for Gatling tests
export JAEGER_ENDPOINT=http://127.0.0.1:14268/api/traces
export JAEGER_SAMPLER_TYPE=const
export JAEGER_SAMPLER_PARAM=0
export JAEGER_SERVICE_NAME=nessie 
!

# Build Nessie
./mvnw clean install -DskipTests -Pnative

echo "GROUP docker ADDED TO USER ubuntu. LOGOUT AND LOGIN AGAIN !"
```

#### Initial provisioning script (not building Nessie on the EC2 instance)
```shell

# Mandatory stuff (Docker, compiler, etc)
suto apt-get update
sudo apt-get install -y git docker.io docker-compose
sudo usermod -a -G docker ubuntu

# Provision the NVMe SSD, mount in /home/ubuntu/nvm
sudo mkfs.ext4 -E nodiscard /dev/nvme1n1
sudo mount /dev/nvme1n1 /home/ubuntu/nvm
sudo chown ubuntu: nvm

cd nvm

# Put the Docker work directory on the NVMe SSD
sudo mkdir -p var-lib-docker
sudo ln -s $(pwd)/var-lib-docker /var/lib/docker

# Download and install GrallVM CE (or any other Java 11 compatible JVM) 
wget https://github.com/graalvm/graalvm-ce-builds/releases/download/vm-21.0.0.2/graalvm-ce-java11-linux-amd64-21.0.0.2.tar.gz
tar xfz graalvm-ce-java11-linux-*.tar.gz
ln -s $(find . -name "graalvm-ce-java11*" -type d -maxdepth 1) jdk

# Update environment
export JAVA_HOME=$(pwd)/jdk
export PATH=$JAVA_HOME/bin:$PATH

cat >> ~/.bashrc <<!

export JAVA_HOME=$JAVA_HOME
export PATH=\$JAVA_HOME/bin:\$PATH
!

# Clone Nessie (this is from my personal branch!)
git clone -o snazy -b trace-commit https://github.com/snazy/nessie.git nessie
cd nessie

# Prepare the directory receiving the Prometheus data (so we can download it)
sudo mkdir perftest/measurement-pack/prometheus-data
sudo chmod o+w perftest/measurement-pack/prometheus-data

cat >> ~/.bashrc <<!

# Environment for the nessie server
export NESSIE_VERSION_STORE_TYPE=DYNAMO
export NESSIE_VERSION_STORE_DYNAMO_INITIALIZE=false
export NESSIE_VERSION_STORE_DYNAMO_CACHE_ENABLED=true
export NESSIE_VERSION_STORE_DYNAMO_COMMIT_BACKOFF_RETRY_SLEEP=PT0S
export NESSIE_VERSION_STORE_DYNAMO_P2_COMMIT_BACKOFF_RETRY_SLEEP=PT0S
export QUARKUS_DYNAMODB_AWS_REGION=us-west-2
export QUARKUS_DYNAMODB_ENDPOINT_OVERRIDE=http://dynamodb.${QUARKUS_DYNAMODB_AWS_REGION}.amazonaws.com
export QUARKUS_LOG_FILE_ENABLE=false
export QUARKUS_JAEGER_ENDPOINT=http://127.0.0.1:14268/api/traces
# Disable Jaeger tracing for our benchmark 
export QUARKUS_JAEGER_SAMPLER_TYPE=const
export QUARKUS_JAEGER_SAMPLER_PARAM=0

# Environment for Gatling tests
export JAEGER_ENDPOINT=http://127.0.0.1:14268/api/traces
export JAEGER_SERVICE_NAME=nessie 
# Disable Jaeger tracing for our benchmark 
export JAEGER_SAMPLER_TYPE=const
export JAEGER_SAMPLER_PARAM=0
!

echo "GROUP docker ADDED TO USER ubuntu. LOGOUT AND LOGIN AGAIN !"
```

### Start the "measurement pack"
```shell
# On the EC2 instance
cd nvm/nessie

cd perftest/measurement-pack

mkdir -p prometheus-data/data
chmod -R o+w prometheus-data

docker-compose up
```

### Start Nessie
```shell
# On the EC2 instance
cd nvm/nessie

# TODO replace with your credentials! See notes below!
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx

# NOTE: Path is valid when building the native image locally.
servers/quarkus-server/target/nessie-quarkus-*-SNAPSHOT-runner
```

### Start the benchmark
```shell
# On the EC2 instance
cd nvm/nessie

# See perftest/gatling/README.md for the perftest parameters.

##############################
## Tests follow 
##############################

./mvnw install gatling:test -Dgatling.simulationClass=org.projectnessie.perftest.gatling.CommitToBranchSimulation \
  -Dsim.users=5 \
  -Dsim.commits=0 \
  -Dsim.branchMode=BRANCH_PER_USER \
  -Dsim.duration.seconds=600 \
  -Dsim.rate=10 \
  -Dhttp.maxConnections=100 \
  -pl :nessie-perftest-gatling ; sleep 60 ; \
\
./mvnw install gatling:test -Dgatling.simulationClass=org.projectnessie.perftest.gatling.CommitToBranchSimulation \
  -Dsim.users=20 \
  -Dsim.commits=0 \
  -Dsim.branchMode=BRANCH_PER_USER \
  -Dsim.duration.seconds=600 \
  -Dsim.rate=10 \
  -Dhttp.maxConnections=100 \
  -pl :nessie-perftest-gatling ; sleep 60 ; \
\
./mvnw install gatling:test -Dgatling.simulationClass=org.projectnessie.perftest.gatling.CommitToBranchSimulation \
  -Dsim.users=5 \
  -Dsim.commits=0 \
  -Dsim.branchMode=SINGLE_BRANCH_TABLE_PER_USER \
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
scp -r -i ~/.ssh/${YOUR_PRIVATE_KEY_FILE} ubuntu@${INSTANCE_IP}:nvm/nessie/perftest/measurement-pack/prometheus-data .
chmod -R o+w prometheus-data
docker-compose up
xdg-open http://127.0.0.1:3000/
```

# Notes

## Creating an AWS/IAM access-key

```shell
aws iam create-user --user-name nessie-perftest
# requires a pre-existing group dynamodb-full
aws iam add-user-to-group --group-name dynamodb-full --user-name nessie-perftest
aws iam create-access-key --user nessie-perftest

# Setup the credentials for the Nessie instance being run using the values returned by "create-access-key"
export AWS_ACCESS_KEY_ID=xxx
export AWS_SECRET_ACCESS_KEY=xxx

# Cleanup
aws iam delete-access-key --access-key-id ${AWS_ACCESS_KEY} --user-name nessie-perftest
```

## ssh into the EC2 instance

```
ssh -L 9090:127.0.0.1:9090 -L 3000:127.0.0.1:3000 -L 19120:127.0.0.1:19120 -i ~/.ssh/<YOUR-SSH-PRIVATE-KEY> ubuntu@<INSTANCE-IP>

ssh -i ~/.ssh/<YOUR-SSH-PRIVATE-KEY> ubuntu@<INSTANCE-IP>
```
