#
# Copyright (C) 2020 Dremio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

## READ CAREFULLY AND UNDERSTAND BEFORE RUNNING COMMANDS FROM THIS SCRIPT!

##############################################################################################
##
## Hack-ish ad-hoc provisioning script for EC2 instance w/ local NVMe + benchmarking Nessie
## from source.
##
## Read and inspect carefully before applying the changes.
##
## USE AT YOUR OWN RISK!

## Some AWS EC2 instance types
#       m5d.8xlarge   32 vCPU,    128 G RAM,    2x 600 G NVMe SSD
#       m5d.4xlarge   16          64            2x 300
# -->   m5d.12xlarge  48          192           2x 900
#       m5d.16xlarge  64          256           4x 600
#       m5d.24xlarge  96          384           4x 900
#       m5d.metal     96          384           4x 900
#       m5dn...

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
git clone https://github.com/projectnessie/nessie.git
cd nessie

# Prepare the directory receiving the Prometheus data (so we can download it)
sudo mkdir perftest/measurement-pack/prometheus-data
sudo chmod o+w perftest/measurement-pack/prometheus-data

cat >> ~/.bashrc <<!

# Environment for the nessie server
export NESSIE_VERSION_STORE_TYPE=DYNAMO
export NESSIE_VERSION_STORE_DYNAMO_INITIALIZE=false
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