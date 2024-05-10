#!/usr/bin/env bash
#
# Copyright (C) 2024 Dremio
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

trap '[[ -n $(jobs -p) ]] && kill $(jobs -p); ${DOCKER} stop $FLINK_CONTAINER_ID > /dev/null 2>&1; ${DOCKER} rm $FLINK_CONTAINER_ID > /dev/null 2>&1' EXIT
set -e

PROJECT_DIR=$(dirname "$0")/../..
cd "$PROJECT_DIR"
PROJECT_DIR=$(pwd)

# Set the default values
ICEBERG_VERSION="1.5.0"
FLINK_VERSION="1.17.2" # 1.18 is not supported by Iceberg
SCALA_VERSION="2.12"
WAREHOUSE_LOCATION="$PROJECT_DIR/build/flink-warehouse"
FLINK_TEMP_DIR=/tmp/flink

# Parse the command line arguments
while [[ $# -gt 0 ]]
do
  key="$1"
  case $key in
    --iceberg-version)
      ICEBERG_VERSION="$2"
      shift
      shift
      ;;
    --flink-version)
      FLINK_VERSION="$2"
      shift
      shift
      ;;
    --scala-version)
      SCALA_VERSION="$2"
      shift
      shift
      ;;
    --warehouse)
      WAREHOUSE_LOCATION="$2"
      shift
      shift
      ;;
    --container-runtime)
      DOCKER="$2"
      shift
      shift
      ;;
    --no-publish)
      NO_PUBLISH_TO_MAVEN_LOCAL="true"
      shift
      ;;
    --no-clear-cache)
      NO_CLEAR_IVY_CACHE="true"
      shift
      ;;
    --no-nessie-start)
      NO_NESSIE_START="true"
      shift
      ;;
    --clear-warehouse)
      CLEAR_WAREHOUSE="true"
      shift
      ;;
    --debug)
      DEBUG="true"
      shift
      ;;
    -v | --verbose)
      VERBOSE="true"
      shift
      ;;
    -h | --help)
      HELP="true"
      shift
      ;;
    *)
      echo "Unknown option $1"
      exit 1
      ;;
  esac

if [[ -n "$HELP" ]]; then
  echo "Usage: flink.sh [options]"
  echo "Options:"
  echo "  --iceberg-version <version>     Iceberg version to use. Default: $ICEBERG_VERSION"
  echo "  --flink-version <version>       Flink version to use. Default: $FLINK_VERSION"
  echo "  --scala-version <version>       Scala version to use. Default: $SCALA_VERSION"
  echo "  --warehouse <location>          Warehouse location. Default: $WAREHOUSE_LOCATION"
  echo "  --container-runtime <runtime>   Container runtime to use. Default: podman if available, docker otherwise"
  echo "  --no-publish                    Do not publish jars to Maven local. Default: false"
  echo "  --no-clear-cache                Do not clear ivy cache. Default: false"
  echo "  --no-nessie-start               Do not start Nessie Core/Catalog, use externally provided instance(s). Default: start"
  echo "  --clear-warehouse               Clear warehouse directory. Default: false"
  echo "  --debug                         Enable debug mode"
  echo "  --verbose                       Enable verbose mode"
  echo "  --help                          Print this help"
  exit 0
fi

done

source "$PROJECT_DIR/catalog/bin/common.sh"

touch ~/.flink-sql-history

FLINK_VERSION_MAJOR_MINOR=$(echo "$FLINK_VERSION" | cut -d'.' -f1-2)

rm -rf $FLINK_TEMP_DIR
mkdir -p $FLINK_TEMP_DIR/project
mkdir -p $FLINK_TEMP_DIR/lib

cat <<EOF > $FLINK_TEMP_DIR/project/settings.gradle
rootProject.name = 'iceberg-flink-dependencies'
EOF

cat <<EOF > $FLINK_TEMP_DIR/project/build.gradle
apply plugin: 'java'
repositories {
  mavenCentral()
  mavenLocal()
}
dependencies {
  implementation("org.apache.iceberg:iceberg-flink-runtime-${FLINK_VERSION_MAJOR_MINOR}:${ICEBERG_VERSION}")
  implementation("org.apache.hadoop:hadoop-client-runtime:3.3.4")
}
configurations {
  runtimeClasspath {
    exclude group: 'com.google.code.findbugs'
    exclude group: 'commons-cli'
    exclude group: 'commons-codec'
    exclude group: 'commons-daemon'
    exclude group: 'commons-io'
    exclude group: 'commons-lang'
    exclude group: 'commons-pool'
    exclude group: 'edu.umd.cs.findbugs'
    exclude group: 'javax.annotation'
    exclude group: 'javax.xml.bind'
    exclude group: 'org.apache.commons'
    exclude group: 'org.apache.flink', module: 'flink-core'
    exclude group: 'org.apache.flink', module: 'flink-runtime'
    exclude group: 'org.apache.flink', module: 'flink-java'
    exclude group: 'org.apache.flink', module: 'flink-shaded-guava'
    exclude group: 'org.apache.httpcomponents'
    exclude group: 'org.codehaus.jackson'
    exclude group: 'org.slf4j'
    exclude group: 'org.xerial.snappy'
  }
}

tasks.register('copyToLib', Copy) {
  into "$FLINK_TEMP_DIR/lib"
  from configurations.runtimeClasspath
}
EOF

./gradlew "${GRADLE_OPTS[@]}" -p $FLINK_TEMP_DIR/project copyToLib

if [[ -n "$DEBUG" ]]; then
  FLINK_JOBM_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5007"
  FLINK_TSKM_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5008"
fi

FLINK_CATALOG_URI="http://${CONTAINER_HOST}:19120/iceberg/main"
FLINK_CATALOG_IMPL=org.apache.iceberg.rest.RESTCatalog

if [[ -z "$VERBOSE" ]]; then
  FLINK_JOBM_PROMPT="[FLINK JOBM] "
  FLINK_TSKM_PROMPT="[FLINK TSKM] "
else
  FLINK_JOBM_PROMPT=$(printf "\r\033[1m\033[34m[FLINK JOBM] \033[0m")
  FLINK_TSKM_PROMPT=$(printf "\r\033[1m\033[35m[FLINK TSKM] \033[0m")
fi

sleep 1
echo "Configuring Flink..."

FLINK_CONTAINER_ID=$(${DOCKER} run --detach --rm -p 8081:8081 -p 5007:5007 -p 5008:5008 \
  --volume "$WAREHOUSE_LOCATION":"$WAREHOUSE_LOCATION" \
  --volume ~/.flink-sql-history:/root/.flink-sql-history \
  -e JVM_ARGS="$FLINK_JOBM_DEBUG_OPTS" \
  docker.io/flink:"${FLINK_VERSION}"-scala_"${SCALA_VERSION}" \
  bash -c 'while [ ! -f /tmp/ready ]; do sleep 0.1; done; /opt/flink/bin/jobmanager.sh start-foreground')

${DOCKER} cp $FLINK_TEMP_DIR/lib "$FLINK_CONTAINER_ID":/opt/flink > /dev/null

if [[ -n "$DEBUG" ]]; then
  for file in log4j log4j-console log4j-session; do
    ${DOCKER} exec "$FLINK_CONTAINER_ID" sh -c 'cat <<EOF >> /opt/flink/conf/'$file'.properties
logger.iceberg.name = org.apache.iceberg
logger.iceberg.level = DEBUG
logger.nessie.name = org.projectnessie
logger.nessie.level = DEBUG
EOF'
  done
fi

echo "Starting Flink Job Manager..."

${DOCKER} exec "$FLINK_CONTAINER_ID" touch /tmp/ready
${DOCKER} logs -f "$FLINK_CONTAINER_ID" 2>&1 | sed ''s/^/"$FLINK_JOBM_PROMPT"/'' >> "$REDIRECT" &

# Wait for the server to finish startup by grepping the logs in a subshell
( ${DOCKER} logs -f "$FLINK_CONTAINER_ID" 2>&1 & ) | grep -q "Starting the resource manager."

echo "Starting Flink Task Manager..."
${DOCKER} exec "$FLINK_CONTAINER_ID" sh -c 'export JVM_ARGS="'"$FLINK_TSKM_DEBUG_OPTS"'"; /opt/flink/bin/taskmanager.sh start'
${DOCKER} exec "$FLINK_CONTAINER_ID" bash -c 'until [ -f /opt/flink/log/flink-*-taskexecutor-*.log ]; do sleep 0.1; done; tail -F /opt/flink/log/flink-*-taskexecutor-*.log' | sed ''s/^/"$FLINK_TSKM_PROMPT"/'' >> "$REDIRECT" &
( ${DOCKER} exec "$FLINK_CONTAINER_ID" bash -c 'until [ -f /opt/flink/log/flink-*-taskexecutor-*.log ]; do sleep 0.1; done; tail -F /opt/flink/log/flink-*-taskexecutor-*.log' 2>&1 & ) | grep -q "Successful registration at resource manager"

sleep 3
echo ""
echo "Flink started with container ID $FLINK_CONTAINER_ID"
echo "Flink Web UI available at http://localhost:8081"
echo ""
echo ""

if [[ -n "$DEBUG" ]]; then
  echo "Nessie Catalog debug port: 5006"
  echo "Flink Job Manager debug port: 5007"
  echo "Flink Task Manager debug port: 5008"
fi

echo "Launching Flink SQL Client..."
echo "You can try the following commands:"
echo ""

cat <<EOF
CREATE CATALOG nessie_catalog WITH (
  'type'                = 'iceberg',
  'catalog-impl'        = '$FLINK_CATALOG_IMPL',
  'uri'                 = '$FLINK_CATALOG_URI',
  'warehouse'           = 's3://demobucket/'
);
CREATE database nessie_catalog.db1;
CREATE TABLE nessie_catalog.db1.spotify (songid BIGINT, artist STRING, rating BIGINT);
INSERT INTO nessie_catalog.db1.spotify VALUES (2, 'drake', 3);
EOF

echo ""
sleep 1

# Note: in verbose mode, the last line in the prompt might be deleted by a line printed by Nessie
# or Flink; if that's concerning, switch to the non-verbose mode or append a newline before the ;
${DOCKER} exec -it "${FLINK_CONTAINER_ID}" bash -c 'unset JVM_ARGS; /opt/flink/bin/sql-client.sh embedded'
