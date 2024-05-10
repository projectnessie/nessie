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

trap '[[ -n $(jobs -p) ]] && kill $(jobs -p); ${DOCKER} stop $TRINO_CONTAINER_ID > /dev/null 2>&1; ${DOCKER} rm $TRINO_CONTAINER_ID > /dev/null 2>&1' EXIT
set -e

PROJECT_DIR=$(dirname "$0")/../..
cd "$PROJECT_DIR"
PROJECT_DIR=$(pwd)

# Set the default values
ICEBERG_VERSION="1.5.0"
TRINO_VERSION="438"
WAREHOUSE_LOCATION="$PROJECT_DIR/build/trino-warehouse"
TRINO_TEMP_DIR=/tmp/trino-catalog

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
    --trino-version)
      TRINO_VERSION="$2"
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
  echo "Usage: trino.sh [options]"
  echo "Options:"
  echo "  --iceberg-version <version>     Iceberg version to use. Default: $ICEBERG_VERSION"
  echo "  --trino-version <version>       Trino version to use. Default: $TRINO_VERSION"
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

rm -rf $TRINO_TEMP_DIR
mkdir -p $TRINO_TEMP_DIR/catalog

if [[ -n "$DEBUG" ]]; then
  TRINO_LOG_LEVEL="DEBUG"
  TRINO_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5007"
else
  TRINO_LOG_LEVEL="INFO"
fi

# defaults taken from https://github.com/trinodb/charts/blob/main/charts/trino/templates/configmap-coordinator.yaml

cat <<EOF > "$TRINO_TEMP_DIR/log.properties"
io.trino=INFO
io.trino.plugin.iceberg=$TRINO_LOG_LEVEL
org.apache.iceberg=$TRINO_LOG_LEVEL
org.projectnessie=$TRINO_LOG_LEVEL
EOF

#
cat <<EOF > "$TRINO_TEMP_DIR/jvm.config"
-server
-Xmx512m
-XX:+UseG1GC
-Djdk.attach.allowAttachSelf=true
-XX:+UnlockDiagnosticVMOptions
${TRINO_DEBUG_OPTS}
EOF


TRINO_CATALOG_URI="http://${CONTAINER_HOST}:19120/iceberg/main"

cat <<EOF > "$TRINO_TEMP_DIR/catalog/iceberg.properties"
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=${TRINO_CATALOG_URI}
iceberg.rest-catalog.warehouse=s3://demobucket/
EOF

echo "Starting Trino server version $TRINO_VERSION..."

TRINO_PORT=8180

# Run Trino in a container, ensure that everything created in the warehouse is readable/writeable
# by Nessie Catalog as well.
TRINO_CONTAINER_ID=$(${DOCKER} run --detach -p ${TRINO_PORT}:${TRINO_PORT} -p 5007:5007 \
  --userns=keep-id --user "$(id -u)" \
  --volume $TRINO_TEMP_DIR/log.properties:/etc/trino/log.properties \
  --volume $TRINO_TEMP_DIR/jvm.config:/etc/trino/jvm.config \
  --volume $TRINO_TEMP_DIR/catalog:/etc/trino/catalog \
  $DOCKER_RUN_OPTS \
  docker.io/trinodb/trino:"$TRINO_VERSION")

if [[ -z "$VERBOSE" ]]; then
  TRINO_PROMPT="[TRINO] "
else
  TRINO_PROMPT=$(printf "\r\033[1m\033[34m[TRINO] \033[0m")
fi

${DOCKER} logs -f "$TRINO_CONTAINER_ID" 2>&1 | sed ''s/^/"$TRINO_PROMPT"/'' >> "$REDIRECT" &

# Wait for the server to finish startup by grepping the logs in a subshell
( ${DOCKER} logs -f "$TRINO_CONTAINER_ID" 2>&1 & ) | grep -q "SERVER STARTED"

echo
echo "Trino server started with container ID ${TRINO_CONTAINER_ID}"
echo "Launching Trino CLI... (type 'exit' to quit)"
echo "You can create a table with the following commands:"

cat <<EOF
CREATE SCHEMA iceberg.db3
  WITH (location='s3://demobucket/');
USE iceberg.db3;
CREATE TABLE yearly_clicks (year, clicks)
  WITH (partitioning = ARRAY['year']) AS VALUES (2021, 10000), (2022, 20000);
EOF

echo

${DOCKER} exec -it "${TRINO_CONTAINER_ID}" trino
