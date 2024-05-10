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

if [[ -z "$DOCKER" ]]; then
  if which podman > /dev/null 2>&1 ; then
    DOCKER=podman
  else
    DOCKER=docker
  fi
fi

case $DOCKER in
podman)
  CONTAINER_HOST=host.containers.internal
  DOCKER_RUN_OPTS="--network slirp4netns:allow_host_loopback=true"
  ;;
docker)
  CONTAINER_HOST=host.docker.internal
  DOCKER_RUN_OPTS=""
  ;;
*)
  echo "Unknown container runtime: $DOCKER"
  exit 1
  ;;
esac

GRADLE_OPTS=("--quiet")

if [[ -z "$VERBOSE" ]]; then
  GRADLE_OPTS+=("--console=plain")
  REDIRECT="$PROJECT_DIR/build/catalog-demo.log"
  NESSIE_COMBINED_PROMPT="[NESSIE] "
else
  REDIRECT="/dev/stdout"
  NESSIE_COMBINED_PROMPT=$(printf "\r\033[1m\033[32m[NESSIE] \033[0m")
fi

echo "Working directory  : $(pwd)"
echo "Warehouse location : $WAREHOUSE_LOCATION"
echo "Nessie logging to  : $REDIRECT"

if [[ -z "$NO_CLEAR_IVY_CACHE" ]]; then
  echo "Clearing Ivy cache..."
  rm -rf ~/.ivy2/cache/org.projectnessie*
  rm -rf ~/.ivy2/cache/org.apache.iceberg*
fi

if [[ -n "$CLEAR_WAREHOUSE" ]]; then
  echo "Clearing warehouse directory: $WAREHOUSE_LOCATION..."
  rm -rf "$WAREHOUSE_LOCATION"
fi

mkdir -p "$WAREHOUSE_LOCATION"

if [[ -z "$NO_PUBLISH_TO_MAVEN_LOCAL" ]]; then
  echo "Publishing to Maven local..."
  ./gradlew "${GRADLE_OPTS[@]}" publishToMavenLocal
fi

echo "Building Nessie server..."
./gradlew "${GRADLE_OPTS[@]}" :nessie-quarkus:quarkusBuild

if [[ -n "$DEBUG" ]]; then
  export QUARKUS_LOG_MIN_LEVEL="DEBUG"
  export QUARKUS_LOG_CONSOLE_LEVEL="DEBUG"
  export QUARKUS_LOG_CATEGORY__ORG_PROJECTNESSIE__LEVEL="DEBUG"
  export QUARKUS_LOG_CATEGORY__ORG_APACHE_ICEBERG__LEVEL="DEBUG"
  export QUARKUS_VERTX_MAX_EVENT_LOOP_EXECUTE_TIME="PT5M"
  DEBUG_NESSIE_CORE=(
    "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
  )
  DEBUG_NESSIE_CATALOG=(
    "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006"
    "-Dnessie.transport.read-timeout=300000"
  )
fi

if [[ -z "$NO_NESSIE_START" ]]; then
  echo "Starting Nessie Catalog server..."

  java "${DEBUG_NESSIE_CATALOG[@]}" \
    -Dquarkus.oidc.tenant-enabled=false -Dquarkus.otel.sdk.disabled=true \
    -jar servers/quarkus-server/build/quarkus-app/quarkus-run.jar | \
    sed ''s/^/"$NESSIE_COMBINED_PROMPT"/'' \
    >> "$REDIRECT" 2>&1 &

  sleep 2
  echo "Waiting for Nessie Catalog server to start..."
  curl --silent --show-error --fail \
    --connect-timeout 5 --retry 5 --retry-connrefused --retry-delay 0 --retry-max-time 10 \
    http://localhost:19120/q/health/ready > /dev/null 2>&1
fi
