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
trap '[[ -n $(jobs -p) ]] && kill $(jobs -p);' EXIT
set -e

PROJECT_DIR=$(dirname "$0")/../..
cd "$PROJECT_DIR"
PROJECT_DIR=$(pwd)

# Set the default values
NESSIE_VERSION=$(./gradlew properties -q | awk '/^version:/ {print $2}')
ICEBERG_VERSION="1.5.0"
SPARK_VERSION="3.5"
SCALA_VERSION="2.12"
AWS_SDK_VERSION="2.20.131"
WAREHOUSE_LOCATION="$PROJECT_DIR/build/spark-warehouse"
CLIENT_ID="client1"
CLIENT_SECRET="s3cr3t"
OAUTH_SERVER_URI="http://127.0.0.1:8080/realms/iceberg/protocol/openid-connect/token"
ICEBERG_URI="http://127.0.0.1:19120/iceberg/main/"

# Parse the command line arguments
while [[ $# -gt 0 ]]
do
  key="$1"
  case $key in
    --nessie-version)
      NESSIE_VERSION="$2"
      shift
      shift
      ;;
    --iceberg-version)
      ICEBERG_VERSION="$2"
      shift
      shift
      ;;
    --spark-version)
      SPARK_VERSION="$2"
      shift
      shift
      ;;
    --scala-version)
      SCALA_VERSION="$2"
      shift
      shift
      ;;
    --aws-sdk-version)
      AWS_SDK_VERSION="$2"
      shift
      shift
      ;;
    --warehouse)
      WAREHOUSE_LOCATION="$2"
      shift
      shift
      ;;
    --old-catalog)
      USE_OLD_CATALOG="true"
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
    --no-extensions)
      NO_EXTENSIONS="true"
      shift
      ;;
    --clear-warehouse)
      CLEAR_WAREHOUSE="true"
      shift
      ;;
    --aws)
      AWS="true"
      shift
      ;;
    --iceberg)
      ICEBERG_URI="$2"
      shift
      shift
      ;;
    --oauth)
      OAUTH="true"
      shift
      ;;
    --client-id)
      CLIENT_ID="$2"
      PROMPT_CLIENT_SECRET="true"
      shift
      shift
      ;;
    --oauth-server-uri)
      OAUTH_SERVER_URI=$2
      shift
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
  echo "Usage: spark-sql.sh [options]"
  echo "Options:"
  echo "  --nessie-version <version>      Nessie version to use. Default: $NESSIE_VERSION"
  echo "  --iceberg-version <version>     Iceberg version to use. Default: $ICEBERG_VERSION"
  echo "  --spark-version <version>       Spark version to use. Default: $SPARK_VERSION"
  echo "  --scala-version <version>       Scala version to use. Default: $SCALA_VERSION"
  echo "  --aws-sdk-version <version>     AWS SDK version to use. Default: $AWS_SDK_VERSION"
  echo "  --warehouse <location>          Warehouse location. Default: $WAREHOUSE_LOCATION"
  echo "  --no-publish                    Do not publish jars to Maven local. Default: false"
  echo "  --no-clear-cache                Do not clear ivy cache. Default: false"
  echo "  --no-nessie-start               Do not start Nessie Core/Catalog, use externally provided instance(s). Default: start"
  echo "  --no-extensions                 Do not use Spark SQL extensions"
  echo "  --clear-warehouse               Clear warehouse directory. Default: false"
  echo "  --aws                           Enable AWS support"
  echo "  --oauth                         Enable OAuth support"
  echo "  --client-id <credentials>       OAuth client ID. Default: $CLIENT_ID. Client secret will be prompted."
  echo "  --oauth-server-uri <uri>        OAuth server URI. Default: $OAUTH_SERVER_URI"
  echo "  --debug                         Enable debug mode"
  echo "  --verbose                       Enable verbose mode"
  echo "  --help                          Print this help and exit"
  exit 0
fi

done

source "$PROJECT_DIR/catalog/bin/common.sh"

PACKAGES=(
  "org.projectnessie.nessie-integrations:nessie-spark-extensions-${SPARK_VERSION}_${SCALA_VERSION}:${NESSIE_VERSION}"
  "org.apache.iceberg:iceberg-spark-runtime-${SPARK_VERSION}_${SCALA_VERSION}:${ICEBERG_VERSION}"
)

if [[ -n "$AWS" ]]; then
  PACKAGES+=(
    "software.amazon.awssdk:bundle:${AWS_SDK_VERSION}"
    "software.amazon.awssdk:url-connection-client:${AWS_SDK_VERSION}"
  )
fi

if [[ -n "$OAUTH" ]]; then
  if [[ -n "$PROMPT_CLIENT_SECRET" ]]; then
    read -r -s -p "Client secret: " CLIENT_SECRET
  fi
  AUTH_CONF=(
    "--conf" "spark.sql.catalog.nessie.scope=catalog sign"
    "--conf" "spark.sql.catalog.nessie.oauth2-server-uri=${OAUTH_SERVER_URI}"
    "--conf" "spark.sql.catalog.nessie.credential=${CLIENT_ID}:${CLIENT_SECRET}"
  )
  AUTH_CONF_DISPLAY=(
    "--conf" "spark.sql.catalog.nessie.scope=catalog sign"
    "--conf" "spark.sql.catalog.nessie.oauth2-server-uri=${OAUTH_SERVER_URI}"
    "--conf" "spark.sql.catalog.nessie.credential=${CLIENT_ID}:********"
  )
fi

if [[ -n "$DEBUG" ]]; then
  DEBUG_SPARK_SHELL=(
    "--conf" "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5007"
    "--conf" "spark.sql.catalog.nessie.transport.read-timeout=300000"
  )
fi

if [[ -z "$NO_EXTENSIONS" ]]; then
  SPARK_EXTENSIONS=(
    "--conf" "spark.sql.extensions=org.projectnessie.spark.extensions.NessieSparkSessionExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  )
fi

echo
echo "Starting spark-sql $SPARK_VERSION ..."

echo "Packages:"
for package in "${PACKAGES[@]}"; do
  echo "- $package"
done

packages_csv=$(printf ",%s" "${PACKAGES[@]}")
packages_csv=${packages_csv:1}

echo ""
echo ""
echo "Starting spark-sql using..."
echo ""
echo ""
echo ""
echo "spark-sql ${DEBUG_SPARK_SHELL[@]} \\"
echo "  --packages \"${packages_csv}\" \\"
echo "  ${SPARK_EXTENSIONS[@]} \\"
echo "  ${AUTH_CONF_DISPLAY[@]} \\"
echo "  --conf spark.sql.catalogImplementation=in-memory \\"
echo "  --conf spark.sql.catalog.nessie.uri=${ICEBERG_URI} \\"
echo "  --conf spark.sql.catalog.nessie.type=rest \\"
echo "  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog"
echo ""
echo ""
echo ""

spark-sql "${DEBUG_SPARK_SHELL[@]}" \
  --packages "${packages_csv}" \
  "${SPARK_EXTENSIONS[@]}" \
  "${AUTH_CONF[@]}" \
  --conf spark.sql.ansi.enabled=true \
  --conf spark.sql.catalogImplementation=in-memory \
  --conf spark.sql.catalog.nessie.uri=${ICEBERG_URI} \
  --conf spark.sql.catalog.nessie.type=rest \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog
