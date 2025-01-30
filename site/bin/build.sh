#!/usr/bin/env bash

set -e

source "$(dirname $0)"/setup_env.sh

bin/collect_iceberg_nessie_spark_versions.sh > iceberg_nessie_spark_versions.json

mkdocs build
