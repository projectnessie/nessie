#!/usr/bin/env bash

set -e

echo "{"


cat ../integrations/spark-scala.properties \
 | grep "^[a-z].*" \
 | while read prop; do
   prop_key="$(echo $prop | cut -d= -f1)"
   prop_val="$(echo $prop | cut -d= -f2 | sed 's/,/", "/g')"
   echo "\"${prop_key}\": [\"${prop_val}\"],"
done

# Collect a JSON object containing the mapping of Iceberg versions (since 1.4) to Nessie versions

# don't consider Iceberg versions starting with 0. and 1.0,1.2,1.3
echo "\"iceberg_nessie\": {"
curl --silent --location -H "Accept: application/vnd.github+json" -H "X-GitHub-Api-Version: 2022-11-28" https://api.github.com/repos/apache/iceberg/releases \
 | jq --raw-output '.[].tag_name' \
 | grep --invert-match --extended-regexp '^apache-iceberg-(0[.].*|[1](\.[0-3])+)$' \
 | while read iceberg_release ; do
  iceberg_version="$(echo "${iceberg_release}" | sed 's/apache-iceberg-//')"
  nessie_version="$(curl --silent --location "https://raw.githubusercontent.com/apache/iceberg/refs/tags/${iceberg_release}/gradle/libs.versions.toml" \
    | grep --extended-regexp 'nessie[ ]*=[ ]*' \
    | sed --regexp-extended 's/nessie[ ]*=[ ]*"([0-9.]+)"/\1/')"
  echo "\"${iceberg_version}\" : \"${nessie_version}\""
done | paste --serial --delimiter ","
echo "}"

echo "}"
