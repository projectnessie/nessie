#!/usr/bin/env bash
#
# Copyright (C) 2022 Dremio
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

. "$(dirname "$0")"/head.in.sh

if [[ -z ${NESSIE_DIR} ]] ; then
  NESSIE_DIR="$(cd "${SCRIPT_DIR}"/.. ; pwd)"
fi
if [[ -z ${ICEBERG_DIR} ]] ; then
  ICEBERG_DIR="/home/snazy/devel/iceberg/master"
fi
if [[ -z ${TRINO_DIR} ]] ; then
  TRINO_DIR="/home/snazy/devel/trinodb/trino"
fi

MAVEN_BINARY="$(command -v mvnd >/dev/null 2>&1 && echo "mvnd" || echo "./mvnw -T1C")"

MAVEN_ARGS=""
if [[ -n ${ICEBERG_VERSION} ]] ; then
  MAVEN_ARGS="-Diceberg.version=${ICEBERG_VERSION}"
fi
if [[ -n ${CLIENT_NESSIE_VERSION} ]] ; then
  MAVEN_ARGS="-Dclient.nessie.version=${CLIENT_NESSIE_VERSION}"
fi

# Dump all properties from the Nessie build into ${NESSIE_DIR}/target/project.properties
(cd "${NESSIE_DIR}"
  ${MAVEN_BINARY} \
    ${MAVEN_ARGS} \
    -Pall-properties \
    "properties:write-project-properties" \
    -pl . \
    -q
)

# Make all properties from the Nessie build available as variables in this shell script.
# Dots and dashes in the property names are replaced with underscores.
# Shell variable name is (in Java pseudo syntax):
#     "nessie_" + propertyKey.replace('.', '_').replace('-', '_')
while IFS='=' read -ra line ; do
  if [[ ${#line[@]} == 2 ]] ; then
    key=${line[0]}
    key=$(echo $key | awk '{ name=$1; gsub(/\.|-/, "_", name); print name; }')
    value=${line[1]}
    declare "nessie_${key}=${value}"
  fi
done < "${NESSIE_DIR}"/target/project.properties

write_declares
