#!/usr/bin/env bash

source "$(dirname $0)"/_lib.sh

set -e

clean

if [[ -z ${CI} ]] ; then
  virtual_env
fi

# shellcheck disable=SC2155
export ICEBERG_VERSION="$(cat ../gradle/libs.versions.toml \
  | grep --extended-regexp 'iceberg[ ]*=[ ]*' \
  | sed --regexp-extended 's/^iceberg[ ]*=[ ]*\"([0-9.]+)\".*/\1/')"
# shellcheck disable=SC2155
export NESSIE_VERSION="$(cat ../version.txt)"

install_deps

generate_docs

pull_versioned_docs
