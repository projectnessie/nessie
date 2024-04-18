#!/usr/bin/env bash

source "$(dirname $0)"/_lib.sh

set -e

clean

if [[ -z ${CI} ]] ; then
  virtual_env
fi

install_deps

generate_docs

pull_versioned_docs
