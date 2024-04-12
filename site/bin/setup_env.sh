#!/usr/bin/env bash

source "$(dirname $0)"/_lib.sh

set -e

clean

if [[ -z ${CI} ]] ; then
  virtual_env
fi

install_deps

pull_versioned_docs
