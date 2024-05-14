#!/usr/bin/env bash

set -e

source "$(dirname $0)"/setup_env.sh

echo ""
echo ""

release_version=$1

if [[ -z "${release_version}" ]]; then
  echo "Must specify the version to release as the first argument to $0" > /dev/stderr
  echo "Or run 'make release RELEASE_VERSION=x.y.z'" > /dev/stderr
  echo "" > /dev/stderr
  exit 1
fi

echo "Do release ${release_version}"

release ${release_version}
