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

set -e

. "$(dirname "$0")"/head.in.sh
source "${DECLARES}"

BASE_NESSIE_DIR="${NESSIE_DIR}"
BASE_ICEBERG_DIR="${ICEBERG_DIR}"
BASE_TRINO_DIR="${TRINO_DIR}"

LOCAL_GIT_DIR="${SCRIPT_BUILD_DIR}"/git

NESSIE_DIR="${LOCAL_GIT_DIR}"/nessie
ICEBERG_DIR="${LOCAL_GIT_DIR}"/iceberg
TRINO_DIR="${LOCAL_GIT_DIR}"/trino

function sync_local_git() {
  local git_base="$1"
  local git_dir="$2"
  echo "Sync ${git_base} into ${git_dir} ..."
  if [[ -d ${git_dir} ]] ; then
    echo "... resetting existing directory ${git_dir} ..."
    git_commit=$(cd "${git_base}" ; git rev-parse HEAD)
    echo "... to commit ${git_commit}"
    (cd "${git_dir}" ; git reset --hard --quiet ${git_commit} ; git clean -xdf)
  else
    echo "... creating new directory ${git_dir} ..."
    (cd "${git_base}" ; git worktree add --detach "${git_dir}")
  fi
  echo "... applying local changes from ${git_base} ..."
  (cd "${git_base}" ; git diff HEAD) | (cd "${git_dir}" ; git apply --quiet --allow-empty -)
  echo "... done."
}

sync_local_git "${BASE_NESSIE_DIR}" "${NESSIE_DIR}"
"${SCRIPT_DIR}"/apply_bump.sh "${NESSIE_DIR}" "integ-bump/iceberg" "main github/main"
sync_local_git "${BASE_ICEBERG_DIR}" "${ICEBERG_DIR}"
sync_local_git "${BASE_TRINO_DIR}" "${TRINO_DIR}"
"${SCRIPT_DIR}"/apply_bump.sh "${TRINO_DIR}" "iceberg-0.14" "master github/master"

write_declares
