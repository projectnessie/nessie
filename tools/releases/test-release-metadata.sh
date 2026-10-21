#!/usr/bin/env bash
#
# Copyright (C) 2026 Dremio
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

set -euo pipefail

base_dir="$(cd "$(dirname "$0")/../.." && pwd)"
metadata_tool="${base_dir}/tools/releases/release-metadata.sh"
temp_dir="$(mktemp -d)"
trap 'rm -rf "${temp_dir}"' EXIT

expect_success() {
  "$@" >/dev/null
}

expect_failure() {
  if "$@" >/dev/null 2>&1; then
    echo "Expected command to fail: $*" >&2
    exit 1
  fi
}

expect_count() {
  local expected_count="$1"
  local pattern="$2"
  local file="$3"
  local actual_count

  actual_count="$(grep -F -c -- "${pattern}" "${file}" || true)"
  [[ "${actual_count}" == "${expected_count}" ]] || {
    echo "Expected ${expected_count} occurrences of '${pattern}' in ${file}, found ${actual_count}" >&2
    exit 1
  }
}

metadata_file="${temp_dir}/release-metadata.properties"
expect_success bash "${metadata_tool}" write "${metadata_file}" mainline
[[ "$(bash "${metadata_tool}" read "${metadata_file}")" == mainline ]]
expect_success bash "${metadata_tool}" write "${metadata_file}" maintenance
[[ "$(bash "${metadata_tool}" read "${metadata_file}")" == maintenance ]]
expect_failure bash "${metadata_tool}" write "${metadata_file}" unknown

expect_failure bash "${metadata_tool}" read "${temp_dir}/missing.properties"
printf 'release_mode=mainline\nrelease_mode=maintenance\n' > "${metadata_file}"
expect_failure bash "${metadata_tool}" read "${metadata_file}"
printf 'release_mode=unknown\n' > "${metadata_file}"
expect_failure bash "${metadata_tool}" read "${metadata_file}"
printf '\n' > "${metadata_file}"
expect_failure bash "${metadata_tool}" read "${metadata_file}"

expect_success bash "${metadata_tool}" validate-create-input mainline patch
expect_success bash "${metadata_tool}" validate-create-input maintenance fix1
expect_success bash "${metadata_tool}" validate-create-input maintenance patch
expect_failure bash "${metadata_tool}" validate-create-input mainline fix1
expect_failure bash "${metadata_tool}" validate-create-input maintenance none
expect_failure bash "${metadata_tool}" validate-create-input maintenance fix0

expect_success bash "${metadata_tool}" validate-version mainline 1.2.3
expect_success bash "${metadata_tool}" validate-version maintenance 1.2.3-fix1
expect_success bash "${metadata_tool}" validate-version maintenance 1.2.3
expect_failure bash "${metadata_tool}" validate-version mainline 1.2.3-fix1
expect_failure bash "${metadata_tool}" validate-version mainline 1.2.3-fix0
expect_failure bash "${metadata_tool}" validate-version maintenance 1.2.3-fix0

create_workflow="${base_dir}/.github/workflows/release-create.yml"
publish_workflow="${base_dir}/.github/workflows/release-publish.yml"
expect_count 3 "if: env.RELEASE_MODE == 'mainline'" "${create_workflow}"
expect_count 1 "if [[ \"\${RELEASE_MODE}\" == mainline ]]" "${create_workflow}"
expect_count 1 "RELEASE_MODE=mainline" "${create_workflow}"
expect_count 1 "RELEASE_MODE=maintenance" "${create_workflow}"
expect_count 0 "releaseMode:" "${create_workflow}"
expect_count 4 "\"\${IMAGE_TAG_OPTIONS[@]}\"" "${publish_workflow}"
expect_count 1 "RELEASE_OPTIONS+=(--prerelease)" "${publish_workflow}"
expect_count 1 "RELEASE_OPTIONS+=(--latest)" "${publish_workflow}"
expect_count 1 "RELEASE_OPTIONS+=(--latest=false)" "${publish_workflow}"
expect_count 1 "if [[ \"\${RELEASE_MODE}\" == mainline ]] ; then" "${publish_workflow}"
expect_count 1 "if [[ ! \"\${RELEASE_VERSION}\" =~ ^[0-9]+[.][0-9]+[.][0-9]+\$ ]] ; then" "${publish_workflow}"
expect_count 1 "release-metadata.sh validate-version" "${publish_workflow}"

echo "release-metadata tests passed"
