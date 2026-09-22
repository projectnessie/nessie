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

error() {
  echo "$*" >&2
  exit 1
}

validate_release_mode() {
  case "$1" in
  mainline | maintenance) ;;
  *) error "Unsupported release mode '$1'; expected mainline or maintenance" ;;
  esac
}

is_fix_bump_type() {
  [[ "$1" =~ ^fix[1-9][0-9]*$ ]]
}

validate_create_release_input() {
  local release_mode="$1"
  local bump_type="$2"

  validate_release_mode "${release_mode}"
  case "${release_mode}" in
  mainline)
    case "${bump_type}" in
    none | patch | minor | major) ;;
    *) error "Mainline releases require bump type none, patch, minor, or major (got '${bump_type}')" ;;
    esac
    ;;
  maintenance)
    if [[ "${bump_type}" != patch ]] && ! is_fix_bump_type "${bump_type}"; then
      error "Maintenance releases require bump type patch or fix<N> (got '${bump_type}')"
    fi
    ;;
  esac
}

write_release_metadata() {
  local metadata_file="$1"
  local release_mode="$2"

  validate_release_mode "${release_mode}"
  printf 'release_mode=%s\n' "${release_mode}" > "${metadata_file}"
}

read_release_mode() {
  local metadata_file="$1"
  local line
  local release_mode=""
  local release_mode_count=0

  [[ -f "${metadata_file}" ]] || error "Release metadata file '${metadata_file}' does not exist"

  while IFS= read -r line || [[ -n "${line}" ]]; do
    case "${line}" in
    release_mode=mainline)
      release_mode=mainline
      release_mode_count=$((release_mode_count + 1))
      ;;
    release_mode=maintenance)
      release_mode=maintenance
      release_mode_count=$((release_mode_count + 1))
      ;;
    *) error "Invalid release metadata entry '${line}' in '${metadata_file}'" ;;
    esac
  done < "${metadata_file}"

  [[ ${release_mode_count} == 1 ]] ||
    error "Release metadata file '${metadata_file}' must contain exactly one release_mode entry"
  printf '%s\n' "${release_mode}"
}

validate_release_mode_for_version() {
  local release_mode="$1"
  local release_version="$2"

  validate_release_mode "${release_mode}"
  case "${release_mode}" in
  maintenance)
    [[ "${release_version}" =~ ^[0-9]+[.][0-9]+[.][0-9]+(-fix[1-9][0-9]*)?$ ]] ||
      error "Maintenance release version '${release_version}' must match x.y.z or x.y.z-fix<N>"
    ;;
  mainline)
    [[ "${release_version}" =~ ^[0-9]+[.][0-9]+[.][0-9]+$ ]] ||
      error "Mainline release version '${release_version}' must match x.y.z"
    ;;
  esac
}

usage() {
  cat >&2 <<'EOF'
Usage: release-metadata.sh <command> [arguments]

Commands:
  validate-create-input <mainline|maintenance> <bump-type>
  write <metadata-file> <mainline|maintenance>
  read <metadata-file>
  validate-version <mainline|maintenance> <release-version>
EOF
}

case "${1:-}" in
validate-create-input)
  [[ $# == 3 ]] || {
    usage
    exit 1
  }
  validate_create_release_input "$2" "$3"
  ;;
write)
  [[ $# == 3 ]] || {
    usage
    exit 1
  }
  write_release_metadata "$2" "$3"
  ;;
read)
  [[ $# == 2 ]] || {
    usage
    exit 1
  }
  read_release_mode "$2"
  ;;
validate-version)
  [[ $# == 3 ]] || {
    usage
    exit 1
  }
  validate_release_mode_for_version "$2" "$3"
  ;;
*)
  usage
  exit 1
  ;;
esac
