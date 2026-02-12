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
#

# Script to unify especially old versioned docs
#
# Background: mkdocs takes a long time to build the projectnessie site due
# to the number of versions documented on the web site.
#
# This script unifies version docs with identical content.

set -e

cd "${0%/*}/.."
base_dir="$(pwd)"
build_dir="${base_dir}/build"
versions_dir="${build_dir}/versions"
cd "${versions_dir}"

temp_diff_dir="$build_dir/cleanup-temp-diff"
mkdir -p "${temp_diff_dir}"

readarray -t versions < <(find . -mindepth 1 -maxdepth 1 -type d -name "[01]*" -printf "%P\n" | sort --version-sort)

num_versions=${#versions[@]}
echo "Found ${num_versions} versions"

collapse_first=""
declare -a collapse_more

prev_version="${versions[0]}"
for i in $(seq 1 "$((num_versions - 2))"); do
  curr_version="${versions[$i]}"
  echo
  echo "Comparing content for ${curr_version} with ${prev_version}"

  readarray diff_files < <(diff --recursive --brief "${curr_version}/docs" "${prev_version}/docs")
  do_collapse=0
  if [[ ${#diff_files[@]} -eq 2 ]]; then
    readarray -t files < <(echo "${diff_files[@]}" | sed -E "s/.*Files ${curr_version}\/(.+) and .+ differ.*/\1/")
    # Last element in files is an empty string, remove it
    unset "files[-1]"
    if [[ ${files[*]} =~ docs/gc.md && ${files[*]} =~ docs/index.md ]]; then
      echo "  Checking gc.md + index.md for just version string differences..."
      any_diff=0
      for file in "${files[@]}"; do
        echo "    Checking $file..."
        sed "s/${prev_version//\./\\.}/__VERSION__/g" "${prev_version}/$file" > "${temp_diff_dir}/prev"
        sed "s/${curr_version//\./\\.}/__VERSION__/g" "${curr_version}/$file" > "${temp_diff_dir}/curr"
        if ! diff --brief "${temp_diff_dir}/prev" "${temp_diff_dir}/curr" > /dev/null; then
          any_diff=1
        fi
        rm -f "${temp_diff_dir}/prev" "${temp_diff_dir}/curr"
      done
      if [[ ${any_diff} -eq 0 ]]; then
        do_collapse=1
      fi
    fi
  fi

  if [[ ${do_collapse} -eq 1 ]]; then
    echo "  ✅ Only version number differences, can collapse ${prev_version} and ${curr_version}"
    if [[ -z ${collapse_first} ]]; then
      collapse_first="${prev_version}"
    fi
    collapse_more+=("${curr_version}")
  else
    echo "  ⚠️ Content differs"
    if [[ -n ${collapse_first} ]]; then
      echo
      echo "Collapsing versions ${collapse_more[*]} into ${collapse_first}"
      # shellcheck disable=SC2001
      append_to_site_name="$(echo "${collapse_more[@]}" | sed 's/ /, /g')"
      echo "$append_to_site_name"
      sed -E "s/site_name: \"Nessie (.*)\"/site_name: \"Nessie \1, ${append_to_site_name}\"/" < "${collapse_first}/mkdocs.yml" > "${temp_diff_dir}/new-mkdocs.yml"
      mv "${temp_diff_dir}/new-mkdocs.yml" "${collapse_first}/mkdocs.yml"
      rm -rf "${collapse_more[*]}"
      removals+=("${collapse_more[@]}")
      sed -E "s/    - Nessie (${collapse_first}.*): '!include build\/versions\/${collapse_first}\/mkdocs.yml'/    - Nessie \1, ${append_to_site_name}: '!include build\/versions\/${collapse_first}\/mkdocs.yml'/" \
        "${base_dir}/nav.yml" > "${temp_diff_dir}/new-nav.yml"
      mv "${temp_diff_dir}/new-nav.yml" "${base_dir}/nav.yml"
      for removal in "${collapse_more[@]}"; do
        echo "  Removing ${removal} from nav.yml"
        grep -v "    - Nessie ${removal}" < "${base_dir}/nav.yml" > "${temp_diff_dir}/new-nav.yml"
        mv "${temp_diff_dir}/new-nav.yml" "${base_dir}/nav.yml"
      done
    fi
    collapse_first=""
    collapse_more=()
  fi

  prev_version="${curr_version}"
done

echo
echo "Summary:"
echo "  Number of versions:             ${num_versions}"
echo "  Number of removed versions:     ${#removals[*]}"
echo "  Removed versions:               ${removals[*]}"
