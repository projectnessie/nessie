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
source "${DECLARES}"

git_dir="$1"
git_branch="$2"
git_base_branches=$3

echo "Applying changes from ${git_branch} to nearest common ancestor of \"${git_base_branches}\" into ${git_dir} ..."
merge_base="$(cd "${git_dir}" ; git merge-base ${git_branch} ${git_base_branches})"
echo "... applying changes of ${git_branch}..${merge_base} ..."
(cd "${git_dir}" ; git diff "${merge_base}" "${git_branch}" | git apply --quiet --allow-empty -)
echo "... done."
