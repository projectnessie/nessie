#!/usr/bin/env bash
#
# Copyright (C) 2023 Dremio
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

#
# Helper script to generate all Docker images for a release (and snapshot publishing).
# The produced Docker images are:
#   native linux/amd64
#     with tags: <version>-native, latest-native
#   Java (Quarkus fast-jar) multiplatform
#     with tags: <version>-java, <version>, latest-java, latest
# The list if generated images is placed into the file passed in via the -i option.
# The generated native binary is placed into the directory passed in via the -r option.
#

set -e

IMAGE_NAME=""
GITHUB=0
NATIVE=0
ARTIFACTS=""
GRADLE_PROJECT=""
PROJECT_DIR=""

if [[ -n ${GITHUB_ENV} ]]; then
  GITHUB=1
fi

function usage() {
  cat << ! > /dev/stderr
  Usage: $0 [options] <docker-image-base-name>

      -g | --gradle-project <project>   Gradle project name, for example
      -p | --project-dir <dir>          Directory of the Gradle project
      -gh | --github                    GitHub actions mode
      -i | --images-file <file>         text file receiving the Docker image names to push
      -a | --artifacts-dir <dir>        directory to place native binaries and uber-jars in

  GitHub mode is automatically enabled, when GITHUB_ENV is present. -i and -r are mandatory in GitHub mode.

  Example: $0 -g :nessie-server -p servers/quarkus-server nessie-unstable
!
}

function gh_group() {
  [ ${GITHUB} == 1 ] && echo "::group::$*" || (echo "" ; echo "** $*" ; echo "")
}

function gh_endgroup() {
  [ ${GITHUB} == 1 ] && echo "::endgroup::" || echo ""
}

function gh_summary() {
  [ ${GITHUB} == 1 ] && echo "$*" >> "${GITHUB_STEP_SUMMARY}" || echo "$*"
}

while [[ $# -gt 0 ]]; do
  arg="$1"
  case "$arg" in
  -g | --gradle-project)
    GRADLE_PROJECT="$2"
    shift
    ;;
  -p | --project-dir)
    PROJECT_DIR="$2"
    shift
    ;;
  -a | --artifacts-dir)
    ARTIFACTS="$2"
    shift
    ;;
  -n | --native)
    NATIVE=1
    ;;
  -gh | --github)
    GITHUB=1
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  -*)
    usage
    exit 1
    ;;
  *)
    IMAGE_NAME="$arg"
    ;;
  esac
  shift
done

if [[ ${GITHUB} == 1 ]] ; then
  if [[ -z $ARTIFACTS ]] ; then
    usage
    exit 1
  fi
fi
if [[ -z $IMAGE_NAME || -z $GRADLE_PROJECT || -z $PROJECT_DIR || ! -d $PROJECT_DIR ]] ; then
  usage
  exit 1
fi

BASE_DIR="$(cd "$(dirname "$0")/../.." ; pwd)"
cd "$BASE_DIR"

if [[ -z ${ARTIFACTS} ]]; then
  mkdir -p "$BASE_DIR/build"
  ARTIFACTS="$(mktemp -p "$BASE_DIR/build" -d dockerbuild-artifacts-XXXXX)"
fi

#
# Prepare
#

gh_group "Prepare Docker image name and tag base"
IMAGE_TAG="$(cat version.txt)"
IMAGE_TAG_BASE="${IMAGE_TAG%-SNAPSHOT}"
echo "Image name: ${IMAGE_NAME}"
echo "Tag base: ${IMAGE_TAG_BASE}"
echo "Placing binaries in: ${ARTIFACTS}"
mkdir -p "${ARTIFACTS}"
gh_endgroup

gh_group "Prepare buildx"
docker buildx use default
docker buildx create \
  --platform linux/amd64,linux/arm64 \
  --use \
  --name nessiebuild \
  --driver-opt network=host || docker buildx use nessiebuild
# Note: '--driver-opt network=host' is needed to be able to push to a local registry (e.g. localhost:5000)
gh_endgroup

gh_group "Docker buildx info"
docker buildx inspect
gh_endgroup

#
# Native amd64 image
#

if [[ ${NATIVE} == 1 ]] ; then
  gh_group "Build native image"
  ./gradlew "${GRADLE_PROJECT}:clean" "${GRADLE_PROJECT}:quarkusBuild" -Pnative
  # Save the native runner binary in case we're publishing it
  cp "${PROJECT_DIR}"/build/*-runner "${ARTIFACTS}"
  gh_endgroup

  gh_group "Docker buildx build"
  NATIVE_PLATFORM="linux/amd64"
  docker buildx build \
    -f "${BASE_DIR}/tools/dockerbuild/docker/Dockerfile-native" \
    --platform "${NATIVE_PLATFORM}" \
    -t "${IMAGE_NAME}:latest-native" \
    -t "${IMAGE_NAME}:${IMAGE_TAG_BASE}-native" \
    "${BASE_DIR}/${PROJECT_DIR}" \
    --push \
    --output type=registry
    # Note: '--output type=registry' is needed to be able to push to a local registry (e.g. localhost:5000)
  gh_summary "## Native image tags, built for ${NATIVE_PLATFORM}"
  gh_summary "* \`docker pull ${IMAGE_NAME}:latest-native\`"
  gh_summary "* \`docker pull ${IMAGE_NAME}:${IMAGE_TAG_BASE}-native\`"
  gh_endgroup
fi

#
# Java multiplatform image
#

gh_group "Build Java linux/amd64"
./gradlew "${GRADLE_PROJECT}:clean" "${GRADLE_PROJECT}:quarkusBuild"
gh_endgroup

gh_group "Docker buildx build"
# All the platforms that are available
PLATFORMS="linux/amd64,linux/arm64/v8,linux/ppc64le,linux/s390x"
docker buildx build \
  -f "${BASE_DIR}/tools/dockerbuild/docker/Dockerfile-jvm" \
  --platform "${PLATFORMS}" \
  -t "${IMAGE_NAME}:latest" \
  -t "${IMAGE_NAME}:latest-java" \
  -t "${IMAGE_NAME}:${IMAGE_TAG_BASE}" \
  -t "${IMAGE_NAME}:${IMAGE_TAG_BASE}-java" \
  "${BASE_DIR}/${PROJECT_DIR}" \
  --push \
  --output type=registry
  # Note: '--output type=registry' is needed to be able to push to a local registry (e.g. localhost:5000)
  gh_summary "## Java image tags, built for ${PLATFORMS}"
  gh_summary "* \`docker pull ${IMAGE_NAME}:latest\`"
  gh_summary "* \`docker pull ${IMAGE_NAME}:latest-java\`"
  gh_summary "* \`docker pull ${IMAGE_NAME}:${IMAGE_TAG_BASE}\`"
  gh_summary "* \`docker pull ${IMAGE_NAME}:${IMAGE_TAG_BASE}-java\`"
gh_endgroup
