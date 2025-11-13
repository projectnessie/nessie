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
#   Java (Quarkus fast-jar) multiplatform
#     with tags: <version>-java, <version>, latest-java, latest
# The list if generated images is placed into the file passed in via the -i option.
#

set -e

IMAGE_NAME=""
GITHUB=0
LOCAL=0
GRADLE_PROJECT=""
PROJECT_DIR=""
DOCKERFILE="Dockerfile-server"

TOOL="$(which docker > /dev/null && echo docker || echo podman)"

if [[ -n ${GITHUB_ENV} ]]; then
  GITHUB=1
fi

function usage() {
  cat << ! > /dev/stderr
  Usage: $0 [options] <docker-image-base-name>

      -g | --gradle-project <project>   Gradle project name, for example :nessie-quarkus
      -p | --project-dir <dir>          Directory of the Gradle project, relative to the root of the repository
      -d | --dockerfile <file>          Dockerfile to use (default: Dockerfile-server)
      -gh | --github                    GitHub actions mode
      -l | --local                      Only build the image for local use (not multi-platform),
                                        not pushed to a registry. Can build with 'docker' and 'podman'.
      -t | --tool                       Name of the podman/docker/podman-remote tool to use.

  Note: multiplatform builds only work with docker buildx, not implemented for podman.

  GitHub mode is automatically enabled, when GITHUB_ENV is present. -a is mandatory in GitHub mode.

  Examples:
  $0 --local --dockerfile Dockerfile-server --gradle-project :nessie-quarkus --project-dir servers/quarkus-server nessie-local
  $0 --dockerfile Dockerfile-server --gradle-project :nessie-quarkus --project-dir servers/quarkus-server nessie-unstable
  $0 --dockerfile Dockerfile-gctool --gradle-project :nessie-gc-tool --project-dir gc/gc-tool nessie-gc-unstable
  $0 --dockerfile Dockerfile-admintool --gradle-project :nessie-server-admin-tool --project-dir tools/server-admin nessie-admin-unstable
  $0 --dockerfile Dockerfile-cli --gradle-project :nessie-cli --project-dir cli/cli nessie-cli-unstable
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
  -d | --dockerfile)
    DOCKERFILE="$2"
    shift
    ;;
  -gh | --github)
    GITHUB=1
    ;;
  -l | --local)
    LOCAL=1
    ;;
  -t | --TOOL)
    TOOL="$2"
    shift
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

if [[ -z $IMAGE_NAME || -z $GRADLE_PROJECT || -z $PROJECT_DIR || ! -d $PROJECT_DIR ]] ; then
  usage
  exit 1
fi

BASE_DIR="$(cd "$(dirname "$0")/../.." ; pwd)"
cd "$BASE_DIR"

#
# Prepare
#

gh_group "Prepare Docker image name and tag base"
VERSION="$(cat version.txt)"
IMAGE_TAG_BASE="${VERSION%-SNAPSHOT}"
echo "Image name: ${IMAGE_NAME}"
echo "Tag base: ${IMAGE_TAG_BASE}"
gh_endgroup

if [[ ${LOCAL} == 0 ]] ; then
  gh_group "Prepare buildx"
  ${TOOL} buildx use default
  ${TOOL} buildx create \
    --platform linux/amd64,linux/arm64 \
    ${BUILDX_CONFIG} \
    --use \
    --name nessiebuild \
    --driver-opt network=host || docker buildx use nessiebuild
  # Note: '--driver-opt network=host' is needed to be able to push to a local registry (e.g. localhost:5000)
  gh_endgroup

  gh_group "Docker buildx info"
  ${TOOL} buildx inspect
  gh_endgroup
fi

#
# Gradle Build
#
gh_group "Build Java"
./gradlew $CUSTOM_GRADLE_ARGS "${GRADLE_PROJECT}:clean" "${GRADLE_PROJECT}:build" -x "${GRADLE_PROJECT}:check"
gh_endgroup

if [[ ${LOCAL} == 1 ]] ; then
  gh_group "Docker build"
  ${TOOL} build \
    --file "${BASE_DIR}/tools/dockerbuild/docker/${DOCKERFILE}" \
    --tag "${IMAGE_NAME}:latest" \
    --tag "${IMAGE_NAME}:${IMAGE_TAG_BASE}" \
    --build-arg VERSION="${VERSION}" \
    "${BASE_DIR}/${PROJECT_DIR}"
  gh_endgroup
else
  gh_group "Docker buildx build"
  # All the platforms that are available
  PLATFORMS="linux/amd64,linux/arm64/v8,linux/ppc64le,linux/s390x"
  ${TOOL} buildx build \
    --file "${BASE_DIR}/tools/dockerbuild/docker/${DOCKERFILE}" \
    --platform "${PLATFORMS}" \
    --tag "${IMAGE_NAME}:latest" \
    --tag "${IMAGE_NAME}:latest-java" \
    --tag "${IMAGE_NAME}:${IMAGE_TAG_BASE}" \
    --tag "${IMAGE_NAME}:${IMAGE_TAG_BASE}-java" \
    --build-arg VERSION="${VERSION}" \
    "${BASE_DIR}/${PROJECT_DIR}" \
    --push \
    --provenance=false --sbom=false \
    --output type=registry
    # Note: '--output type=registry' is needed to be able to push to a local registry (e.g. localhost:5000)
    # Note: '--provenance=false --sbom=false' work around UI issues in ghcr + quay showing 'unknown/unknown' architectures
    gh_summary "## Java image tags, built for ${PLATFORMS}"
    gh_summary "* \`docker pull ${IMAGE_NAME}:latest\`"
    gh_summary "* \`docker pull ${IMAGE_NAME}:latest-java\`"
    gh_summary "* \`docker pull ${IMAGE_NAME}:${IMAGE_TAG_BASE}\`"
    gh_summary "* \`docker pull ${IMAGE_NAME}:${IMAGE_TAG_BASE}-java\`"
  gh_endgroup
fi
