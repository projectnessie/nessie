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

# This is a script to verify that
# * all artifacts to be published have a corresponding .asc to verify that all artifacts are signed
# * all jar artifacts have corresponding -sources.jar and -javadoc.jar artifacts
# * all tests-jar artifacts have corresponding sources and javadoc artifacts

KEY_NAME="dummy-test-gpg@key-to-verify-publication.in-our-build"

gpg --no-tty --quick-generate-key --pinentry-mode=loopback --passphrase "" "${KEY_NAME}" RSA

ORG_GRADLE_PROJECT_signingKey="$(gpg --no-tty -a --export-secret-key "${KEY_NAME}")"
ORG_GRADLE_PROJECT_signingPassword=""
export ORG_GRADLE_PROJECT_signingKey
export ORG_GRADLE_PROJECT_signingPassword

cd "$(dirname "$0")/.." || exit 1

if [[ ! "$1" == "--noclean" ]]; then
  ./gradlew clean
fi
./gradlew publishToMavenLocal -Prelease

declare -g ERROR
ERROR=0

echo ""
echo "Checking for non-existing .asc files..."
echo ""
while read -r mavenPubFile ; do
  EXPECTED_ASC="${mavenPubFile}.asc"
  if [[ ! -f ${EXPECTED_ASC} ]]; then
    echo "Found artifact '${mavenPubFile}' without corresponding .asc file!" > /dev/stderr
    ERROR=1
  fi
done < <(find . \( -path "**/build/*publications/maven/*" -or -path "**/build/*libs/*SNAPSHOT.jar" \) -not -name "*.asc")

echo ""
echo "Checking for non-existing -sources.jar/-javadoc.jar files..."
echo ""
while read -r mavenPubFile ; do
  EXPECTED_SOURCES="${mavenPubFile/%.jar/-sources.jar}"
  if [[ ! -f ${EXPECTED_SOURCES} ]]; then
    echo "Found artifact '${mavenPubFile}' without corresponding -sources.jar file!" > /dev/stderr
    ERROR=1
  fi
  EXPECTED_JAVADOC="${mavenPubFile/%.jar/-javadoc.jar}"
  if [[ ! -f ${EXPECTED_JAVADOC} ]]; then
    echo "Found artifact '${mavenPubFile}' without corresponding -javadoc.jar file!" > /dev/stderr
    ERROR=1
  fi
done < <(find . -path "**/build/*libs/*SNAPSHOT.jar" -or -path "**/build/*libs/*SNAPSHOT-tests.jar")

echo ""
echo ""
echo ""

KEY_FINGERPRINT="$(gpg --list-secret-keys --with-colons --fingerprint ${KEY_NAME} | sed -n 's/^fpr:::::::::\([[:alnum:]]\+\):/\1/p')"

gpg --batch --yes --no-tty --delete-secret-key "${KEY_FINGERPRINT}"
gpg --batch --yes --no-tty --delete-key "${KEY_NAME}"

exit ${ERROR}
