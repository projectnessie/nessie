# Notes on how to publish a Nessie release

TODO Flesh out...

Nessie releases need not much manual interaction. To perform a release, a "fully green"
commit in the `main` branch is needed, so the statuses recorded for the git commit that
shall become a release must all have the "success" result (the green checkmark).

To start the release process, manually start the "Create Release" (`release-create.yml`)
GitHub workflow, which requires two input parameters:
* the next release version (e.g. `0.6.0`)
* the next development iteration version (e.g. `0.6.1`) *without* any suffix like SNAPSHOT
* the git reference to create the release from (e.g. a Git SHA) is optional, it defaults
  to HEAD of the `main` branch
  
The "Create Release" workflow effectively just bumps the Nessie version to the release version
in a new git commit with a new git tag plus another git commit to bump to the Nessie version
to the next development iteration version. Those two commits + the tag are pushed to the
`main` branch.

Pushing the Nessie release git-tag triggers the "Publish release" (`release-publish.yml`) 
workflow. This workflow runs in the `release` GitHub environment, which requires manual
approval (GH calls it a review). The "Publish release" workflow then creates and deploys
the release artifacts.


# TODO TODO TODO TODO remove the following stuff entirely! 

## Build Nessie incl Gradle-Plugin and pynessie

This is a prerequisite step, that's repeated below for the "final" release version.
Run one command from the snippet below at a time - one after the other - not "copy & paste". 

```shell
# cd to nessie root directory

# Manually verify you're running Java 11
java -version

# Clean all build files
./mvnw clean
(cd tools/apprunner-gradle-plugin/ ; ./gradlew clean)

# Sanity builds
./mvnw install -Pnative -Pjdk8-tests -Pcode-coverage,jdk8-tests,native
(cd tools/apprunner-gradle-plugin/ ; ./gradlew build)
(cd python ; . venv/bin/activate ; python3 setup.py sdist bdist_wheel)
```

## Actual release

Update the environment variables at the top to your needs, then run
*ONE COMMAND AT A TIME MANUALLY AFTER THE OTHER !!!*

*DO NOT JUST COPY&PASTE EVERYTHING AND HOPE THAT IT WILL WORK!*

```shell
RELEASE_VERSION=0.5.1
NEXT_VERSION_BASE=0.5.2
NEXT_VERSION=${NEXT_VERSION_BASE}-SNAPSHOT
UPSTREAM=github
GIT_TAG=nessie-${RELEASE_VERSION}

git fetch ${UPSTREAM}
git checkout main
git reset --hard ${UPSTREAM}/main
# NOTE: THIS WILL DESTROY ANY IDE CONFIGURATION !!!
git clean -xdf

# Clean all build files (should be no-ops due to preceding `git-clean`)
./mvnw clean
(cd tools/apprunner-gradle-plugin/ ; ./gradlew clean)

(cd python ; virtualenv venv)
(cd python ; . venv/bin/activate ; pip install bumpversion twine)
(cd python ; . venv/bin/activate ; bumpversion --no-commit --new-version ${RELEASE_VERSION} minor)
./mvnw versions:set -DgenerateBackupPoms=false -DnewVersion=${RELEASE_VERSION}

# Commit locally (don't push yet)
git commit -a -m "[release] prepare release nessie-${RELEASE_VERSION}"
git tag ${GIT_TAG}

# Build with tests
./mvnw install -Pnative -Pjdk8-tests -Pcode-coverage,jdk8-tests,native
(cd tools/apprunner-gradle-plugin/ ; ./gradlew build)
(cd python ; . venv/bin/activate ; python3 setup.py sdist bdist_wheel)

# Test pypi
(cd python ; . venv/bin/activate ; python3 -m twine upload --repository testpypi dist/*)

# Real pypi
(cd python ; . venv/bin/activate ; python3 -m twine upload dist/*)

git push ${UPSTREAM} main
git push ${UPSTREAM} :refs/tags/${GIT_TAG}

(cd tools/apprunner-gradle-plugin/ ; ./gradlew publishPlugins)

# Next development version
./mvnw versions:set -DgenerateBackupPoms=false -DnewVersion=${NEXT_VERSION}
(cd python ; . venv/bin/activate ; bumpversion --no-commit --new-version ${NEXT_VERSION_BASE} minor)
git commit -a -m "[release] prepare for next development iteration"
git push ${UPSTREAM} main



# Lookup Docker image SHA from build logs for the release-git-tag from the main branch -
# look for the "Push Docker images" step.

DOCKER_IMAGE_SHA=xxx

docker pull projectnessie/nessie-unstable@sha256:${DOCKER_IMAGE_SHA}
docker tag projectnessie/nessie-unstable@sha256:${DOCKER_IMAGE_SHA} projectnessie/nessie:${RELEASE_VERSION} && \
  docker tag projectnessie/nessie-unstable@sha256:${DOCKER_IMAGE_SHA} projectnessie/nessie:latest
docker push projectnessie/nessie:${RELEASE_VERSION} && docker push projectnessie/nessie:latest
```


## TODO

* Automatically close + deploy staging repo
  See https://github.com/netty/netty/blob/4.1/.github/workflows/ci-release.yml#L192-L204
