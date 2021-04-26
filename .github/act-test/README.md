# Running Nessie workflows with https://github.com/nektos/act

**THE CONTENTS IN THIS DIRECTORY ARE PURELY TO TEST GITHUB WORKFLOWS LOCALLY!**

**TEST EXTREMELY CAREFULLY**

## Disadvantages of nektos/act

* "Reference that triggered event" - the `ref` attribute in an event JSON is rather meaningless.
  "act" always uses the HEAD of the current git repo ([see here](https://github.com/nektos/act/blob/master/pkg/runner/run_context.go#L502-L514)).
  So you need to run "act" on a different clone and specify the `--workflows` directory.
* The latest version of "act" does not work correctly with recent git versions, i.e. it falls back
  to the branch name, when the `.git/refs/tags` directory is empty ([see here]()https://github.com/nektos/act/pull/633)

## Prepare/setup

### Build a Docker image w/ `gh`

Even the "huge" image `nektos/act-environments-ubuntu:18.04` doesn't have `gh` installed, so we need
our own Docker image for the runner.

```
cd image
docker build --force-rm --rm --tag projectnessie/act-environments-gh-ubuntu:18.04 --file Dockerfile .
```

### Install "act"

Don't use the main branch, but [this PR](https://github.com/nektos/act/pull/633), follow the
instructions on their web site (install Go, make, etc).
This should be sufficient `make && sudo cp dist/local/act /usr/local/bin/act`.

### git remote name

Make sure that the name of the git remote is `origin`

### Update `~/.actrc`

Create a GitHub personal access token and add it to `~/.actrc`:
```
--secret GITHUB_TOKEN=ghp_YOUR_PAT
```
Change the line starting with `-P ubuntu-latest` to:
```
-P ubuntu-latest=projectnessie/act-environments-gh-ubuntu:18.04
```

## Running the "Release Create" workflow

1. Check/update `release-create.json`
2. Run `act -v workflow_dispatch -j create-release -e ../main/.github/act-test/release-create.json  -W ../main/.github/workflows -C .`
3. The "Release Publish" workflow *WOULD* start automatically on GitHub, but it's tricky to get *only*
   that one triggered with act. Also, the git-commits+tag created by `release-create.yml` are *not*
   pushed when running in "act".

## Running the "Release Publish" workflow for a `push`

1. Run `act -v push -j publish-release -W ../main/.github/workflows -C .`

## Running the "Release Publish" workflow for a `workflow_dispatch` (manual re-release)

1. Check/update `release-publish-manual.json`
2. Run `act -v workflow_dispatch -j publish-release -e ../main/.github/act-test/release-publish-manual.json -W ../main/.github/workflows -C .`

## To test only the "Release Publish" workflow 

In case you just want to test the "Release Publish" workflow without "Release Create" (i.e. against
any commit in the main branch), run the following commands:

```shell
cd python
[[ ! -d venv ]] && virtuanenv venv
. venv bin/activate
pip install bump2version
## **NOTE** ONLY RUN BUMP2VERSION IF THE CURRENT VERSION IS DIFFERENT FROM THE RELEASE-VERSION!!
bump2version --no-commit --no-tag --new-version 0.5.3 minor
./mvnw versions:set -DgenerateBackupPoms=false -DnewVersion=0.5.3
git commit -am "[release] THIS IS A TEST!"
git tag -f nessie-0.5.3
```

**DO NOT GIT-PUSH THIS !!!** 

Then run the "Release Publish" workflow as described above.

## Notes

Be careful when just running `act` - it will start the matching workflows, for example the whole
"Main CI" spiel.
