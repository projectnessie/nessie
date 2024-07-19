#!/usr/bin/env bash

set -e

export REMOTE="nessie_site_docs"

# Always change the current directory to site/
cd "$(dirname $0)"/..

# Ensures the presence of a specified remote repository for documentation.
# If the remote doesn't exist, it adds it using the provided URL.
# Then, it fetches updates from the remote repository.
create_or_update_docs_remote () {
  echo " --> create or update Git remote for site docs"

  # Check if the remote exists before attempting to add it
  git config "remote.${REMOTE}.url" >/dev/null || 
    git remote add "${REMOTE}" https://github.com/projectnessie/nessie.git

  # Fetch updates from the remote repository
  git fetch "${REMOTE}"
}

# Pulls updates from a specified branch of a remote repository.
# Arguments:
#   $1: Branch name to pull updates from
pull_remote () {
  echo " --> pull remote"

  local BRANCH
  BRANCH="$1"

  # Ensure the branch argument is not empty
  assert_not_empty "${BRANCH}"

  # Perform a pull from the specified branch of the remote repository
  git pull "${REMOTE}" "${BRANCH}"
}

# Sets up and activates the virtual environment
virtual_env () {
  echo " --> venv"

  if [[ ! -e venv/ ]] ; then
    if virtualenv -h >/dev/null 2>&1 ; then
      virtualenv venv/
      echo "     created"
    else
      echo "     NOT creating (virtualenv binary not found)"
    fi
  fi
  if [[ -d venv/ ]] ; then
    source venv/bin/activate
    echo "     activated (${VIRTUAL_ENV})"
  else
    echo "     not activated"
  fi
}

# Installs or upgrades dependencies specified in the 'requirements.txt' file using pip.
install_deps () {
  echo " --> install deps"

  # Use pip to install or upgrade dependencies from the 'requirements.txt' file quietly
  pip install -U pip
  pip install -r requirements.txt --upgrade
}

# Checks if a provided argument is not empty. If empty, displays an error message and exits with a status code 1.
# Arguments:
#   $1: Argument to check for emptiness
assert_not_empty () {
  if [ -z "$1" ]; then
    echo "No argument supplied"

    # Exit with an error code if no argument is provided
    exit 1
  fi
}

# Finds and retrieves the latest version of the documentation based on the directory structure.
# Assumes the documentation versions are numeric folders within 'build/versions/'.
get_latest_version () {
  local latest
  local latest_version

  # Find the latest numeric folder within 'build/versions/' structure
  latest=$(ls -d build/versions/[0-9]* | sort -V | tail -1)

  # Extract the version number from the latest directory path
  latest_version=$(basename "${latest}")

  # Output the latest version number
  echo "${latest_version}"
}

# Creates a 'nightly' version of the documentation that points to the current versioned docs
# located at the root-level `/docs` directory.
create_nightly () {
  local version
  version="$(cat ../version.txt)"

  echo " --> create nightly"
  echo  "     ... version: ${version}"

  # Remove any existing 'nightly' directory and recreate it
  rm -rf build/versions/nightly/
  mkdir -p build/versions/nightly/docs

  # Create symbolic links and copy configuration files for the 'nightly' documentation
  cp -rL in-dev/* build/versions/nightly/docs
  mv build/versions/nightly/docs/mkdocs.yml build/versions/nightly
  rm build/versions/nightly/docs/index-release.md

  echo "     ... replace version placeholders in versioned docs"
  find build/versions/nightly/docs -name "*.md" -exec sed -i='' "s/::NESSIE_VERSION::/${version}/g" {} \;
  find build/versions/nightly/docs -name "*.md" -exec sed -i='' "s/::NESSIE_DOCKER_SUFFIX::/-unstable:latest/g" {} \;
  # remove backups created by sed
  find build/versions/nightly/docs -name "*.md=" -exec rm {} +

  cd build/versions/

  # Update version information within the 'nightly' documentation
  update_version "nightly"
  cd - > /dev/null
}

# Creates a 'latest' version of the documentation based on a specified NESSIE_VERSION.
# Arguments:
#   $1: version - The version number of the documentation to be treated as the latest.
create_latest () {
  local version
  version="$1"

  echo " --> create latest from ${version}"

  # Ensure version is not empty
  assert_not_empty "${version}"

  # Remove any existing 'latest' directory and recreate it
  rm -rf build/versions/latest/
  mkdir build/versions/latest/

  # Copy docs + configuration files for the 'latest' documentation
  cp -rL "build/versions/${version}/docs" build/versions/latest/
  cp "build/versions/${version}/mkdocs.yml" build/versions/latest/

  # Exclude the latest-version-NUMBER directory from the search index
  # (keeps the 'nessie-latest' in the search index)
  search_exclude_versioned_docs "build/versions/${version}"
  # Only exclude the raw generated .md files (those are included).
  search_exclude_generated_docs "build/versions/latest"

  cd build/versions/

  # Update version information within the 'latest' documentation
  update_version "latest"

  cd - > /dev/null
}

# Updates version information within the mkdocs.yml file for a specified version.
# Arguments:
#   $1: version - The version number used for updating the mkdocs.yml file.
update_version () {
  local version
  version="$1"

  echo " --> update version in mkdocs.yml ${version}"

  # Ensure version is not empty
  assert_not_empty "${version}"

  # Update version information within the mkdocs.yml file using sed commands
  if [ "$(uname)" == "Darwin" ]; then
    sed -i '' -E "s/(^site\_name:[[:space:]]+).*$/\1Nessie ${version}/" "${version}/mkdocs.yml"
  elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    sed -i'' -E "s/(^site_name:[[:space:]]+).*$/\1Nessie ${version}/" "${version}/mkdocs.yml"
  fi
}

# Excludes versioned documentation from search indexing by modifying .md files.
# Arguments:
#   $1: version_docs_dir - The docs/ directory of the versioned documentation to exclude from
#       search indexing.
search_exclude_versioned_docs () {
  echo " --> search exclude version docs"

  local version_docs_dir
  version_docs_dir="$1"

  # Ensure version is not empty
  assert_not_empty "${version_docs_dir}"

  echo "     ... in ${version_docs_dir}"

  cd "${version_docs_dir}"

  # Modify .md files to exclude versioned documentation from search indexing
  python3 -c "import glob
for f in glob.glob('./**/*.md', recursive=True):
  lines = open(f).readlines()
  # Add an empty front-matter, if not present.
  if lines[0] != '---\n':
    lines = ['---\n', '---\n', '<!--start-->\n', '\n'] + lines
  lines = lines[:1] + ['search:\n', '  exclude: true\n'] + lines[1:]
  open(f, 'w').writelines(lines)
"

  cd - > /dev/null
}

# Excludes generated documentation from search indexing by modifying .md files.
# Arguments:
#   $1: version_docs_dir - The docs/ directory of the versioned documentation to exclude from
#       search indexing.
search_exclude_generated_docs () {
  echo " --> search exclude generated docs"

  local version_docs_dir
  version_docs_dir="$1"

  # Ensure version is not empty
  assert_not_empty "${version_docs_dir}"

  echo "     ... in ${version_docs_dir}"

  cd "${version_docs_dir}"

  # Modify .md files to exclude versioned documentation from search indexing
  python3 -c "import glob
for f in glob.glob('./**/generated-docs/*.md', recursive=True):
  lines = open(f).readlines()
  # Add an empty front-matter, if not present.
  if lines[0] != '---\n':
    lines = ['---\n', '---\n', '<!--start-->\n', '\n'] + lines
  lines = lines[:1] + ['search:\n', '  exclude: true\n'] + lines[1:]
  open(f, 'w').writelines(lines)
"

  cd - > /dev/null
}

# Sets up local worktrees for the documentation and performs operations related to different versions.
pull_versioned_docs () {
  echo " --> pull versioned docs"

  # Ensure the remote repository for documentation exists and is up-to-date
  create_or_update_docs_remote

  # Add local worktrees for documentation and javadoc from the remote repository
  echo "     ... build/versions from ${REMOTE}/site-docs"
  git worktree add -f build/versions "${REMOTE}/site-docs"
  echo "     ... build/javadoc from ${REMOTE}/site-javadoc"
  git worktree add -f build/javadoc "${REMOTE}/site-javadoc"

  # Retrieve the latest version of documentation for processing
  local latest_version
  latest_version=$(get_latest_version)

  # Output the latest version for debugging purposes
  echo "     ... latest version is: ${latest_version}"

  # Create the 'latest' version of documentation
  create_latest "${latest_version}"

  # Create the 'nightly' version of documentation
  create_nightly
  search_exclude_versioned_docs "build/versions/nightly/"
}

# Generates docs as markdown include files
generate_docs() {
  cd ..

  ./gradlew :nessie-doc-generator:generateDocs

  cd - > /dev/null
}

# Called during a Nessie release.
# 1. Updates the current Nessie version in `mkdocs.yml`
# 2. Pushes the current contents of `in-dev` as a released version to `site-docs` branch.
# 3. Adjusts the contents of `nav.yml` to contain the new release
#
# Arguments:
#   $1: version - The version number of Nessie release.
release() {
  local RELEASE_VERSION
  local target
  local site_docs_dir
  RELEASE_VERSION="$1"
  site_docs_dir="build/site-docs"
  target="${site_docs_dir}/${RELEASE_VERSION}"

  echo " --> release Nessie ${RELEASE_VERSION}"

  assert_not_empty "${RELEASE_VERSION}"

  # Retrieve the latest version of documentation for processing
  local latest_version
  latest_version=$(get_latest_version)

  echo "     ... latest version is: ${latest_version}"

  echo "     ... update top-level mkdocs.yml"
  sed -i "s/^    nessie: [0-9.]*$/    nessie: ${RELEASE_VERSION}/"  ./mkdocs.yml

  create_or_update_docs_remote

  echo "     ... build/site-docs from ${REMOTE}/site-docs"
  git worktree add -f "${site_docs_dir}" "${REMOTE}/site-docs"
  (cd "${site_docs_dir}" ; git checkout -B RELEASE_site-docs ; git branch --set-upstream-to="${REMOTE}/site-docs")

  echo "     ... copying release version doc contents"
  rm -rf "${target}"
  mkdir -p "${target}"/docs
  cp -rL in-dev/* "${target}/docs"
  mv "${target}/docs/mkdocs.yml" "${target}"

  echo "     ... replace index.md with index-release.md"
  mv "${target}/docs/index-release.md" "${target}/docs/index.md"

  echo "     ... replace title in versioned mkdocs.yml"
  sed -i'' -E "s/(^site_name:[[:space:]]+).*$/\1\"Nessie ${RELEASE_VERSION}\"/" "${target}/mkdocs.yml"

  echo "     ... replace version placeholders in versioned docs"
  find "${target}" -name "*.md" -exec sed -i='' "s/::NESSIE_VERSION::/${RELEASE_VERSION}/g" {} \;
  find "${target}" -name "*.md" -exec sed -i='' "s/::NESSIE_DOCKER_SUFFIX::/:${RELEASE_VERSION}/g" {} \;
  # remove backups created by sed
  find "${target}" -name "*.md=" -exec rm {} +

  echo "     ... adding release to nav.yml"
  sed -i "s/ RELEASE_PLACEHOLDER_MARKER$/ RELEASE_PLACEHOLDER_MARKER\\n    - Nessie ${RELEASE_VERSION}: '\!include build\\/versions\\/${RELEASE_VERSION}\\/mkdocs.yml'/" ./nav.yml

  # Exclude the previous latest version from the search index
  search_exclude_versioned_docs "${site_docs_dir}/${latest_version}/"

  echo "     ... committing to local site-docs branch"
  (cd "${site_docs_dir}" ; git add . ; git commit -m "Add Nessie release ${RELEASE_VERSION}")
}

# Cleans up artifacts and temporary files generated during documentation management.
clean () {
  echo " --> clean"

  # Temporarily disable script exit on errors to ensure cleanup continues
  set +e 

  # Remove temp directories and related Git worktrees
  rm -rf build/versions/latest &> /dev/null
  rm -rf build/versions/nightly &> /dev/null

  git worktree remove build/versions &> /dev/null
  git worktree remove build/javadoc &> /dev/null

  # Remove any remaining artifacts
  rm -rf build/javadoc build/versions site/ build/

  set -e # Re-enable script exit on errors
}
