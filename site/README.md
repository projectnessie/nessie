# Nessie Site

This directory contains the source for the Nessie docs site.

* Site structure is maintained in mkdocs.yml
* Pages are maintained in markdown in the `docs/` folder
* Versioned docs are in the `docs/docs/` folder - do not edit those, because those live in a different branch
  than `main`.
* Links use bare page names: `[link text](target-page)`

## Installation

Requirements: local installation of Python 3.11

The site is built using mkdocs, with some tooling around.

Tip: You do not need to setup a Python virtualenv yourself.

1. Build the site locally. This step will create a virtualenv and install the Python dependencies. It does also
   fetch the versioned docs from GitHub.
   ```bash
   make build
   ```
2. Serve the site locally:
   ```bash
   make serve
   ```

## Directories and their meanings

* `.cache/` - Cache for the mkdocs privacy plugin (ignored by Git)
* `bin/` - Shell scripts to help developing, building, deploying and releasing the site
* `build/` - Contents of the remote branches `site-docs` + `site-javadoc` (ignored by Git)
* `docs/` - Non-Nessie version dependent docs
* `in-dev/` - Version dependent docs - for the current "in-development/nightly/snapshot".
  Eventually becomes the content for the next release.
* `overrides/` - mkdocs overrides
* `venv/` - Python virtual environment (ignored by Git)

## _Current_ state of the documentation (aka in-development / nightly / snapshot)

The `in-dev/` folder appears on the projectnessie.org site under `nessie-nightly`:
[projectnessie.org/versions/nightly](https://projectnessie.org/nessie-nightly/) !

The `mkdocs.yml` here is used for each version - it contains the _navigation_ for the Nessie version.

Hints:

* Mkdocs `extras` CANNOT be changed in an included [`mkdocs.yml`](./mkdocs.yml). Since the docs of Nessie versions are
  _included_ (see [nav.yml](../../nav.yml)), all pages would refer to the _latest_ Nessie release defined in the [top
  level `mkdocs.yml`](../../mkdocs.yml). Therefore the version of a release is replaced using good-old `sed` when a
  release is cut. Use the marker `::NESSIE_VERSION::` placeholder to let the tools replace it with the right version
  string.
* The `index.md` in `in-dev/docs/` contains the markdown for nightlies/snapshots.
* The `index-release.md` in `in-dev/docs/` will replace the `index.md` for Nessie releases.
