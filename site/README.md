## Nessie Site

This directory contains the source for the Nessie docs site.

* Site structure is maintained in mkdocs.yml
* Pages are maintained in markdown in the `docs/` folder
* Links use bare page names: `[link text](target-page)`

### Installation

The site is built using mkdocs. To install mkdocs and the theme, run:

```
pip install mkdocs
pip install mkdocs-material
pip install mkdocs-minify-plugin
pip install mkdocs-redirects
pip install pymdown-extensions
pip install mkdocs-awesome-pages-plugin
```

### Local Changes

To see changes locally before committing, use mkdocs to run a local server from this directory.

```
mkdocs serve
```

### Publishing

After site changes are committed, you can publish the site with this command:

```
mkdocs gh-deploy
```

This assumes that the remote is named `dremio` and will push to the `gh-pages` branch. To use a different remote add `-r <remote-name>`.

### Generate python API docs

1. Enter into `python` directory and activate venv. 
2. Run `make docs`.

### Generate java API docs

1. from root run `mvn javadoc:aggregate`
2. `cp -R target/site/apidocs/* site/docs/javadoc`
