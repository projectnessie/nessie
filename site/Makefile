
.PHONY: help
help: # Show help for each of the Makefile recipes.
	@grep -E '^[a-zA-Z0-9 -]+:.*#'  Makefile | sort | while read -r l; do printf "\033[1;32m$$(echo $$l | cut -f 1 -d':')\033[00m:$$(echo $$l | cut -f 2- -d'#')\n"; done

.PHONY: serve
serve: # Clean, build, and run the docs site locally.
	bin/serve.sh

.PHONY: build
build: # Clean and build the docs site locally.
	bin/build.sh

.PHONY: clean
clean: # Clean the local docs site.
	bin/clean.sh
