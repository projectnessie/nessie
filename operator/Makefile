
VERSION ?= $(shell cat ../version.txt | sed -e 's/.*-SNAPSHOT/latest/g')
RELEASE_VERSION ?= $(shell cat ../version.txt | sed -e 's/-SNAPSHOT//g')

# CHANNELS define the bundle channels used in the bundle.
# Add a new line here if you would like to change its default config. (E.g CHANNELS = "candidate,fast,stable")
# To re-generate a bundle for other specific channels without changing the standard setup, you can:
# - use the CHANNELS as arg of the bundle target (e.g make bundle CHANNELS=candidate,fast,stable)
# - use environment variables to overwrite this value (e.g export CHANNELS="candidate,fast,stable")
ifneq ($(origin CHANNELS), undefined)
BUNDLE_CHANNELS := --channels=$(CHANNELS)
endif

# DEFAULT_CHANNEL defines the default channel used in the bundle.
# Add a new line here if you would like to change its default config. (E.g DEFAULT_CHANNEL = "stable")
# To re-generate a bundle for any other default channel without changing the default setup, you can:
# - use the DEFAULT_CHANNEL as arg of the bundle target (e.g make bundle DEFAULT_CHANNEL=stable)
# - use environment variables to overwrite this value (e.g export DEFAULT_CHANNEL="stable")
ifneq ($(origin DEFAULT_CHANNEL), undefined)
BUNDLE_DEFAULT_CHANNEL := --default-channel=$(DEFAULT_CHANNEL)
endif
BUNDLE_METADATA_OPTS ?= $(BUNDLE_CHANNELS) $(BUNDLE_DEFAULT_CHANNEL)

# IMAGE_TAG_BASE defines the docker.io namespace and part of the image name for remote images.
# This variable is used to construct full image tags for bundle and catalog images.
IMAGE_TAG_BASE ?= ghcr.io/projectnessie/nessie-operator

# BUNDLE_IMG defines the image:tag used for the bundle.
# You can use it as an arg. (E.g make bundle-build BUNDLE_IMG=<some-registry>/<project-name-bundle>:<tag>)
BUNDLE_IMG ?= $(IMAGE_TAG_BASE)-bundle:$(VERSION)

# Image URL to use all building/pushing image targets
IMG ?= $(IMAGE_TAG_BASE):$(VERSION)

PULL_POLICY ?= $(shell [ "$(VERSION)" = "latest" ] && echo "Always" || echo "IfNotPresent")
PLATFORM ?= linux/$(shell arch)

all: docker-build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk commands is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Build

docker-build: ## Build docker image with the manager.
	../gradlew :nessie-operator:spotlessApply :nessie-operator:clean :nessie-operator:build -x check \
		-Dquarkus.container-image.build=true \
		-Dquarkus.container-image.image=${IMG} \
		-Dquarkus.jib.platforms=${PLATFORM} \
		-Dquarkus.kubernetes.prometheus.generate-service-monitor=false \
		-Dquarkus.kubernetes.image-pull-policy=${PULL_POLICY}

docker-push: ## Build and push docker image with the manager.
	../gradlew :nessie-operator:spotlessApply :nessie-operator:clean :nessie-operator:build -x check \
		-Dquarkus.container-image.build=true \
		-Dquarkus.container-image.push=true \
		-Dquarkus.container-image.image=${IMG} \
		-Dquarkus.jib.platforms=${PLATFORM} \
		-Dquarkus.kubernetes.prometheus.generate-service-monitor=false \
		-Dquarkus.kubernetes.image-pull-policy=${PULL_POLICY}

##@ Deployment

install: ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	@$(foreach file, $(wildcard build/kubernetes/*-v1.yml), kubectl apply -f $(file);)

uninstall: ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config.
	@$(foreach file, $(wildcard build/kubernetes/*-v1.yml), kubectl delete -f $(file);)

deploy: ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	kubectl apply -f build/kubernetes/kubernetes.yml

undeploy: ## Undeploy controller from the K8s cluster specified in ~/.kube/config.
	kubectl delete -f build/kubernetes/kubernetes.yml

##@ Helm

helm-install: ## Install CRDs and the operator using Helm.
	helm install nessie-operator build/helm -n nessie-operator

helm-upgrade: ## Upgrade CRDs and the operator using Helm.
	helm upgrade nessie-operator build/helm -n nessie-operator

helm-uninstall: ## Uninstall CRDs and the operator using Helm.
	helm uninstall nessie-operator -n nessie-operator

##@ Bundle

.PHONY: bundle
bundle:  ## Generate bundle manifests and metadata, then validate generated files.
	cat build/kubernetes/* | operator-sdk generate bundle -q --overwrite --version $(RELEASE_VERSION) $(BUNDLE_METADATA_OPTS)
	operator-sdk bundle validate ./bundle
	# TODO use quarkus

.PHONY: bundle-build
bundle-build: ## Build the bundle image.
	docker build -f build/bundle/nessie-operator/bundle.Dockerfile -t $(BUNDLE_IMG) build/bundle/nessie-operator

.PHONY: bundle-push
bundle-push: ## Push the bundle image.
	docker push $(BUNDLE_IMG)
