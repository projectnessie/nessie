# Kubernetes Operator for Nessie

## Overview

This module is a Kubernetes Operator for Nessie.

**WARNING: This is a work in progress and is not ready for production use.**

The operator is designed to manage the lifecycle of Nessie instances in a Kubernetes cluster. It can
also be used to run GC jobs.

This project was created using [Operator SDK]:

```bash
operator-sdk init --plugins=quarkus --domain=projectnessie.org --project-name=nessie-operator
operator-sdk create api --plugins=quarkus --group nessie --version=v1alpha1 --kind=Nessie
operator-sdk create api --plugins=quarkus --group nessie --version=v1alpha1 --kind=NessieGC
```

[Operator SDK]:https://sdk.operatorframework.io/docs/cli/operator-sdk/

## Development

### Prerequisites

- Operator SDK: https://sdk.operatorframework.io/docs/installation/

### Adhoc testing with Minikube

Install [minikube](https://minikube.sigs.k8s.io/docs/start/).

If you need ingress, install the ingress addon:

```bash
minikube addons enable ingress
minikube tunnel
```

Create the `nessie-operator` and `nessie-ns` namespaces (only needed once):

```bash
kubectl create namespace nessie-operator
kubectl create namespace nessie-ns
```

Grant admin rights to the `nessie-operator` service account (only needed once):

```bash
kubectl apply -f - <<EOF
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: nessie-operator-admin
subjects:
  - kind: ServiceAccount
    name: nessie-operator
    namespace: nessie-operator
roleRef:
  kind: ClusterRole
  name: cluster-admin
  apiGroup: ""
EOF
```

Build the operator docker image _inside_ minikube to facilitate testing (doesn't need a registry):

```bash
eval $(minikube docker-env)
cd operator
make docker-build PULL_POLICY=IfNotPresent
```

Note: the `PULL_POLICY=IfNotPresent` is required to avoid pulling the image from a registry.

Install the CRDs and deploy the operator in the `nessie-operator` namespace:

```bash
make install deploy
```

Create a Nessie resource in the `nessie-ns` namespace:

```bash
kubectl apply -n nessie-ns -f examples/nessie-simple.yaml
```

You should see 1 pod, 1 deployment and 1 service running. More example resources can be found in the
`examples` directory.
