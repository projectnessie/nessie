# Kubernetes Operator for Nessie

This module is a Kubernetes Operator for Nessie.

**WARNING: This is a work in progress and is not ready for production use.**

The operator is designed to manage the lifecycle of Nessie instances in a Kubernetes cluster.

This project was bootstrapped using [Operator SDK]:

```bash
operator-sdk init --plugins=quarkus --domain=projectnessie.org --project-name=nessie-operator
operator-sdk create api --plugins=quarkus --group nessie --version=v1alpha1 --kind=Nessie
operator-sdk create api --plugins=quarkus --group nessie --version=v1alpha1 --kind=NessieGc
```

[Operator SDK]:https://sdk.operatorframework.io/docs/cli/operator-sdk/
