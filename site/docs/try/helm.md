# Nessie Helm Chart

The easiest way to get started with Nessie is to use the Helm chart.

Add the Nessie Helm repo:
```
helm repo add nessie-helm https://charts.projectnessie.org
helm repo update
```

Install the Helm chart:
```
helm install -n nessie-ns -name nessie nessie-helm/nessie
```

Additional docs (incl. configuration settings) can be found [here](https://github.com/projectnessie/nessie/blob/main/helm/README.md). 
