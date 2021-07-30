# Nessie Helm chart

## Installation

```
helm install --namespace nessie --generate-name helm/nessie
```



## Dev installation

* Install Minicube as described in https://minikube.sigs.k8s.io/docs/start/
* Install Helm as described in https://helm.sh/docs/intro/install/ 
* Start Minicube cluster: `minikube start`
* Create K8s Namespace: `kubectl create namespace nessie-dev`
* Install Nessie Helm chart: `helm install nessie -n nessie-dev helm/nessie`

