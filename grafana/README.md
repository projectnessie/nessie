# Prometheus & Grafana for Nessie

## Dev installation for local experiments

See the [Helm Dev installation](https://github.com/projectnessie/nessie/blob/main/helm/nessie/README.md#dev-installation) section for how to
set up Nessie via Helm on a local dev environment.


## Prometheus & Grafana & Nessie installation

We use the `kube-prometheus-stack` which includes Prometheus & Grafana.
To scrape Nessie metrics we use a [ServiceMonitor](https://github.com/projectnessie/nessie/blob/main/helm/nessie/templates/servicemonitor.yaml).

1. Add Prometheus repo:
    ```sh
    helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
    helm repo update
    ```
1. Install Prometheus
    ```sh
    helm -n nessie-ns install prometheus prometheus-community/kube-prometheus-stack
    ```
1. Install Nessie (see also https://github.com/projectnessie/nessie/blob/main/helm/nessie/README.md)
    ```sh
    helm repo add nessie-helm https://charts.projectnessie.org
    helm repo update
    helm install --namespace nessie-ns nessie nessie-helm/nessie --set 'serviceMonitor.labels.release=prometheus'
    ```
1. Add port forwarding for Prometheus & Grafana
    ```sh
    nohup kubectl -n nessie-ns port-forward svc/prometheus-kube-prometheus-prometheus 9090 &
    nohup kubectl -n nessie-ns port-forward svc/prometheus-grafana 3000:80 &
    ```
1. Connect to Grafana on http://localhost:3000 with Username: `admin`, password: `prom-operator`
1. Import the `nessie.json` Dashboard into Grafana


## Stop/Uninstall everything

```sh
helm uninstall --namespace nessie-ns prometheus
```

See the [Helm Dev installation](https://github.com/projectnessie/nessie/blob/main/helm/nessie/README.md#dev-installation) section for how to stop/uninstall Nessie & minikube.
