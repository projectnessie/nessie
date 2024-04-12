# Nessie on Minikube

If you followed our [Docker guide](../guides/docker.md), you can now try Nessie on Kubernetes with
Minikube.

Minikube is a tool that makes it easy to run Kubernetes locally. This document describes how to run
Nessie on Minikube, using the [Nessie Helm chart]. Please familiarize yourself with the Nessie Helm
chart before proceeding.

[Nessie Helm chart]: ../guides/kubernetes.md

## Setup

* Install Minikube as described in https://minikube.sigs.k8s.io/docs/start/
* Install Helm as described in https://helm.sh/docs/intro/install/
* Start Minikube cluster: `minikube start`
* Create a namespace: `kubectl create namespace nessie-ns`

Then install Nessie with Helm:

* From the Helm repo: see instructions in [Nessie Helm chart]
* From Nessie source code root: `helm install nessie -n nessie-ns helm/nessie`
  * Note: this installs whatever is in your `helm/nessie` directory. Use only for development!

The rest of this page describes how to do some common tasks with Nessie on Minikube:

* [Ingress with Minikube](#ingress-with-minikube)
* [OpenTelemetry Collector with Minikube](#opentelemetry-collector-with-minikube)
* [Custom Docker images for Nessie with Minikube](#custom-docker-images-for-nessie-with-minikube)

Additionally, you can check how to set up Ingress with TLS in [this separate
guide](../guides/tls.md).

Once you are done, you can stop or delete Minikube and / or uninstall Nessie:

```sh
helm uninstall --namespace nessie-ns nessie
minikube stop
minikube delete # if you want to delete the VM
```

## Ingress with Minikube

This is broadly following the example from https://kubernetes.io/docs/tasks/access-application-cluster/ingress-minikube/

* Start Minikube as described above and create the namespace `nessie-ns`.
* Install Nessie Helm chart with Ingress enabled, from your local repository root:
  ```bash
  helm install nessie -n nessie-ns helm/nessie \
    --set ingress.enabled=true \
    --set ingress.hosts[0].host='chart-example.local' \
    --set ingress.hosts[0].paths[0]='/'
  ```

* Verify that the IP address is set:
  ```bash
  kubectl get ingress -n nessie-ns
  NAME     CLASS   HOSTS   ADDRESS        PORTS   AGE
  nessie   nginx   *       192.168.49.2   80      4m35s
  ```
* Use the IP from the above output and add it to `/etc/hosts`, for example:
  ```bash
  echo "192.168.49.2 chart-example.local" | sudo tee /etc/hosts`
  ```
* Verify that `curl chart-example.local` works

!!! note
    If you also need to enable TLS, check [this separate guide](./tls.md).

## OpenTelemetry Collector with Minikube

* Start Minikube as described above and create the namespace `nessie-ns`.
* Install cert-manager:

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.12.7/cert-manager.yaml
```

* Install Jaeger Operator:

```bash
kubectl create namespace observability
kubectl apply -f https://github.com/jaegertracing/jaeger-operator/releases/download/v1.53.0/jaeger-operator.yaml -n observability
```

If the above command fails with "failed to call webhook [...] connection refused", then cert-manager
was not yet ready. Wait a few seconds and try again.

* Create a Jaeger instance in Nessie's namespace:

```bash
kubectl apply -n nessie-ns -f - <<EOF
apiVersion: jaegertracing.io/v1
kind: Jaeger
metadata:
  name: jaeger
EOF
```

If the above command fails with "failed to call webhook [...] connection refused", then the Jaeger
Operator was not yet ready. Wait a few seconds and try again.

* Install Nessie Helm chart with OpenTelemetry Collector enabled:

```bash
helm install nessie -n nessie-ns helm/nessie \
  --set tracing.enabled=true \
  --set tracing.endpoint=http://jaeger-collector:4317
```

* Forward ports to Jaeger UI and Nessie UI:

```bash
kubectl port-forward -n nessie-ns service/nessie 19120:19120 &
kubectl port-forward -n nessie-ns service/jaeger-query 16686:16686 &
```

* Open the following URLs in your browser:
  * Nessie UI (to generate some traces): http://localhost:19120
  * Jaeger UI (to retrieve the traces): http://localhost:16686/search

To kill the port forwarding processes, run:

```bash
killall -9 kubectl
```

## Custom Docker images for Nessie with Minikube

You can modify Nessie's code and deploy it to Minikube.

Once you've satisfied with your changes, build the project with:

```bash
./gradlew :nessie-quarkus:quarkusBuild
```

Then, build the Docker image and install it in your Minikube node as follows:

```bash
eval $(minikube docker-env)
docker build -f ./tools/dockerbuild/docker/Dockerfile-server -t nessie-test:latest ./servers/quarkus-server
```

By running `eval $(minikube docker-env)`, you are setting the Docker environment variables to point
to the Minikube Docker daemon. This means that when you run `docker build`, the image will be built
in the Minikube Docker daemon, and not in your local Docker daemon, and it will be available to
Minikube immediately.

Then deploy Nessie with the custom Docker image:

```bash
helm install nessie -n nessie-ns helm/nessie \
  --set image.repository=nessie-test \ 
  --set image.tag=latest
```
