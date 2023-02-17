<!---
This README.md file was generated with:
https://github.com/norwoodj/helm-docs
Do not modify the README.md file directly, please modify README.md.gotmpl instead.
To re-generate the README.md file, install helm-docs then run from the repo root:
helm-docs --chart-search-root=helm
-->

# Nessie Helm chart

![Version: 0.49.0](https://img.shields.io/badge/Version-0.49.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square)

A Helm chart for Nessie.

**Homepage:** <https://projectnessie.org/>

## Maintainers
* [nastra](https://github.com/nastra)
* [snazy](https://github.com/snazy)
* [dimas-b](https://github.com/dimas-b)
* [adutra](https://github.com/adutra)

## Source Code

* <https://github.com/projectnessie/nessie>

## Installation

### From Helm repo
```bash
$ helm repo add nessie-helm https://charts.projectnessie.org
$ helm repo update
$ helm install --namespace nessie-ns nessie nessie-helm/nessie
```

### From local directory (for development purposes)

From Nessie repo root:

```bash
$ helm install --namespace nessie-ns nessie helm/nessie
```

### Uninstalling the chart

```bash
$ helm uninstall --namespace nessie-ns nessie
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` | Affinity and anti-affinity for nessie pods. See https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#affinity-and-anti-affinity. |
| authentication.enabled | bool | `false` | Specifies whether authentication for the nessie server should be enabled. |
| authentication.oidcAuthServerUrl | string | `"http://127.255.0.0:0/auth/realms/unset/"` | Sets the base URL of the OpenID Connect (OIDC) server. Needs to be overridden with authentication.enabled=true |
| authentication.oidcClientId | string | `"nessie"` | Set the OIDC client ID when authentication.enabled=true. Each application has a client ID that is used to identify the application |
| authorization.enabled | bool | `false` | Specifies whether authorization for the nessie server should be enabled. |
| authorization.rules | object | `{}` | The authorization rules when authorization.enabled=true. Example rules can be found at https://projectnessie.org/features/metadata_authorization/#authorization-rules |
| autoscaling.enabled | bool | `false` | Specifies whether automatic horizontal scaling should be enabled. Do not enable this when using ROCKS version store type. |
| autoscaling.maxReplicas | int | `3` | The maximum number of replicas to maintain. |
| autoscaling.minReplicas | int | `1` | The minimum number of replicas to maintain. |
| autoscaling.targetCPUUtilizationPercentage | int | `80` | Optional; set to zero or empty to disable. |
| autoscaling.targetMemoryUtilizationPercentage | string | `nil` | Optional; set to zero or empty to disable. |
| dynamodb.region | string | `"us-west-2"` | The AWS region to use. |
| dynamodb.secret.awsAccessKeyId | string | `"aws_access_key_id"` | The secret key storing the AWS secret key id. |
| dynamodb.secret.awsSecretAccessKey | string | `"aws_secret_access_key"` | The secret key storing the AWS secret access key. |
| dynamodb.secret.name | string | `"awscreds"` | The secret name to pull AWS credentials from. |
| image.pullPolicy | string | `"IfNotPresent"` | The image pull policy. |
| image.repository | string | `"projectnessie/nessie"` | The image repository to pull from. |
| image.tag | string | `""` | Overrides the image tag whose default is the chart version. |
| ingress.annotations | object | `{}` | Annotations to add to the ingress. |
| ingress.enabled | bool | `false` | Specifies whether an ingress should be created. |
| ingress.hosts | list | `[{"host":"chart-example.local","paths":[]}]` | A list of host paths used to configure the ingress. |
| ingress.tls | list | `[]` | A list of TLS certificates; each entry has a list of hosts in the certificate, along with the secret name used to terminate TLS traffic on port 443. |
| jaegerTracing.enabled | bool | `false` | Specifies whether jaeger tracing for the nessie server should be enabled. |
| jaegerTracing.endpoint | string | `""` | The traces endpoint, in case the client should connect directly to the Collector, e.g. http://jaeger-collector:14268/api/traces |
| jaegerTracing.publishMetrics | bool | `true` | Whether metrics are published if tracing is enabled. |
| jaegerTracing.samplerParam | int | `1` | The request sampling probability. 1=Sample all requests. Set samplerParam to somewhere between 0 and 1, e.g. 0.50, if you do not wish to sample all requests. |
| jaegerTracing.samplerType | string | `"ratelimiting"` | The sampler type (const, probabilistic, ratelimiting or remote). |
| jaegerTracing.serviceName | string | `"nessie"` | The Jaeger service name. |
| logLevel | string | `"INFO"` | The default logging level for the nessie server. |
| mongodb.connectionString | string | `"mongodb://localhost:27017"` | The MongoDB connection string. |
| mongodb.name | string | `"nessie"` | The MongoDB database name. |
| mongodb.secret.name | string | `"mongodb-creds"` | The secret name to pull MongoDB credentials from. |
| mongodb.secret.password | string | `"mongodb_password"` | The secret key storing the MongoDB password. |
| mongodb.secret.username | string | `"mongodb_username"` | The secret key storing the MongoDB username. |
| nodeSelector | object | `{}` | Node labels which must match for the nessie pod to be scheduled on that node. See https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodeselector. |
| podAnnotations | object | `{}` | Annotations to apply to nessie pods. |
| podSecurityContext | object | `{}` | Security context for the nessie pod. See https://kubernetes.io/docs/tasks/configure-pod-container/security-context/. |
| postgres.jdbcUrl | string | `"jdbc:postgresql://localhost:5432/my_database"` | The Postgres JDBC connection string. |
| postgres.secret.name | string | `"postgres-creds"` | The secret name to pull Postgres credentials from. |
| postgres.secret.password | string | `"postgres_password"` | The secret key storing the Postgres password. |
| postgres.secret.username | string | `"postgres_username"` | The secret key storing the Postgres username. |
| replicaCount | int | `1` | The number of replicas to deploy (horizontal scaling). Beware that replicas are stateless; don't set this number > 1 when using ROCKS version store type. |
| resources | object | `{}` | Configures the resources requests and limits for nessie pods. We usually recommend not to specify default resources and to leave this as a conscious choice for the user. This also increases chances charts run on environments with little resources, such as Minikube. If you do want to specify resources, uncomment the following lines, adjust them as necessary, and remove the curly braces after 'resources:'. |
| rocksdb.selectorLabels | object | `{}` | Labels to add to the persistent volume claim spec selector; a persistent volume with matching labels must exist. Leave empty if using dynamic provisioning. |
| rocksdb.storageClassName | string | `"standard"` | The storage class name of the persistent volume claim to create. |
| rocksdb.storageSize | string | `"1Gi"` | The size of the persistent volume claim to create. |
| securityContext | object | `{}` | Security context for the nessie container. See https://kubernetes.io/docs/tasks/configure-pod-container/security-context/. |
| service.annotations | object | `{}` | Annotations to add to the service. |
| service.port | int | `19120` | The port on which the service should listen. |
| service.type | string | `"ClusterIP"` | The type of service to create. |
| serviceAccount.annotations | object | `{}` | Annotations to add to the service account. |
| serviceAccount.create | bool | `true` | Specifies whether a service account should be created. |
| serviceAccount.name | string | `""` | The name of the service account to use. If not set and create is true, a name is generated using the fullname template. |
| serviceMonitor.enabled | bool | `true` | Specifies whether a ServiceMonitor for Prometheus operator should be created. |
| serviceMonitor.interval | string | `""` | The scrape interval; leave empty to let Prometheus decide. Must be a valid duration, e.g. 1d, 1h30m, 5m, 10s. |
| serviceMonitor.labels | object | `{}` | Labels for the created ServiceMonitor so that Prometheus operator can properly pick it up. |
| tolerations | list | `[]` | A list of tolerations to apply to nessie pods. See https://kubernetes.io/docs/concepts/scheduling-eviction/taint-and-toleration/. |
| versionStoreAdvancedConfig | object | `{}` | Advanced version store configuration. The key-value pairs specified here will be passed to the Nessie server as environment variables. See https://projectnessie.org/try/configuration/#version-store-advanced-settings for available properties. Naming convention: to set the property nessie.version.store.advanced.repository-id, use the key: NESSIE_VERSION_STORE_ADVANCED_REPOSITORY_ID. |
| versionStoreType | string | `"INMEMORY"` | Which type of version store to use: INMEMORY, ROCKS, DYNAMO, MONGO, TRANSACTIONAL. |

## Using secrets

### Providing secrets for Dynamo Version Store

* Make sure you have a Secret in the following form:
```
> cat $PWD/awscreds
aws_access_key_id=YOURACCESSKEYDATA
aws_secret_access_key=YOURSECRETKEYDATA
```

* Create the secret from the given file
  `kubectl create secret generic awscreds --from-env-file="$PWD/awscreds"`

* Now you can use `DYNAMO` as the version store when installing Nessie via `helm install -n nessie-ns nessie helm/nessie --set versionStoreType=DYNAMO`.

### Providing secrets for MongoDB

* Providing secrets for MongoDB is strongly recommended, but not enforced.
* Make sure you have a Secret in the following form:
```
> cat $PWD/mongodb-creds
mongodb_username=YOUR_USERNAME
mongodb_password=YOUR_PASSWORD
```

* Create the secret from the given file
  `kubectl create secret generic mongodb-creds --from-env-file="$PWD/mongodb-creds"`

* The `mongodb-creds` secret will now be picked up when you use `MONGO` as the version store when installing Nessie via `helm install -n nessie-ns nessie helm/nessie --set versionStoreType=MONGO`.

### Providing secrets for Transactional Version Store

* Make sure you have a Secret in the following form:
```
> cat $PWD/postgres-creds
postgres_username=YOUR_USERNAME
postgres_password=YOUR_PASSWORD
```

* Create the secret from the given file
  `kubectl create secret generic postgres-creds --from-env-file="$PWD/postgres-creds"`

* Now you can use `TRANSACTIONAL` as the version store when installing Nessie via `helm install -n nessie-ns nessie helm/nessie --set versionStoreType=TRANSACTIONAL`.

## Dev installation

* Install Minikube as described in https://minikube.sigs.k8s.io/docs/start/
* Install Helm as described in https://helm.sh/docs/intro/install/
* Start Minikube cluster: `minikube start`
* Create K8s Namespace: `kubectl create namespace nessie-ns`
* Install Nessie Helm chart: `helm install nessie -n nessie-ns helm/nessie`

### Ingress with Minikube

This is broadly following the example from https://kubernetes.io/docs/tasks/access-application-cluster/ingress-minikube/

* Start Minikube cluster: `minikube start`
* Enable NGINX Ingress controller: `minikube addons enable ingress`
* Verify Ingress controller is running: `kubectl get pods -n ingress-nginx`
* Create K8s Namespace: `kubectl create namespace nessie-ns`
* Install Nessie Helm chart with Ingress enabled:
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
* Use the IP from the above output and add it to `/etc/hosts` via `echo "192.168.49.2 chart-example.local" | sudo tee /etc/hosts`
* Verify that `curl chart-example.local` works

### Stop/Uninstall everything in Dev

```sh
helm uninstall --namespace nessie-ns nessie
minikube delete
```
