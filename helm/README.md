# Nessie Helm chart

## Installation from local directory
```bash
$ helm install --namespace nessie-ns nessie helm/nessie
```

## Installation from Helm repo
```bash
$ helm repo add nessie-helm https://charts.projectnessie.org
$ helm repo update
$ helm install --namespace nessie-ns nessie nessie-helm/nessie
```

## Uninstalling the Chart

```bash
$ helm uninstall --namespace nessie-ns nessie
```

## Configuration
### Nessie Configuration Parameters
The following table lists the configurable parameters of the Nessie chart and their default values.

| Parameter  | Description | Default |
| -----------| ----------- | ------- |
| replicaCount | The number of replicas to run | `1` |
| image | Nessie Image settings | |
| image.repository | The nessie image to use | `projectnessie/nessie` |
| image.pullPolicy | Image pull policy, such as `IfNotPresent` / `Always` / `Never` | `IfNotPresent` |
| image.tag | Overrides the image tag whose default is the chart version | `version` from `Chart.yaml` |
| logLevel | The default logging level to be used | `INFO` |
| versionStoreType | Version store to use: `INMEMORY` / `ROCKS` / `DYNAMO` / `MONGO` | `INMEMORY` |
| rocksdb | Configuration specific to `versionStoreType: ROCKS` | |
| rocks.dbPath | Sets RocksDB storage path | `/tmp/rocks-nessie` |
| dynamodb | Configuration specific to `versionStoreType: DYNAMO` | |
| dynamodb.region | The region to configure for Dynamo | `us-west-2` |
| dynamodb.secret.name | The name of the secret where credentials are stored | `awscreds` |
| dynamodb.secret.awsAccessKeyId | The AWS access key id in the secret | `aws_access_key_id` |
| dynamodb.secret.awsSecretAccessKey | The AWS secret access key | `aws_secret_access_key` |
| mongodb | Configuration specific to `versionStoreType: MONGO` | |
| mongodb.name | The database name | `nessie` |
| mongodb.connectionString | The database connection string | `mongodb://localhost:27017` |
| authentication | Authentication settings | |
| authentication.enabled | Configures whether authentication is enabled | `false` |
| authentication.oidcAuthServerUrl | Sets the base URL of the OpenID Connect (OIDC) server. Needs to be overridden with `authentication.enabled=true` | `http://127.255.0.0:0/auth/realms/unset/` |
| authentication.oidcClientId | Set the OIDC client ID when `authentication.enabled=true`. Each application has a client ID that is used to identify the application | `nessie` |
| jaegerTracing | Jaeger Tracing configuration | |
| jaegerTracing.enabled | Determines whether Jaeger Tracing for Nessie is enabled or not via `true` / `false` | `false` |
| jaegerTracing.endpoint | The traces endpoint, in case the client should connect directly to the Collector, such as `http://jaeger-collector:14268/api/traces` | None |
| jaegerTracing.serviceName | The Jaeger service name | `nessie` |
| jaegerTracing.publishMetrics | Determines whether metrics should be published or not via `true` / `false` | `true` |
| jaegerTracing.samplerType | The sampler type (`const`, `probabilistic`, `ratelimiting` or `remote`) | `ratelimiting` |
| jaegerTracing.samplerParam | 1=Sample all requests. Set `samplerParam` to somewhere between 0 and 1, e.g. 0.50, if you do not wish to sample all requests | `1` |
| serviceMonitor | Configuration specific to a `ServiceMonitor` for the Prometheus Operator | |
| serviceMonitor.enabled | Specifies whether a `ServiceMonitor` for Prometheus operator should be created | `true` |
| serviceMonitor.interval | Interval at which metrics should be scraped | `30s` |
| serviceMonitor.labels | Additional labels to add to the created `ServiceMonitor` so that Prometheus can properly pick it up | None |


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

## Dev installation

* Install Minikube as described in https://minikube.sigs.k8s.io/docs/start/
* Install Helm as described in https://helm.sh/docs/intro/install/ 
* Start Minikube cluster: `minikube start`
* Create K8s Namespace: `kubectl create namespace nessie-dev`
* Install Nessie Helm chart: `helm install nessie -n nessie-dev helm/nessie`

