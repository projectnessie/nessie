# Nessie on Kubernetes

The easiest and recommended way to get started with Nessie on Kubernetes is to use the Helm chart
described below. 

!!! note
    We are also working on a Kubernetes Operator for Nessie, but it is not available yet. If you
    are interested in deploying Nessie via an operator, please [get in
    touch](https://projectnessie.org/community/).

!!! note
    See [separate page](reverse-proxy.md) about how to configure Nessie for using a reverse proxy
    like istio or nginx. 

For more information on Helm and Helm charts, see the [Helm docs](https://helm.sh/docs/).

## Installing the Helm chart

Add the Nessie Helm repo:

```bash
helm repo add nessie-helm https://charts.projectnessie.org
helm repo update
```

Install the Helm chart in the `nessie-ns` namespace (create the namespace first if it doesn't
exist), and name the release `nessie`:

```bash
helm install -n nessie-ns nessie nessie-helm/nessie
```

Additional docs (incl. all configuration settings) can be found in the [Nessie Helm chart docs]
hosted in Nessie's GitHub repository. 

[Nessie Helm chart docs]: https://github.com/projectnessie/nessie/blob/main/helm/nessie/README.md

## Customizing the Helm chart

For example, to install the Helm chart with a predefined image, simply do this:

```bash
helm install -n nessie-ns nessie nessie-helm/nessie \
    --set image.repository=ghcr.io/projectnessie/nessie \
    --set image.tag={{ versions.nessie }}
```

It's also useful to create more than one replica of the Nessie server. To do this, simply set the
`replicaCount` value:

```bash
helm install -n nessie-ns nessie nessie-helm/nessie --set replicaCount=3
```

### Configuring memory and CPU

By default, the Helm chart does not set any resource limits. It is generally recommended though
to set memory requests and limits (usually to the same value), as well as CPU requests.

There are no one-size-fits-all values for these settings, so you should adjust them according to
your needs. A good starting point for a production deployment is to set the memory request and
limit to 8Gi or higher, and the CPU request to a minimum of 4.

For example, to set the memory request and limit to 8Gi, and the CPU request to 4 cores, you can
do this:

```bash
helm install -n nessie-ns nessie nessie-helm/nessie \
    --set-string resources.requests.memory=8Gi \
    --set-string resources.limits.memory=8Gi \
    --set-string resources.requests.cpu=4
```

Regarding memory: when a limit is set with `resources.limits.memory`, Nessie will by default use 80%
of that limit as the maximum heap size. For example, if the limit is 8Gi, then the maximum effective
heap size will be 6.4Gi. If you want to change this, you can set the `JAVA_MAX_MEM_RATIO`
environment variable to a different value. For example, to set the maximum heap size to 50% of the
memory limit, you can do this:

```bash
helm install -n nessie-ns nessie nessie-helm/nessie \
    --set-string 'extraEnv[0].name=JAVA_MAX_MEM_RATIO' \
    --set-string 'extraEnv[0].value=50'
```

You can also set `JAVA_MAX_MEM_RATIO` to `0`, in which case the maximum heap size will not be
constrained and will be only bounded by the container's memory limit size itself.

See the server [configuration
page](../nessie-latest/configuration.md#advanced-docker-image-tuning-java-images-only) for more
details about memory settings.

### Configuring database authentication

Nessie supports a variety of version stores, each of which requires different configuration. For
example, the JDBC version store requires a JDBC URL, username and password, while the DynamoDB
version store requires AWS credentials. 

_All database authentication options must be provided as Kubernetes secrets, and these must be
created before installing the Helm chart._

#### Providing secrets for JDBC datastores

* Make sure you have a Secret in the following form (assuming PostgreSQL, but the same applies to
  other JDBC datastores):

```text
> cat $PWD/postgres-creds
postgres_username=YOUR_USERNAME
postgres_password=YOUR_PASSWORD
```

* Create the secret from the given file:

```bash
kubectl create secret generic postgres-creds --from-env-file="$PWD/postgres-creds"
```

* The `postgres-creds` secret will now be picked up when you use `JDBC` as the version store 
when installing Nessie (see below).

#### Providing secrets for MongoDB

* Providing secrets for MongoDB is strongly recommended, but not enforced.
* Make sure you have a Secret in the following form:

```text
> cat $PWD/mongodb-creds
mongodb_username=YOUR_USERNAME
mongodb_password=YOUR_PASSWORD
```

* Create the secret from the given file:

```bash
kubectl create secret generic mongodb-creds --from-env-file="$PWD/mongodb-creds"
```

* The `mongodb-creds` secret will now be picked up when you use `MONGODB` as the version store 
when installing Nessie (see below).

#### Providing secrets for Cassandra

* Providing secrets for Cassandra is strongly recommended, but not enforced.
* Make sure you have a Secret in the following form:

```text
> cat $PWD/cassandra-creds
cassandra_username=YOUR_USERNAME
cassandra_password=YOUR_PASSWORD
```

* Create the secret from the given file:

```bash
kubectl create secret generic cassandra-creds --from-env-file="$PWD/cassandra-creds"
```

* The `cassandra-creds` secret will now be picked up when you use `CASSANDRA` as the version store 
when installing Nessie (see below).

#### Providing secrets for DynamoDB

* Make sure you have a Secret in the following form:

```text
> cat $PWD/awscreds
aws_access_key_id=YOURACCESSKEYDATA
aws_secret_access_key=YOURSECRETKEYDATA
```

* Create the secret from the given file:

```bash
kubectl create secret generic awscreds --from-env-file="$PWD/awscreds"
```

* The `awscreds` secret will now be picked up when you use `DYNAMODB` as the version store 
when installing Nessie (see below).

#### Providing secrets for Bigtable

A secret is not required for Bigtable. If one is present, it is assumed that authentication will use
a service account JSON key. See  [this page](https://cloud.google.com/iam/docs/keys-create-delete)
for details on how to create a service account key.

If no secret is used, then Workload Identity usage is assumed instead; in this case, make sure that
the pod's service account has been granted access to BigTable. See [this
page](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity#authenticating_to)
for details on how to create a suitable service account.

Important: when using Workload Identity, unless the cluster is in Autopilot mode, it is also
required to add the following `nodeSelector` label:

```yaml
iam.gke.io/gke-metadata-server-enabled: "true"
```

This is not done automatically by the chart because this selector would be invalid for Autopilot
clusters.

* Make sure you have a Secret in the following form:

```bash
> cat $PWD/bigtable-creds
sa_json=YOUR_SA_JSON_KEY
```

* Create the secret from the given file:

```bash
kubectl create secret generic bigtable-creds --from-env-file="$PWD/bigtable-creds"
```

* The `bigtable-creds` secret will now be picked up when you use `BIGTABLE` as the version store 
when installing Nessie (see below).

### Configuring the version store

#### Configuring JDBC version stores

!!! note
    When setting up your SQL backend, _both the database (sometimes called catalog) and the 
    schema (sometimes called namespace, for backends that distinguish between database and schema) 
    must be created beforehand, as the Helm chart will not create them for you_. Check your database 
    documentation for more information, especially around the `CREATE DATABASE` and `CREATE SCHEMA` 
    commands. You must also create a user with the necessary permissions to access the database and 
    schema.

Let's assume that we want to use a PostgreSQL service, that the database is called `nessiedb` and
the schema `nessie`. The PostgreSQL service is running at `postgres:5432` in the same namespace.

Next, we need to configure the Helm chart to use the `JDBC` version store type and to pull the
database credentials from the secret that was created previously. We can do this by creating a
`values.yaml` file with the following content:

```yaml
versionStoreType: JDBC
jdbc:
  jdbcUrl: jdbc:postgresql://postgres:5432/nessiedb?currentSchema=nessie
  secret:
    name: postgres-creds
    username: postgres_username
    password: postgres_password
```

Let's now assume that we are using MariaDB or MySQL instead of PostgreSQL. These backends do not
support schemas, thus only the database name needs to be provided. MariaDB and MySQL share the same
JDBC driver (the MariaDB one), so the JDBC URL is roughly the same for both; a minimal JDBC URL for
these backends would look like this:

For MariaDB:

```yaml
jdbcUrl: jdbc:mariadb://mariadb:3306/nessiedb
```

For MySQL:

```yaml
jdbcUrl: jdbc:mysql://mysql:3306/nessiedb
```

In the above examples, `mariadb` and `mysql` are the service names of the MariaDB and MySQL
services, respectively. The database name is `nessiedb`.

!!! note 
    The exact format of the JDBC URL may vary depending on the database you are using. Also,
    JDBC drivers usually support various optional connection properties. Check the documentation of 
    your database and its JDBC driver for more information (for PostgreSQL, [check out this
    page](https://jdbc.postgresql.org/documentation/use/) and for MariaDB, [check out this
    one](https://mariadb.com/kb/en/about-mariadb-connector-j/#connection-strings)).

!!! note 
    While the database and the schema must be created beforehand, the required tables can be
    created automatically by Nessie if they don't exist, in the target database and schema. If they 
    do exist, they will be used as-is. You must ensure that their structure is up-to-date with the
    version of Nessie that you are using. Check the [Nessie release notes] for more information on
    schema upgrades.

[Nessie release notes]: https://github.com/projectnessie/nessie/releases

Then, we can install the Helm chart with the following values:

```bash
helm install -n nessie-ns nessie nessie-helm/nessie -f values.yaml
```

#### Configuring MongoDB version stores

Let's assume that we want to use a MongoDB database. _The database must be created beforehand, as
the Helm chart will not create it for you_. Let's assume that the database is called `nessie`. The
MongoDB service is running at `mongodb:27017` in the same namespace.

Then, we need to configure the Helm chart to use the `MONGODB` version store type and to pull the
database credentials from the secret that was created previously. We can do this by creating a
`values.yaml` file with the following content:

```yaml
versionStoreType: MONGODB
mongodb:
  database: nessie
  connectionString: mongodb://mongodb:27017
  secret:
    name: mongodb-creds
    username: mongodb_username
    password: mongodb_password
```

#### Configuring DynamoDB version stores

Let's assume that we want to use a DynamoDB database in the `us-west-2` region. The tables will be
created automatically by Nessie if they don't exist.

Then, we need to configure the Helm chart to use the `DYNAMODB` version store type and to pull the
AWS credentials from the secret that was created previously. We can do this by creating a
`values.yaml` file with the following content:

```yaml
versionStoreType: DYNAMODB
dynamodb:
  region: us-west-2
  secret:
    name: awscreds
    awsAccessKeyId: aws_access_key_id
    awsSecretAccessKey: aws_secret_access_key
```

#### Configuring Bigtable version stores

Let's assume that we want to use a Bigtable instance named `nessie-bigtable` in the `prod-us`
project, using the default profile id. The tables will be created automatically by Nessie if they
don't exist, but the instance must be created and configured beforehand.

Then, we need to configure the Helm chart to use the `BIGTABLE` version store type and to pull the
Bigtable credentials from the secret that was created previously. We can do this by creating a
`values.yaml` file with the following content:

```yaml
versionStoreType: BIGTABLE
bigtable:
  projectId: prod-us
  instanceId: nessie-bigtable
  appProfileId: default
```

The above will use Workload Identity. If you are using instead a service account JSON key as
described above, you can also specify it in the `values.yaml` file:

```yaml
versionStoreType: BIGTABLE
bigtable:
  projectId: prod-us
  instanceId: nessie-bigtable
  appProfileId: default
  secret:
    name: bigtable-creds
    key: sa_json
```

#### Configuring other datasource types

Other datasource types are supported, and most of them have mandatory and optional configuration
options. Again, check the [Nessie Helm chart docs] for more information.

## Uninstalling the Helm chart

To uninstall the Helm chart and delete the `nessie` release from the `nessie-ns` namespace:

```bash
helm uninstall -n nessie-ns nessie
```

## Troubleshooting

The first step in troubleshooting a Nessie Kubernetes deployment is to check the logs of the Nessie
server pod. You can do this by running:

```bash
kubectl logs -n <namespace> <pod>
```

You can also check the status of the pod:

```bash
kubectl describe pod -n <namespace> <pod>
```

It's also possible to get a terminal into the Nessie pod's main container:

```bash
kubectl exec -it -n <namespace> <pod> -- /bin/bash
```

But beware that the container does not have some tools installed, e.g. `curl`, `wget`, etc. are not
present.

### Troubleshooting connectivity

Connectivity issues require more powerful tools. One useful technique is to run an ephemeral
container in the same pod as the Nessie server, which shares the same network namespace and can
access the Nessie server as `localhost`. This can be done with the `kubectl debug` command:

```bash
kubectl debug -it -n <namespace> <pod> --image=nicolaka/netshoot --target=nessie --share-processes
```

The above example uses the `nicolaka/netshoot` image, which contains a lot of useful tools for
debugging. See the [nicolaka/netshoot Docker Hub page](https://hub.docker.com/r/nicolaka/netshoot)
for more information. The command should give you a shell in the Nessie pod, where you can use
`curl`, `wget`, `netstat`, `dig`, `tcpdump`, etc. 

For example, once you get a shell in the debug container, you can:

* Check which processes are running in the Nessie pod with `ps aux`;
* Check Nessie's management API on port 9000 to see if the server is healthy:
    ```bash
    curl http://127.0.0.1:9000/q/health
    ```
* Check the Nessie server's API endpoint on port 19120:
    ```bash
    curl http://127.0.0.1:19120/api/v2/config
    ```

### Advanced Nessie JVM troubleshooting

JVM issues such as memory leaks, high CPU usage, etc. can be debugged using JVM tools.

The Nessie container ships with few utilities, but it does have `jcmd`, a command-line utility for
interacting with the JVM, installed.

First, get a shell in the Nessie container:

```bash
kubectl exec -it -n <namespace> <pod> -c nessie -- /bin/bash
```

Then, you can use `jcmd` to capture a thread dump, heap dump, etc.:

```bash
jcmd 1 Thread.print
jcmd 1 GC.heap_dump /tmp/heapdump.hprof
```

!!! Tip
    Nessie server PID is usually 1. You can double-check with `ps aux` or `jps`.

If you need other JVM tools, such as `jfr` or `async-profiler`, a more complex setup is required.

First, restart the Nessie pod with some extra JVM options. The most useful option to add is the
`--XX:+StartAttachListener` JVM option; without it, the JVM will not allow attaching to it and tools
like `jcmd` will fail.

This can be done by modifying the pod template spec in the deployment spec, and adding/updating the
`JAVA_OPTS_APPEND` environment variable:

```bash
java_opts_append=$(kubectl get deployment -n <namespace> <deployment> -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="JAVA_OPTS_APPEND")].value}')
kubectl set env -n <namespace> deployment <deployment> JAVA_OPTS_APPEND="$java_opts_append -XX:+StartAttachListener"
```

!!! Warning
    The above command will restart all Nessie pods! Unfortunately that's inevitable, because 
    environment variables cannot be changed in the pod spec directly, if it belongs to a deployment.

Once the target pod is ready to be attached, you will need an image with the required tools. One
example is the `lightrun-platform/koolkits/koolkit-jvm` image, which contains a JVM-based toolset
for debugging Java applications:

```bash
kubectl debug -it -n <namespace> <pod> --image=lightruncom/koolkits:jvm --target=nessie --share-processes
```

See the [JVM KoolKits page](https://github.com/lightrun-platform/koolkits/blob/main/jvm/README.md)
for more information. Beware that the image is quite large, so it may take some time to download.

A few preliminary commands may need to be executed prior to be able to use the tools, for example if
users and groups don't match: the Nessie process runs as UID 10000 and GID 10001 by default, while
in many debug containers, the running user is root (UID 0). If that is the case, you won't be able
to attach to the Nessie JVM. You can solve the problem by creating a new user with the same UID and
GID as the Nessie process. In the case of JVM KoolKits, you also need to copy a few files before
switching to the new user; here is the whole snippet to run:

```bash
addgroup --gid 10001 debug
adduser --home /home/debug --uid 10000 --gid 10001 --disabled-password --gecos "" debug
cp -Rf /root/.sdkman/ /home/debug/ && chown -R 10000:10001 /home/debug/.sdkman
cp /root/.bashrc /home/debug/ && chown 10000:10001 /home/debug/.bashrc 
su - debug
```

After running the above commands, you should be able to use `jps`, `jcmd`, `jmap`, etc. as well as
other tools like `jfr` (Java Flight Recorder), etc. For example, you can use JFR to record a
profile for 30 seconds:

```bash
jcmd 1 JFR.start duration=30s filename=/tmp/profile.jfr
```

Note that the profile will be saved in the Nessie container, not the debug container. You can copy
it to your local machine with `kubectl cp`:

```bash
kubectl cp -n <namespace> <pod>:/tmp/profile.jfr profile.jfr
```

### Remote debugging

If the above doesn't help, you can also enable remote debugging, by attaching to the Nessie pod with
a remote debugger.

Again, this will require restarting the Nessie pod with some extra JVM options. The most useful
option to add is the `-agentlib:jdwp` JVM option, which enables the Java Debug Wire Protocol (JDWP).

This can be done more easily done by simply setting the `JAVA_DEBUG` and `JAVA_DEBUG_PORT`
environment variables in the deployment spec:

```bash
kubectl set env -n <namespace> deployment <deployment> JAVA_DEBUG="true" JAVA_DEBUG_PORT="*:5005"
```

Once the pod is ready to be debugged, you must forward the 5005 port to your local machine:

```bash
kubectl port-forward -n <namespace> <pod> 5005:5005
```

Then you can attach to the Nessie server with your favorite IDE or command-line debugger.

!!! Tip
    If you are using IntelliJ IDEA, you should create a new "Remote JVM Debug" run configuration. 
    Using the "Attach to process..." option will not work, since it only supports attaching to local 
    JVM processes.
