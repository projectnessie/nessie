# Setting Up Nessie

<iframe width="780" height="500" src="https://www.youtube.com/embed/QUmOU8ea_i4" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>

As part of each release, Nessie is made available as a Docker image. This is the easiest
and fastest way to try out Nessie locally and test all its capabilities.

The primary repository for Nessie images is [GitHub Container Registry]. Images are also mirrored
to [Quay.io]. Note that Nessie images are no longer published or synced to Docker Hub.

[GitHub Container Registry]: https://ghcr.io/projectnessie/nessie
[Quay.io]: https://quay.io/repository/projectnessie/nessie

The image is relatively small and builds on top of standard base images. To get started:

```bash
docker pull ghcr.io/projectnessie/nessie
```

You should see something like this:

```text
Pulling from ghcr.io/projectnessie/nessie
0fd3b5213a9b: Already exists
aebb8c556853: Already exists
a50558612231: Pull complete
Digest: sha256:bda3dead4eb51a4c0ff87c7ce5a81ad49a37dd17d785f2549f4559f06cbf24d6
Status: Downloaded newer image for ghcr.io/projectnessie/nessie
```

Once the image is downloaded, you can start it with:

```bash
docker run -p 19120:19120 ghcr.io/projectnessie/nessie
```

You should see something like this:

```text
Starting the Java application using /opt/jboss/container/java/run/run-java.sh ...
INFO exec -a "java" java -XX:MaxRAMPercentage=80.0 -XX:+UseParallelGC -XX:MinHeapFreeRatio=10 -XX:MaxHeapFreeRatio=20 -XX:GCTimeRatio=4 -XX:AdaptiveSizePolicyWeight=90 -XX:+ExitOnOutOfMemoryError -cp "." -jar /deployments/quarkus-run.jar 
INFO running in /deployments
 _   _               _         ____
| \ | |             (_)       / __ \
|  \| | ___  ___ ___ _  ___  / /__\/ ___ _ ____   _____ _ __
| . ` |/ _ \/ __/ __| |/ _ \ \___. \/ _ \ '__\ \ / / _ \ '__|
| |\  |  __/\__ \__ \ |  __/ /\__/ /  __/ |   \ V /  __/ |
\_| \_/\___||___/___/_|\___| \____/ \___|_|    \_/ \___|_|

                               https://projectnessie.org/

                                     Powered by Quarkus 3.5.0
2024-01-27 17:49:20,685 INFO  [org.pro.eve.ser.EventSubscribers] (main) Starting subscribers...
2024-01-27 17:49:20,685 INFO  [org.pro.eve.ser.EventSubscribers] (main) Done starting subscribers.
2024-01-27 17:49:20,820 INFO  [org.pro.qua.pro.sto.PersistProvider] (main) Creating/opening version store IN_MEMORY ...
2024-01-27 17:49:20,846 INFO  [org.pro.qua.pro.sto.PersistProvider] (main) Using IN_MEMORY version store, with 3903 MB objects cache
2024-01-27 17:49:20,876 INFO  [io.quarkus] (main) nessie-quarkus 0.73.0 on JVM (powered by Quarkus 3.5.0) started in 2.229s. Listening on: http://0.0.0.0:19120
2024-01-27 17:49:20,876 INFO  [io.quarkus] (main) Profile prod activated. 
2024-01-27 17:49:20,876 INFO  [io.quarkus] (main) Installed features: [agroal, amazon-dynamodb, cassandra-client, cdi, google-cloud-bigtable, hibernate-validator, jdbc-postgresql, logging-sentry, micrometer, mongodb-client, narayana-jta, oidc, opentelemetry, reactive-routes, resteasy, resteasy-jackson, security, security-properties-file, smallrye-context-propagation, smallrye-health, smallrye-openapi, swagger-ui, vertx]
```

!!! note
    If you see a warning about `OIDC Server is not available` in the logs, you can safely ignore it 
    for now. This happens in older Nessie versions because Nessie was configured to use OIDC by 
    default to authenticate users, but no OIDC server was configured out of the box.

If you need to configure Nessie, you can do so by passing in environment variables. For example, you
can change the port Nessie listens on by passing in the `QUARKUS_HTTP_PORT` environment variable:

```bash
docker run -p 19120:19120 ghcr.io/projectnessie/nessie
```

Check all the available configuration options in the 
[configuration reference](../nessie-latest/configuration.md).

From there, you can use one of the three main Nessie integrations of:

* Take a look at your current empty repository in the [Web UI](./ui.md)
* NessieCatalog for [Spark via Iceberg](../iceberg/spark.md) integration
* Try [Nessie on Kubernetes](./kubernetes.md), on your cloud provider or 
  [on your laptop](minikube.md)

You can also install the [Nessie CLI/REPL](../nessie-latest/cli.md).

```bash
curl -L -o nessie-cli-{{ versions.nessie }}-runner.jar \
  https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-cli-{{ versions.nessie }}-runner.jar
```
