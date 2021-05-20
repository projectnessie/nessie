# Setting Up Nessie

<iframe width="780" height="500" src="https://www.youtube.com/embed/QUmOU8ea_i4" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>

As part of each release, Nessie is made available as a fast-start docker
image. This is the easiest and fastest way to try out nessie locally and test all its capabilities.
The image is relatively small and builds on top of standard base images. To get started:

```bash
$ docker pull projectnessie/nessie
```

```bash
Pulling from projectnessie/nessie
0fd3b5213a9b: Already exists
aebb8c556853: Already exists
a50558612231: Pull complete
Digest: sha256:bda3dead4eb51a4c0ff87c7ce5a81ad49a37dd17d785f2549f4559f06cbf24d6
Status: Downloaded newer image for projectnessie/nessie
```

```bash
$ docker run -p 19120:19120 projectnessie/nessie
```

```bash
__  ____  __  _____   ___  __ ____  ______
 --/ __ \/ / / / _ | / _ \/ //_/ / / / __/
 -/ /_/ / /_/ / __ |/ , _/ ,< / /_/ /\ \
--\___\_\____/_/ |_/_/|_/_/|_|\____/___/
2020-10-01 21:50:27,166 INFO  [io.quarkus] (main) nessie-quarkus 0.1-SNAPSHOT native (powered by Quarkus 1.8.1.Final) started in 0.025s. Listening on: http://0.0.0.0:19120
2020-10-01 21:50:27,166 INFO  [io.quarkus] (main) Profile prod activated.
2020-10-01 21:50:27,166 INFO  [io.quarkus] (main) Installed features: [amazon-dynamodb, cdi, hibernate-validator, jaeger, resteasy, resteasy-jackson, security, security-properties-file, sentry, smallrye-health, smallrye-metrics, smallrye-openapi, smallrye-opentracing]
```

Once the docker image is up and running, you can install the [Nessie cli](../tools/cli.md).

```bash
$ pip install pynessie
```

You're now ready to start using Nessie. To create a new branch, you can do
the following:

```bash
# create a branch pointing to the same hash as
# the current default branch (typically the main branch)
$ nessie branch my_branch
```


From there, you can use one of the three main Nessie integrations of:

* Take a look at your current empty repository in the [Web UI](../tools/ui.md)
* NessieCatalog for Iceberg [within Spark](../tools/spark.md)
* Nessie Log Handle for Delta Lake [within Spark](../tools/spark.md)
* Hive or HMS compatible tool use with the [Nessie HMS Bridge](../tools/hive.md)

