# Getting Started with Nessie and Iceberg REST

Nessie supports Apache Iceberg REST! Some of the features are

* All clients that "speak" Iceberg REST can work with Nessie, whether it's running under Java, Scala, Python or Rust.
* Nessie supports S3 and compatible object stores like [MinIO](https://min.io/), and supports both request signing and
  session tokens.
* (Currently experimental) support for Google Cloud Storage and ADLS Gen2.
* Plus all the existing Nessie features.

To give Nessie with Iceberg REST a try, we prepared a Docker/Podman compose file for you.
Running Nessie on your laptop and accessing it using Spark SQL and Iceberg REST is not difficult.
The following starts Nessie with Minio and a predefined bucket.

```bash
git clone projectnessie/nessie
cd nessie/docker
docker-compose -f catalog-auth-s3/docker-compose.yml up
# or use podman-compose, if you're using Podman
```

Once all the containers are running, you can run Spark-SQL:

```bash
spark-sql  \
  --packages "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:{{ versions.nessie }},org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{{ versions.iceberg }},org.apache.iceberg:iceberg-aws-bundle:{{ versions.iceberg }}" \
  --conf spark.sql.extensions=org.projectnessie.spark.extensions.NessieSparkSessionExtensions,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalogImplementation=in-memory \
  --conf "spark.sql.catalog.nessie.scope=catalog sign" \
  --conf spark.sql.catalog.nessie.oauth2-server-uri=http://127.0.0.1:8080/realms/iceberg/protocol/openid-connect/token \
  --conf spark.sql.catalog.nessie.credential=client1:s3cr3t \
  --conf spark.sql.catalog.nessie.uri=http://127.0.0.1:19120/iceberg/main/ \
  --conf spark.sql.catalog.nessie.type=rest \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog
```

!!! note
    With Nessie, all necessary configuration about the object store (in the above example it is S3 via MinIO) is
    pushed from Nessie to the Iceberg client used by Spark. Requests from Spark/Iceberg to the object store are
    "secured" via Nessie, in the above example using S3 request signing. This means, that you do not need to
    configure object store credentials on your Iceberg REST clients.

And that's basically all to connect to Nessie using Iceberg REST and try it out _locally_ - from there it just works.

The above docker/podman-compose configuration does a bunch of things for the convenience of a demo/try-out.
A production-like setup would need a couple configuration settings. In other words, Nessie needs to know a few things,
before it can serve Iceberg REST:

* The name and (object store) location of your warehouse, [see configuration reference](../../nessie-latest/configuration/#catalog-and-iceberg-rest-settings)
* Configuration of the object store (S3/GCS/ADLS, cloud region and credentials), [see configuration reference](../../nessie-latest/configuration/#s3-settings)

