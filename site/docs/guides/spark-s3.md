---
title: "Accessing data in S3 with Apache Spark"
---

# Accessing data in S3 with Apache Spark

In this guide we walk through the process of configuring an [Apache Spark](https://spark.apache.org/) session to work 
with data files stored in Amazon [S3](https://aws.amazon.com/s3/) and version history in a local Nessie Server.

Docker is used at the runtime environments for Nessie. Spark is assumed to be installed locally.

## Setting up Nessie Server

Start the Nessie server container from the `projectnessie/nessie` Docker image in default mode.

```shell
docker run -p 19120:19120 ghcr.io/projectnessie/nessie:latest
```

Note: this example will run the Nessie Server using in-memory storage for table metadata. If/when the container
is deleted, Nessie data about table changes will be lost, yet the data files in S3 will remain.

## Setting up Spark Session for Amazon S3

Configure [AWS SDK credentials](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html) in any
way appropriate for the default AWS SDK Credentials Provider Chain.

In this guide we assume an AWS profile (e.g. called `demo`) in defined in `~/.aws/credentials` (or other location
appropriate for your OS) and contains S3 credentials. Make this profile active for CLI tools by exporting its name
in the `AWS_PROFILE` environment variable. For example:

```shell
export AWS_PROFILE=demo
```

Create an Amazon S3 bucket of your own. This guide uses the bucket name `spark-demo1`.

Start a Spark session:

```shell
spark-sql \
 --packages \
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,\
software.amazon.awssdk:bundle:2.20.131,\
software.amazon.awssdk:url-connection-client:2.20.131 \
 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions  \
 --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
 --conf spark.sql.catalog.nessie.warehouse=s3://spark-demo1 \
 --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
 --conf spark.sql.catalog.nessie.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
 --conf spark.sql.catalog.nessie.uri=http://localhost:19120/api/v1 \
 --conf spark.sql.catalog.nessie.ref=main \
 --conf spark.sql.catalog.nessie.cache-enabled=false
```

Note: `spark-demo1` is the name of the S3 bucket that will hold table data files.

Note: the `--packages` option lists modules required for Iceberg to write data files into S3.
Please refer to [Iceberg documentation](https://iceberg.apache.org/docs/latest/aws/#iceberg-aws-integrations)
for the most up-to-date information on how to connect Iceberg to S3.

Note: the word `nessie` in configuration property names is the name of the Nessie catalog in the Spark session.
A different name can be chosen according the user's liking.

## Setting up Spark Session for Minio

Using Minio is mostly the same as using Amazon S3 except that the S3 endpoint and credentials are different.

For this example, start a local Minio server using Docker:

```shell
docker run -p 9000:9000 -p 9001:9001 --name minio \
 -e "MINIO_ROOT_USER=datauser" -e "MINIO_ROOT_PASSWORD=minioSecret" \
 quay.io/minio/minio:latest server /data --console-address ":9001"
```

Configure AWS SDK crendetials the same way you would configure them for Amazon S3 (refer to the section above)
but use relevant Minio credentials. In this example the credentials for the Minio server running in Docker are:
`aws_access_key_id = datauser`, `aws_secret_access_key = minioSecret`. Assuming the credentials are stored in an
AWS profile named `demo`, export the profile name in the `AWS_PROFILE` environment variable. For example:

```shell
export AWS_PROFILE=demo
```

Create a Minio bucket of your own. This guide uses the bucket name `spark1`.

Start a Spark session:

```shell
spark-sql \
 --packages \
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,\
software.amazon.awssdk:bundle:2.20.131,\
software.amazon.awssdk:url-connection-client:2.20.131 \
 --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions  \
 --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
 --conf spark.sql.catalog.nessie.warehouse=s3://minio/spark1 \
 --conf spark.sql.catalog.nessie.s3.endpoint=http://localhost:9000
 --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
 --conf spark.sql.catalog.nessie.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
 --conf spark.sql.catalog.nessie.uri=http://localhost:19120/api/v1 \
 --conf spark.sql.catalog.nessie.ref=main \
 --conf spark.sql.catalog.nessie.cache-enabled=false
```

Note the `s3.endpoint` catalog property. it should point to the appropriate Minio endpoint. In this example it points 
to the local Minio server running in Docker.

## Running DDL and DML in Spark SQL Shell

Once the Spark session initializes, issue a `use` statement to make `nessie` the current catalog:
```
spark-sql> use nessie
```

This command will establish a connection to the Nessie Server. When it is done, it will be possible to create tables
and run DML. For example:

```
spark-sql> CREATE TABLE demo (id bigint, data string);
Time taken: 1.615 seconds
spark-sql> show tables;
demo
Time taken: 0.425 seconds, Fetched 1 row(s)
spark-sql> INSERT INTO demo (id, data) VALUES (1, 'a');
Time taken: 4.017 seconds
spark-sql> SELECT * FROM demo;
1	a
Time taken: 3.225 seconds, Fetched 1 row(s)
```

Branches, merges and other git-like commands can be run as well, as explained in the 
[Getting Started](../guides/index.md) guide. 

Note: The above example uses the `spark-sql` shell, but the same configuration options apply to `spark-shell`.

## Authentication

This example uses implicit AWS authentication via credentials configured in a credentials file plus the `AWS_PROFILE`
environment variable.

The Nessie Server in this example does not require authentication.

If the Nessie Server runs with authentication enabled, additional configuration parameters will be required in the
Spark session. Please refer to the [Authentication in Tools](../nessie-latest/client_config.md) section for details.
