# In-development of Nessie Catalog

## Getting Started

1. Generate a local Docker image using
   ```bash
   tools/dockerbuild/build-push-images.sh \
     --gradle-project :nessie-quarkus \
     --project-dir servers/quarkus-server \
     --local localhost/projectnessie/nessie
   ```
2. Start "all the machinery" using
   ```bash
   docker-compose -f docker/catalog-full/docker-compose.yml up
   ```
   or, if you are using Podman,
   ```bash
   podman-compose -f docker/catalog-full/docker-compose.yml up
   ```
3. Run Spark SQL using Iceberg REST against Nessie:
   ```bash
   catalog/bin/spark-sql.sh --no-nessie-start
   ```
   Switch to "nessie" in Spark SQL using
   ```sql
   USE nessie;
   ```
4. Run Flink using Iceberg REST against Nessie:
   ```bash
   catalog/bin/flink.sh --no-nessie-start
   ```
5. (Trino coming soon)
 
