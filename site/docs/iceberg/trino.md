---
title: "Nessie + Iceberg + Trino"
---

# Trino via Iceberg

Trino can be deployed for use with Nessie by any of the methods mentioned in the [Trino Installation Guide](https://trino.io/docs/current/installation.html).
To access Iceberg tables, one needs to configure the Iceberg connector. Refer to the [Trino Iceberg Connector Documentation](https://trino.io/docs/current/connector/iceberg.html?highlight=iceberg+connector) for more information.
Nessie catalog properties required for the Iceberg connector can be found at https://trino.io/docs/current/object-storage/metastores.html#nessie-catalog.

Sample Nessie configuration for Iceberg connector in `etc/catalog/iceberg.properties`:
```properties
connector.name=iceberg
iceberg.catalog.type=nessie
iceberg.nessie-catalog.uri=http://localhost:19120/api/v2
iceberg.nessie-catalog.ref=main
iceberg.nessie-catalog.default-warehouse-dir=s3://warehouse
fs.native-s3.enabled=true
s3.endpoint=https://<s3_url>
s3.region=<aws_region>
s3.aws-access-key=<aws_access_key>
s3.aws-secret-key=<aws_secret_key>
```

!!! note
    1. It is recommended to use Trino version 443 and above, which includes Iceberg 1.5.0 with bug fixes for Nessie integration.
    2. Trino currently lacks SQL support for managing catalog-level branches and tags related to Nessie.
       a. To create branches and tags, use the Nessie CLI available [here](https://projectnessie.org/nessie-latest/cli/).
       b. To switch references (branch or tag), update the catalog property `iceberg.nessie-catalog.ref`.

Nessie can also be accessed via REST catalog path.
Sample Nessie-REST configuration for Iceberg connector in `etc/catalog/iceberg.properties`:
```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://localhost:19120/iceberg
iceberg.rest-catalog.prefix=main
```


