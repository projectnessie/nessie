---
title: "Nessie + Iceberg + Trino"
---

# Trino via Iceberg

Trino comes with Iceberg and Nessie catalog clients installed by default. 
You can deploy Trino using any of the methods mentioned in [this document](https://trino.io/docs/current/installation.html) and to become familiar with the Iceberg connector, refer [this document](https://trino.io/docs/current/connector/iceberg.html?highlight=iceberg+connector).
Nessie catalog properties required for the Iceberg connector can be found at https://trino.io/docs/current/object-storage/metastores.html#nessie-catalog.

Sample Nessie configuration for Iceberg connector in `etc/catalog/iceberg.properties`
```
connector.name=iceberg
iceberg.catalog.type=nessie
iceberg.nessie-catalog.uri=http://localhost:19120/api/v2
iceberg.nessie-catalog.default-warehouse-dir=/Users/foo/wh
iceberg.nessie-catalog.ref=main
```

!!! note
    1. It is recommended to use Trino version 443 and above, which includes Iceberg 1.5.0 with bug fixes for Nessie integration.
    2. Trino currently lacks SQL support for managing catalog-level branches and tags related to Nessie.
       a. To create branches and tags, use the Nessie CLI available [here](https://projectnessie.org/nessie-latest/cli/).
       b. To switch references (branch or tag), update the catalog property `iceberg.nessie-catalog.ref`.


