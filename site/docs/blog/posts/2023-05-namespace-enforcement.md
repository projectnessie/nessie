---
date: 2023-05-08
authors:
  - abhat
---

# Namespace enforcement in Nessie

Starting from Nessie version 0.52.3, it is required to have existing Namespaces before creating or committing to tables.  
<!-- more -->

In case of tables that have implicit namespaces, new commits will fail after the upgrade as this rule will be enforced.  
Therefore, it is necessary to explicitly create the namespaces.

Namespaces can be created using any of these options:  
* Using SQL command (for Spark, `CREATE NAMESPACE catalogName.namespaceName`).   
* Using [Iceberg Catalog API](https://github.com/apache/iceberg/blob/3ab00171b48bb793a3b71845eb12d5077ba892f1/nessie/src/main/java/org/apache/iceberg/nessie/NessieCatalog.java#L262).   
* Using [content generator tool](https://github.com/projectnessie/nessie/blob/main/tools/content-generator/README.md) to batch create the missing namespaces with `create-missing-namespaces` option.

[More info from the spec](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md#200-beta1).   
[Issue background](https://github.com/projectnessie/nessie/issues/6244).
