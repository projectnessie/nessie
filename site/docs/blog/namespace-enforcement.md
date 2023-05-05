# Namespace enforcement in Nessie

Starting from Nessie version 0.52.3, it is required to have existing Namespaces before creating or committing to tables.
<br>In case of tables that have implicit namespaces, new commits will fail after the upgrade as this rule will be enforced.
<br>Therefore, it is necessary to explicitly create the namespaces using the using SQL command (for Spark, `CREATE NAMESPACE catalogName.namespaceName`) or Iceberg Catalog API.
<br>[Issue background](https://github.com/projectnessie/nessie/issues/6244)
<br>[more info from the spec](https://github.com/projectnessie/nessie/blob/main/api/NESSIE-SPEC-2-0.md#200-beta1)
