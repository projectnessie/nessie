---
title: "Spark SQL Extension"
---

# Nessie Spark SQL Extension Reference

See also the [Nessie Spark SQL Extensions](/guides/sql/) main page.

## Nessie SQL commands reference

The following syntax descriptions illustrate how commands are used and the order of
the clauses.

The commands provided by the Nessie Spark SQL are actually a subset of the commands
that are available in the [Nessie CLI](cli.md). Nessie Spark SQL commands however
have the `IN <catalog-name>` clause, which is not needed in the Nessie CLI.

!!! warn
    Use the version of the Nessie Spark SQL extensions that matches the Nessie version included
    in the Iceberg version you want to use! See [Nessie Spark SQL Extensions page](/guides/sql/)
    for details.

!!! info
    `CODE` style means the term is a keyword.

    **BoldTerms** mean variable input, see <u>[Descripton of Command Parts below](#command-parts)</u>

    Square brackets `[` `]` mean that the contents are optional (0 or 1 occurrence).

    Curly brackets `{` `}` mean that the contents can be repeated 0 or more times.

### **`CREATE BRANCH` / `TAG`**

{% include './generated-docs/spark-sql-syntax-CreateReferenceStatement.md' %}

{% include './generated-docs/cli-help-CreateReferenceStatement.md' %}

### **`DROP BRANCH` / `TAG`**

{% include './generated-docs/spark-sql-syntax-DropReferenceStatement.md' %}

{% include './generated-docs/cli-help-DropReferenceStatement.md' %}

### **`ASSIGN BRANCH` / `TAG`**

{% include './generated-docs/spark-sql-syntax-AssignReferenceStatement.md' %}

{% include './generated-docs/cli-help-AssignReferenceStatement.md' %}

### **`LIST REFERENCES`**

{% include './generated-docs/spark-sql-syntax-ListReferencesStatement.md' %}

{% include './generated-docs/cli-help-ListReferencesStatement.md' %}

### **`MERGE BRANCH`**

{% include './generated-docs/spark-sql-syntax-MergeBranchStatement.md' %}

{% include './generated-docs/cli-help-MergeBranchStatement.md' %}

### **`SHOW LOG`**

{% include './generated-docs/spark-sql-syntax-ShowLogStatement.md' %}

{% include './generated-docs/cli-help-ShowLogStatement.md' %}

### **`SHOW REFERENCE`**

{% include './generated-docs/spark-sql-syntax-ShowReferenceStatement.md' %}

{% include './generated-docs/cli-help-ShowReferenceStatement.md' %}

## Command parts

### **CatalogName**

Spark catalog name.

### **ReferenceType**

{% include './generated-docs/spark-sql-syntax-ReferenceType.md' %}

### **ExistingReference**

{% include './generated-docs/cli-help-ExistingReference.md' %}

### **ReferenceName**

{% include './generated-docs/cli-help-ReferenceName.md' %}

### **TimestampOrCommit**

{% include './generated-docs/cli-help-TimestampOrCommit.md' %}
