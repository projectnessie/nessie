---
title: "Nessie CLI"
---

# Nessie CLI

The Nessie CLI is an easy way to get started with Nessie. It supports multiple branch 
and tag management capabilities.

Nessie CLI is designed to be usable as an interactive REPL supporting auto-completion,
highlighting where appropriate and has built-in help. Long outputs, like a commit log,
are automatically paged like the Unix `less` command.



![Nessie CLI](../img/cli-intro.png)

## Installation

Nessie CLI is available as a standalone uber jar, or as a Docker image. See download options in
the [Nessie download page](../downloads/index.md) or in the [releases page on
GitHub](https://github.com/projectnessie/nessie/releases/).

## Usage

Use `CONNECT TO http://127.0.0.1:19120/iceberg` to connect to a locally running Nessie instance with
Iceberg REST. Use `CONNECT TO http://127.0.0.1:19120/api/v2` for Nessie's native REST API.

Use `CONNECT TO https://app.dremio.cloud/repositories/<project-id>/api/v2` to connect to your Dremio
cloud instance using Nessie's native REST API.

See [`CONNECT` statement](#connect) below.

### Command line options

{% include './generated-docs/cli-help.md' %}

## REPL Commands

The following syntax descriptions illustrate how commands are used and the order of
the clauses.

!!! info
    `CODE` style means the term is a keyword.

    **BoldTerms** mean variable input, see <u>[Descripton of Command Parts below](#command-parts)</u>

    Square brackets `[` `]` mean that the contents are optional (0 or 1 occurrence).

    Curly brackets `{` `}` mean that the contents can be repeated 0 or more times.

### **`CONNECT`**

{% include './generated-docs/cli-syntax-ConnectStatement.md' %}

{% include './generated-docs/cli-help-ConnectStatement.md' %}

### **`CREATE BRANCH` / `TAG`**

{% include './generated-docs/cli-syntax-CreateReferenceStatement.md' %}

{% include './generated-docs/cli-help-CreateReferenceStatement.md' %}

### **`CREATE NAMESPACE`**

{% include './generated-docs/cli-syntax-CreateNamespaceStatement.md' %}

{% include './generated-docs/cli-help-CreateNamespaceStatement.md' %}

### **`DROP BRANCH` / `TAG`**

{% include './generated-docs/cli-syntax-DropReferenceStatement.md' %}

{% include './generated-docs/cli-help-DropReferenceStatement.md' %}

### **`DROP TABLE` / `VIEW` / `NAMESPACE`**

{% include './generated-docs/cli-syntax-DropContentStatement.md' %}

{% include './generated-docs/cli-help-DropContentStatement.md' %}

### **`ASSIGN BRANCH` / `TAG`**

{% include './generated-docs/cli-syntax-AssignReferenceStatement.md' %}

{% include './generated-docs/cli-help-AssignReferenceStatement.md' %}

### **`ALTER NAMESPACE`**

{% include './generated-docs/cli-syntax-AlterNamespaceStatement.md' %}

{% include './generated-docs/cli-help-AlterNamespaceStatement.md' %}

### **`LIST CONTENTS`**

{% include './generated-docs/cli-syntax-ListContentsStatement.md' %}

{% include './generated-docs/cli-help-ListContentsStatement.md' %}

### **`LIST REFERENCES`**

{% include './generated-docs/cli-syntax-ListReferencesStatement.md' %}

{% include './generated-docs/cli-help-ListReferencesStatement.md' %}

### **`MERGE BRANCH`**

{% include './generated-docs/cli-syntax-MergeBranchStatement.md' %}

{% include './generated-docs/cli-help-MergeBranchStatement.md' %}

### **`REVERT CONTENT`**

{% include './generated-docs/cli-syntax-RevertContentStatement.md' %}

{% include './generated-docs/cli-help-RevertContentStatement.md' %}

### **`SHOW LOG`**

{% include './generated-docs/cli-syntax-ShowLogStatement.md' %}

{% include './generated-docs/cli-help-ShowLogStatement.md' %}

### **`SHOW TABLE` / `VIEW` / `NAMESPACE`**

{% include './generated-docs/cli-syntax-ShowContentStatement.md' %}

{% include './generated-docs/cli-help-ShowContentStatement.md' %}

### **`SHOW REFERENCE`**

{% include './generated-docs/cli-syntax-ShowReferenceStatement.md' %}

{% include './generated-docs/cli-help-ShowReferenceStatement.md' %}

### **`USE`**

{% include './generated-docs/cli-syntax-UseReferenceStatement.md' %}

{% include './generated-docs/cli-help-UseReferenceStatement.md' %}

### **`HELP`**

{% include './generated-docs/cli-syntax-HelpStatement.md' %}

{% include './generated-docs/cli-help-HelpStatement.md' %}

### **`EXIT`**

{% include './generated-docs/cli-syntax-ExitStatement.md' %}

{% include './generated-docs/cli-help-ExitStatement.md' %}

## Command parts

### **ReferenceType**

{% include './generated-docs/cli-syntax-ReferenceType.md' %}

### **ContentKind**

{% include './generated-docs/cli-syntax-ContentKind.md' %}

### **ExistingReference**

{% include './generated-docs/cli-help-ExistingReference.md' %}

### **ReferenceName**

{% include './generated-docs/cli-help-ReferenceName.md' %}

### **TimestampOrCommit**

{% include './generated-docs/cli-help-TimestampOrCommit.md' %}
