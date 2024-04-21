---
title: "Nessie ::NESSIE_VERSION::"
---

# Nessie ::NESSIE_VERSION::

GitHub [release page](https://github.com/projectnessie/nessie/releases/tag/nessie-::NESSIE_VERSION::) for ::NESSIE_VERSION::.

For all download options, refer to the [Downloads page](../downloads/index.md).


## Download options for this Nessie ::NESSIE_VERSION:: release

### Nessie Server as Docker image

Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.

=== "GitHub Container Registry"
    [![ghcr.io GitHub Container Registry](https://img.shields.io/maven-central/v/org.projectnessie.nessie/nessie?label=quay.io+Docker&logo=docker&color=3f6ec6&style=for-the-badge&logoColor=white)](https://ghcr.io/projectnessie/nessie)
    ```bash
    docker pull ghcr.io/projectnessie/nessie:::NESSIE_VERSION::
    docker run -p 19120:19120 ghcr.io/projectnessie/nessie:::NESSIE_VERSION::
    ```
=== "Quay.io"
    [![quay.io Quay](https://img.shields.io/maven-central/v/org.projectnessie.nessie/nessie?label=quay.io+Docker&logo=docker&color=3f6ec6&style=for-the-badge&logoColor=white)](https://quay.io/repository/projectnessie/nessie?tab=tags)
    ```bash
    docker pull quay.io/projectnessie/nessie:::NESSIE_VERSION::
    docker run -p 19120:19120 quay.io/projectnessie/nessie:::NESSIE_VERSION::
    ```

### Nessie Server Helm Chart

=== "Artifact Hub"
    [![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/nessie&color=3f6ec6&labelColor=&style=for-the-badge&logoColor=white)](https://artifacthub.io/packages/search?repo=nessie)
=== "Nessie Helmchart Repo"
    [https://charts.projectnessie.org/](https://charts.projectnessie.org/)
=== "Tarball"
    [Nessie ::NESSIE_VERSION:: Helm Chart](https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-helm-::NESSIE_VERSION::.tgz)

### Nessie Server as a standalone uber jar

Requires Java 17 or newer.

```bash
curl -o nessie-quarkus-::NESSIE_VERSION::-runner.jar \
  https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-quarkus-::NESSIE_VERSION::-runner.jar
java -jar nessie-quarkus-::NESSIE_VERSION::-runner.jar
```

## Nessie GC Tool as Docker image

Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.

=== "GitHub Container Registry"
    [![ghcr.io GitHub Container Registry](https://img.shields.io/maven-central/v/org.projectnessie.nessie/nessie?label=ghcr.io+Docker&logo=docker&color=3f6ec6&style=for-the-badge&logoColor=white)](https://github.com/projectnessie/nessie/pkgs/container/nessie-gc)
    ```bash
    docker pull ghcr.io/projectnessie/nessie-gc:::NESSIE_VERSION::
    docker run -p 19120:19120 ghcr.io/projectnessie/nessie-gc:::NESSIE_VERSION:: --help
    ```
=== "Quay.io"
    [![quay.io Quay](https://img.shields.io/maven-central/v/org.projectnessie.nessie/nessie?label=quay.io+Docker&logo=docker&color=3f6ec6&style=for-the-badge&logoColor=white)](https://quay.io/repository/projectnessie/nessie-gc?tab=tags)
    ```bash
    docker pull quay.io/projectnessie/nessie-gc:::NESSIE_VERSION::
    docker run -p 19120:19120 quay.io/projectnessie/nessie-gc:::NESSIE_VERSION:: --help
    ```

### Nessie GC Tool as a standalone uber jar

Requires Java 11, Java 17 recommended.

```bash
curl -o https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-gc-::NESSIE_VERSION:: \
  https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-gc-::NESSIE_VERSION::
java -jar https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-gc-::NESSIE_VERSION::
```

### Nessie Repository Management tool as a standalone uber jar

Requires Java 17 or newer.

```bash
curl -o nessie-quarkus-cli-::NESSIE_VERSION::-runner.jar \
  https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-quarkus-cli-::NESSIE_VERSION::-runner.jar
java -jar nessie-quarkus-cli-::NESSIE_VERSION::-runner.jar
```

### Nessie REST API

=== "View on Swagger Hub"
    [![Swagger Hub](https://img.shields.io/badge/swagger%20hub-nessie-3f6ec6?style=for-the-badge&logo=swagger&link=https%3A%2F%2Fapp.swaggerhub.com%2Fapis%2Fprojectnessie%2Fnessie)](https://app.swaggerhub.com/apis/projectnessie/nessie/::NESSIE_VERSION::)
=== "Download"
    [OpenAPI Download](https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-openapi-::NESSIE_VERSION::.yaml)

### Nessie artifacts on Maven Central

[![Maven Central](https://img.shields.io/maven-central/v/org.projectnessie.nessie/nessie?label=Maven%20Central&logo=apachemaven&color=3f6ec6&style=for-the-badge&logoColor=white)](https://search.maven.org/artifact/org.projectnessie.nessie/nessie)

