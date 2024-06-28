---
title: "Nessie ::NESSIE_VERSION::"
---

# Nessie ::NESSIE_VERSION::

Older release downloads are available via [GitHub](https://github.com/projectnessie/nessie/releases).

Download options for this Nessie ::NESSIE_VERSION:: release:

* [Nessie Server](#nessie-server)
* [CLI & REPL](#nessie-cli--repl)
* [GC Tool](#nessie-gc-tool)
* [REST OpenAPI](#nessie-rest-api)
* [Maven Central](#nessie-artifacts-on-maven-central)
* [License Reports](#license-reports)

GitHub [release page](https://github.com/projectnessie/nessie/releases/tag/nessie-::NESSIE_VERSION::) for ::NESSIE_VERSION:: with release artifacts.

## Nessie Server

The main Nessie server serves the Nessie repository using the Iceberg REST API and Nessie's native REST API.

### Docker image

Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.

=== "GitHub Container Registry"
    [GitHub Container Registry](https://ghcr.io/projectnessie/nessie)
    ```bash
    docker pull ghcr.io/projectnessie/nessie:::NESSIE_VERSION::
    docker run -p 19120:19120 -p 9000:9000 ghcr.io/projectnessie/nessie:::NESSIE_VERSION::
    ```
=== "Quay.io"
    [quay.io](https://quay.io/repository/projectnessie/nessie?tab=tags)
    ```bash
    docker pull quay.io/projectnessie/nessie:::NESSIE_VERSION::
    docker run -p 19120:19120 -p 9000:9000 quay.io/projectnessie/nessie:::NESSIE_VERSION::
    ```

### Helm Chart

=== "Artifact Hub"
    [Artifact Hub](https://artifacthub.io/packages/search?repo=nessie)
=== "Nessie Helmchart Repo"
    [https://charts.projectnessie.org/](https://charts.projectnessie.org/)
=== "Tarball"
    [Nessie ::NESSIE_VERSION:: Helm Chart](https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-helm-::NESSIE_VERSION::.tgz)

### Standalone uber jar

Requires Java 17 or newer.

```bash
curl -L -o nessie-quarkus-::NESSIE_VERSION::-runner.jar \
  https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-quarkus-::NESSIE_VERSION::-runner.jar
java -jar nessie-quarkus-::NESSIE_VERSION::-runner.jar
```

## Nessie CLI & REPL

[Nessie CLI](cli.md) is both a command-line interface but primarily a REPL.

### Docker image

Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.

=== "GitHub Container Registry"
    [GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-cli)
    ```bash
    docker pull ghcr.io/projectnessie/nessie-cli:::NESSIE_VERSION::
    docker run -it ghcr.io/projectnessie/nessie-cli:::NESSIE_VERSION:: 
    ```
=== "Quay.io"
    [quay.io](https://quay.io/repository/projectnessie/nessie-cli?tab=tags)
    ```bash
    docker pull quay.io/projectnessie/nessie-cli:::NESSIE_VERSION::
    docker run -it quay.io/projectnessie/nessie-cli:::NESSIE_VERSION::
    ```

### Standalone uber jar

Requires Java 11 or newer.

```bash
curl -L -o nessie-cli-::NESSIE_VERSION::.jar \
  https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-cli-::NESSIE_VERSION::.jar
java -jar nessie-cli-::NESSIE_VERSION::.jar
```

## Nessie GC Tool

[Nessie GC](gc.md) allows mark and sweep data files based on flexible expiration policies.

### Docker image

Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.

=== "GitHub Container Registry"
    [ghcr.io GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-gc)
    ```bash
    docker pull ghcr.io/projectnessie/nessie-gc:::NESSIE_VERSION::
    docker run ghcr.io/projectnessie/nessie-gc:::NESSIE_VERSION:: --help
    ```
=== "Quay.io"
    [quay.io Quay](https://quay.io/repository/projectnessie/nessie-gc?tab=tags)
    ```bash
    docker pull quay.io/projectnessie/nessie-gc:::NESSIE_VERSION::
    docker run quay.io/projectnessie/nessie-gc:::NESSIE_VERSION:: --help
    ```

### Standalone uber jar

Requires Java 11, Java 17 recommended.

```bash
curl -L -o nessie-gc-::NESSIE_VERSION::.jar \
  https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-gc-::NESSIE_VERSION::.jar
java -jar nessie-gc-::NESSIE_VERSION::.jar
```

## Nessie Server Admin Tool

Nessie's [Server Admin Tool](export_import.md) allows migration (export/import) of a
Nessie repository.

### Docker image

Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.

=== "GitHub Container Registry"
    [ghcr.io GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-server-admin)
    ```bash
    docker pull ghcr.io/projectnessie/nessie-server-admin:::NESSIE_VERSION::
    docker run ghcr.io/projectnessie/nessie-server-admin:::NESSIE_VERSION:: --help
    ```
=== "Quay.io"
    [quay.io Quay](https://quay.io/repository/projectnessie/nessie-server-admin?tab=tags)
    ```bash
    docker pull quay.io/projectnessie/nessie-server-admin:::NESSIE_VERSION::
    docker run quay.io/projectnessie/nessie-server-admin:::NESSIE_VERSION:: --help
    ```

### Standalone uber jar

Requires Java 17 or newer.

```bash
curl -L -o nessie-server-admin-tool-::NESSIE_VERSION::-runner.jar \
  https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-server-admin-tool-::NESSIE_VERSION::-runner.jar
java -jar nessie-server-admin-tool-::NESSIE_VERSION::-runner.jar
```

## Nessie REST API

=== "View on Swagger Hub"
    [![Swagger Hub](https://img.shields.io/badge/swagger%20hub-nessie-3f6ec6?style=for-the-badge&logo=swagger&link=https%3A%2F%2Fapp.swaggerhub.com%2Fapis%2Fprojectnessie%2Fnessie)](https://app.swaggerhub.com/apis/projectnessie/nessie/::NESSIE_VERSION::)
=== "Download"
    [OpenAPI Download](https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-openapi-::NESSIE_VERSION::.yaml)

## Nessie artifacts on Maven Central

[Maven Central](https://search.maven.org/artifact/org.projectnessie.nessie/nessie)

## License Reports

License reports for this release are [available via this link (zip file)](https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-aggregated-license-report-::NESSIE_VERSION::.zip).
