---
hide:
  - navigation
---

# Nessie {{ versions.nessie }} Downloads

Older release downloads are available via [GitHub](https://github.com/projectnessie/nessie/releases).

Download options for this Nessie {{ versions.nessie }} release:

* [Nessie Server](#nessie-server)
* [CLI & REPL](#nessie-cli--repl)
* [GC Tool](#nessie-gc-tool)
* [REST OpenAPI](#nessie-rest-api)
* [Maven Central](#nessie-artifacts-on-maven-central)
* [License Reports](#license-reports)

See also the [Nessie release page page for version {{ versions.nessie }} on GitHub](https://github.com/projectnessie/nessie/releases/tag/nessie-{{ versions.nessie }})

## Nessie Server

The main Nessie server serves the Nessie repository using the Iceberg REST API and Nessie's native REST API.

### Docker image

Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.

=== "GitHub Container Registry"
    [GitHub Container Registry](https://ghcr.io/projectnessie/nessie)
    ```bash
    docker pull ghcr.io/projectnessie/nessie:{{ versions.nessie }}
    docker run -p 19120:19120 ghcr.io/projectnessie/nessie:{{ versions.nessie }}
    ```
=== "Quay.io"
    [quay.io](https://quay.io/repository/projectnessie/nessie?tab=tags)
    ```bash
    docker pull quay.io/projectnessie/nessie:{{ versions.nessie }}
    docker run -p 19120:19120 quay.io/projectnessie/nessie:{{ versions.nessie }}
    ```

### Helm Chart

=== "Artifact Hub"
    [Artifact Hub](https://artifacthub.io/packages/search?repo=nessie)
=== "Nessie Helmchart Repo"
    [https://charts.projectnessie.org/](https://charts.projectnessie.org/)
=== "Tarball"
    [Nessie {{ versions.nessie }} Helm Chart](https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-helm-{{ versions.nessie }}.tgz)

### Standalone uber jar

Requires Java 17 or newer.

```bash
curl -L -o nessie-quarkus-{{ versions.nessie }}-runner.jar \
  https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-quarkus-{{ versions.nessie }}-runner.jar
java -jar nessie-quarkus-{{ versions.nessie }}-runner.jar
```

## Nessie CLI & REPL

[Nessie CLI](/nessie-latest/cli/) is both a command-line interface but primarily a REPL.

### Standalone uber jar

Requires Java 11 or newer.

```bash
curl -L -o nessie-cli-{{ versions.nessie }}.jar \
  https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-cli-{{ versions.nessie }}.jar
java -jar nessie-cli-{{ versions.nessie }}.jar
```

## Nessie GC Tool

[Nessie GC](/nessie-latest/gc/) allows mark and sweep data files based on flexible expiration policies.

### Docker image

Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.

=== "GitHub Container Registry"
    [GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-gc)
    ```bash
    docker pull ghcr.io/projectnessie/nessie-gc:{{ versions.nessie }}
    docker run -p 19120:19120 ghcr.io/projectnessie/nessie-gc:{{ versions.nessie }} --help
    ```
=== "Quay.io"
    [quay.io](https://quay.io/repository/projectnessie/nessie-gc?tab=tags)
    ```bash
    docker pull quay.io/projectnessie/nessie-gc:{{ versions.nessie }}
    docker run -p 19120:19120 quay.io/projectnessie/nessie-gc:{{ versions.nessie }} --help
    ```

### Standalone uber jar

Requires Java 11, Java 17 recommended.

```bash
curl -L -o nessie-gc-{{ versions.nessie }}.jar \
  https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-gc-{{ versions.nessie }}.jar
java -jar nessie-gc-{{ versions.nessie }}.jar
```

## Nessie Server Admin Tool

Nessie's [Server Admin Tool](/nessie-latest/export-import/) allows migration (export/import) of a
Nessie repository.

### Docker image

Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.

=== "GitHub Container Registry"
    [GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-server-admin)
    ```bash
    docker pull ghcr.io/projectnessie/nessie-server-admin:{{ versions.nessie }}
    docker run -p 19120:19120 ghcr.io/projectnessie/nessie-server-admin:{{ versions.nessie }} --help
    ```
=== "Quay.io"
    [quay.io](https://quay.io/repository/projectnessie/nessie-server-admin?tab=tags)
    ```bash
    docker pull quay.io/projectnessie/nessie-server-admin:{{ versions.nessie }}
    docker run -p 19120:19120 quay.io/projectnessie/nessie-server-admin:{{ versions.nessie }} --help
    ```

### Standalone uber jar

Requires Java 17 or newer.

```bash
curl -L -o nessie-server-admin-tool-{{ versions.nessie }}-runner.jar \
  https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-server-admin-tool-{{ versions.nessie }}-runner.jar
java -jar nessie-server-admin-tool-{{ versions.nessie }}-runner.jar
```

## Nessie REST API

=== "View on Swagger Hub"
    [![Swagger Hub](https://img.shields.io/badge/swagger%20hub-nessie-3f6ec6?style=for-the-badge&logo=swagger&link=https%3A%2F%2Fapp.swaggerhub.com%2Fapis%2Fprojectnessie%2Fnessie)](https://app.swaggerhub.com/apis/projectnessie/nessie/{{ versions.nessie }})
=== "Download"
    [OpenAPI Download](https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-openapi-{{ versions.nessie }}.yaml)

## Nessie artifacts on Maven Central

[Maven Central](https://search.maven.org/artifact/org.projectnessie.nessie/nessie)

## License Reports

License reports for this release are [available via this link (zip file)](https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-aggregated-license-report-{{ versions.nessie }}.zip).
