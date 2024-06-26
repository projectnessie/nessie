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

=== "Docker Image"

    Docker images are multiplatform images for amd64, arm64, ppc64le and s390x.
    They are available from the following repositories:
    
    * [GitHub Container Registry](https://ghcr.io/projectnessie/nessie):

    ```bash
    docker pull ghcr.io/projectnessie/nessie:{{ versions.nessie }}
    docker run -p 19120:19120 -p 9000:9000 ghcr.io/projectnessie/nessie:{{ versions.nessie }}
    ```
    
    * [Quay.io](https://quay.io/repository/projectnessie/nessie?tab=tags):
    
    ```bash
    docker pull quay.io/projectnessie/nessie:{{ versions.nessie }}
    docker run -p 19120:19120 -p 9000:9000 quay.io/projectnessie/nessie:{{ versions.nessie }}
    ```

=== "Helm Chart"

    Nessie {{ versions.nessie }} Helm chart is available from the following locations:

    * [Nessie Helm Repo](https://charts.projectnessie.org/):
    
    ```bash
    helm repo add nessie https://charts.projectnessie.org/
    helm repo update
    helm install my-nessie nessie/nessie --version "{{ versions.nessie }}"
    ```

    * [Artifact Hub](https://artifacthub.io/packages/helm/nessie/nessie).
    * [Nessie {{ versions.nessie }} Helm Chart Tarball](https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-helm-{{ versions.nessie }}.tgz).

=== "Standalone Jar"

    Requires Java 17 or newer.
    
    ```bash
    curl -L -o nessie-quarkus-{{ versions.nessie }}-runner.jar \
      https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-quarkus-{{ versions.nessie }}-runner.jar
    java -jar nessie-quarkus-{{ versions.nessie }}-runner.jar
    ```

=== "Maven Central"

    * [Nessie Server on Maven Central](https://search.maven.org/artifact/org.projectnessie.nessie/nessie-quarkus/{{ versions.nessie }}/runner)

    * Gradle Kotlin snippet:

    ```kotlin
    implementation("org.projectnessie.nessie:nessie-quarkus:{{ versions.nessie }}:runner")
    ```

    * Maven snippet:

    ```xml
    <dependency>
      <groupId>org.projectnessie.nessie</groupId>
      <artifactId>nessie-quarkus</artifactId>
      <version>{{ versions.nessie }}</version>
    </dependency>
    ```

    * Download all dependencies with Maven:

    ```bash 
    mvn dependency:get -Dartifact=org.projectnessie.nessie:nessie-quarkus:{{ versions.nessie }}:jar:runner
    ```

## Nessie CLI & REPL

[Nessie CLI](/nessie-latest/cli/) is both a command-line interface but primarily a REPL.

=== "Standalone Jar"

    Requires Java 11 or newer.

    ```bash
    curl -L -o nessie-cli-{{ versions.nessie }}.jar \
      https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-cli-{{ versions.nessie }}.jar
    java -jar nessie-cli-{{ versions.nessie }}.jar
    ```

=== "Docker Image"

    Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.
    They are available from the following repositories:
     
    * [GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-cli):

    ```bash
    docker pull ghcr.io/projectnessie/nessie-cli:{{ versions.nessie }}
    docker run -it ghcr.io/projectnessie/nessie-cli:{{ versions.nessie }}
    ```

    * [Quay.io](https://quay.io/repository/projectnessie/nessie-cli?tab=tags):

    ```bash
    docker pull quay.io/projectnessie/nessie-cli:{{ versions.nessie }}
    docker run -it quay.io/projectnessie/nessie-cli:{{ versions.nessie }}
    ```

=== "Maven Central"

    * [Nessie CLI on Maven Central](https://search.maven.org/artifact/org.projectnessie.nessie/nessie-cli/{{ versions.nessie }}/jar)

    * Gradle Kotlin snippet:

    ```kotlin
    implementation("org.projectnessie.nessie:nessie-cli:{{ versions.nessie }}")
    ```

    * Maven snippet:

    ```xml
    <dependency>
      <groupId>org.projectnessie.nessie</groupId>
      <artifactId>nessie-cli</artifactId>
      <version>{{ versions.nessie }}</version>
    </dependency>
    ```

    * Download all dependencies with Maven:

    ```bash 
    mvn dependency:get -Dartifact=org.projectnessie.nessie:nessie-cli:{{ versions.nessie }}:jar
    ```

## Nessie GC Tool

[Nessie GC](/nessie-latest/gc/) allows mark and sweep data files based on flexible expiration policies.

=== "Docker Image"

    Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.
    They are available from the following repositories:

    * [GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-gc):

    ```bash
    docker pull ghcr.io/projectnessie/nessie-gc:{{ versions.nessie }}
    docker run ghcr.io/projectnessie/nessie-gc:{{ versions.nessie }} --help
    ```

    * [Quay.io](https://quay.io/repository/projectnessie/nessie-gc?tab=tags):

    ```bash
    docker pull quay.io/projectnessie/nessie-gc:{{ versions.nessie }}
    docker run quay.io/projectnessie/nessie-gc:{{ versions.nessie }} --help
    ```

=== "Standalone Jar"

    Requires Java 11, Java 17 recommended.
    
    ```bash
    curl -L -o nessie-gc-{{ versions.nessie }}.jar \
      https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-gc-{{ versions.nessie }}.jar
    java -jar nessie-gc-{{ versions.nessie }}.jar
    ```

=== "Maven Central"

    * [Nessie GC on Maven Central](https://search.maven.org/artifact/org.projectnessie.nessie-integrations/nessie-gc-tool/{{ versions.nessie }}/jar)

    * Gradle Kotlin snippet:

    ```kotlin
    implementation("org.projectnessie.nessie-integrations:nessie-gc-tool:{{ versions.nessie }}")
    ```

    * Maven snippet:

    ```xml
    <dependency>
      <groupId>org.projectnessie.nessie-integrations</groupId>
      <artifactId>nessie-gc-tool</artifactId>
      <version>{{ versions.nessie }}</version>
    </dependency>
    ```

    * Download all dependencies with Maven:

    ```bash 
    mvn dependency:get -Dartifact=org.projectnessie.nessie-integrations:nessie-gc-tool:{{ versions.nessie }}:jar
    ```

## Nessie Server Admin Tool

Nessie's [Server Admin Tool](/nessie-latest/export-import/) allows migration (export/import) of a
Nessie repository.

=== "Docker Image"

    Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.
    They are available from the following repositories:
    
    * [GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-server-admin)

    ```bash
    docker pull ghcr.io/projectnessie/nessie-server-admin:{{ versions.nessie }}
    docker run ghcr.io/projectnessie/nessie-server-admin:{{ versions.nessie }} --help
    ```

    * [Quay.io](https://quay.io/repository/projectnessie/nessie-server-admin?tab=tags)

    ```bash
    docker pull quay.io/projectnessie/nessie-server-admin:{{ versions.nessie }}
    docker run quay.io/projectnessie/nessie-server-admin:{{ versions.nessie }} --help
    ```

=== "Standalone Jar"

    Requires Java 17 or newer.
    
    ```bash
    curl -L -o nessie-server-admin-tool-{{ versions.nessie }}-runner.jar \
      https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-server-admin-tool-{{ versions.nessie }}-runner.jar
    java -jar nessie-server-admin-tool-{{ versions.nessie }}-runner.jar
    ```

=== "Maven Central"

    * [Nessie Server Admin Tool on Maven Central](https://search.maven.org/artifact/org.projectnessie.nessie/nessie-server-admin-tool/{{ versions.nessie }}/runner)

    * Gradle Kotlin snippet:

    ```kotlin
    implementation("org.projectnessie.nessie:nessie-server-admin-tool:{{ versions.nessie }}:runner")
    ```

    * Maven snippet:

    ```xml
    <dependency>
      <groupId>org.projectnessie.nessie</groupId>
      <artifactId>nessie-server-admin-tool</artifactId>
      <version>{{ versions.nessie }}</version>
    </dependency>
    ```

    * Download all dependencies with Maven:

    ```bash 
    mvn dependency:get -Dartifact=org.projectnessie.nessie:nessie-server-admin-tool:{{ versions.nessie }}:jar
    ```

## Nessie REST API

=== "View on Swagger Hub"
    [![Swagger Hub](https://img.shields.io/badge/swagger%20hub-nessie-3f6ec6?style=for-the-badge&logo=swagger&link=https%3A%2F%2Fapp.swaggerhub.com%2Fapis%2Fprojectnessie%2Fnessie)](https://app.swaggerhub.com/apis/projectnessie/nessie/{{ versions.nessie }})
=== "Download"
    [OpenAPI Download](https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-openapi-{{ versions.nessie }}.yaml)

## Nessie artifacts on Maven Central

Artifacts are available in two groups: `org.projectnessie.nessie` and
`org.projectnessie.nessie-integrations`. Most users will only need the `org.projectnessie.nessie`
group, which contains the Nessie server and CLI. The `org.projectnessie.nessie-integrations` group
contains additional tools and integrations:

* Spark extensions
* Nessie GC tool

Useful links:

* [Nessie {{ versions.nessie }} BOM (Bill of Materials)](https://search.maven.org/artifact/org.projectnessie.nessie/nessie/{{ versions.nessie }}/pom)
* [Nessie {{ versions.nessie }} `org.projectnessie.nessie` artifacts](https://search.maven.org/search?q=g:org.projectnessie.nessie%20v:{{ versions.nessie }})
* [Nessie {{ versions.nessie }} `org.projectnessie.nessie-integrations` artifacts](https://search.maven.org/search?q=g:org.projectnessie.nessie-integrations%20v:{{ versions.nessie }})

## License Reports

License reports for this release are [available via this link (zip file)](https://github.com/projectnessie/nessie/releases/download/nessie-{{ versions.nessie }}/nessie-aggregated-license-report-{{ versions.nessie }}.zip).
