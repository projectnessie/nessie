---
title: "Nessie 0.110.0"
---

# Nessie 0.110.0

Older release downloads are available via [GitHub](https://github.com/projectnessie/nessie/releases).

Download options for this Nessie 0.110.0 release:

* [Nessie Server](#nessie-server)
* [CLI & REPL](#nessie-cli--repl)
* [GC Tool](#nessie-gc-tool)
* [Server Admin Tool](#nessie-server-admin-tool)
* [REST OpenAPI](#nessie-rest-api)
* [Maven Central](#nessie-artifacts-on-maven-central)
* [Software bill of materials](#software-bill-of-materials)
* [Licenses and notices](#licenses-and-notices)

GitHub [release page](https://github.com/projectnessie/nessie/releases/tag/nessie-0.110.0) for 0.110.0 with release artifacts.

## Nessie Server

The main Nessie server serves the Nessie repository using the Iceberg REST API and Nessie's native REST API.

=== "Docker Image"

    Docker images are multiplatform images for amd64, arm64, ppc64le and s390x.
    They are available from the following repositories:

    * [GitHub Container Registry](https://ghcr.io/projectnessie/nessie):

    ```bash
    docker pull ghcr.io/projectnessie/nessie:0.110.0
    docker run -p 19120:19120 -p 9000:9000 ghcr.io/projectnessie/nessie:0.110.0
    ```

    * [Quay.io](https://quay.io/repository/projectnessie/nessie?tab=tags):

    ```bash
    docker pull quay.io/projectnessie/nessie:0.110.0
    docker run -p 19120:19120 -p 9000:9000 quay.io/projectnessie/nessie:0.110.0
    ```

=== "Helm Chart"

    Nessie 0.110.0 Helm chart is available from the following locations:

    * [Nessie Helm Repo](https://charts.projectnessie.org/):

    ```bash
    helm repo add nessie https://charts.projectnessie.org/
    helm repo update
    helm install my-nessie nessie/nessie --version "0.110.0"
    ```

    * [Artifact Hub](https://artifacthub.io/packages/helm/nessie/nessie).
    * [Nessie 0.110.0 Helm Chart Tarball](https://github.com/projectnessie/nessie/releases/download/nessie-0.110.0/nessie-helm-0.110.0.tgz).

=== "Standalone Jar"

    Java version: minimum 17, 21 recommended, [supported operating systems](configuration.md#supported-operating-systems)

    ```bash
    curl -L -o nessie-quarkus-0.110.0-runner.jar \
      https://github.com/projectnessie/nessie/releases/download/nessie-0.110.0/nessie-quarkus-0.110.0-runner.jar
    java -jar nessie-quarkus-0.110.0-runner.jar
    ```

## Nessie CLI & REPL

[Nessie CLI](cli.md) is both a command-line interface but primarily a REPL.

=== "Docker Image"

    Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.

    They are available from the following repositories:

    * [GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-cli):

    ```bash
    docker pull ghcr.io/projectnessie/nessie-cli:0.110.0
    docker run -it ghcr.io/projectnessie/nessie-cli:0.110.0 
    ```

    * [Quay.io](https://quay.io/repository/projectnessie/nessie-cli?tab=tags):

    ```bash
    docker pull quay.io/projectnessie/nessie-cli:0.110.0
    docker run -it quay.io/projectnessie/nessie-cli:0.110.0
    ```

=== "Standalone Jar"

    Requires Java 17, Java 21 recommended.

    ```bash
    curl -L -o nessie-cli-0.110.0.jar \
      https://github.com/projectnessie/nessie/releases/download/nessie-0.110.0/nessie-cli-0.110.0.jar
    java -jar nessie-cli-0.110.0.jar
    ```

## Nessie GC Tool

[Nessie GC](gc.md) allows mark and sweep data files based on flexible expiration policies.

=== "Docker Image"

    Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.
    They are available from the following repositories:

    * [GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-gc):

    ```bash
    docker pull ghcr.io/projectnessie/nessie-gc:0.110.0
    docker run ghcr.io/projectnessie/nessie-gc:0.110.0 --help
    ```

    * [Quay.io](https://quay.io/repository/projectnessie/nessie-gc?tab=tags):

    ```bash
    docker pull quay.io/projectnessie/nessie-gc:0.110.0
    docker run quay.io/projectnessie/nessie-gc:0.110.0 --help
    ```

=== "Standalone Jar"

    Requires Java 17, Java 21 recommended, [supported operating systems](configuration.md#supported-operating-systems)

    ```bash
    curl -L -o nessie-gc-0.110.0.jar \
      https://github.com/projectnessie/nessie/releases/download/nessie-0.110.0/nessie-gc-0.110.0.jar
    java -jar nessie-gc-0.110.0.jar
    ```

## Nessie Server Admin Tool

Nessie's [Server Admin Tool](export_import.md) allows migration (export/import) of a
Nessie repository.

=== "Docker Image"

    Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.
    They are available from the following repositories:

    * [GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-server-admin):

    ```bash
    docker pull ghcr.io/projectnessie/nessie-server-admin:0.110.0
    docker run ghcr.io/projectnessie/nessie-server-admin:0.110.0 --help
    ```

    * [Quay.io](https://quay.io/repository/projectnessie/nessie-server-admin?tab=tags):

    ```bash
    docker pull quay.io/projectnessie/nessie-server-admin:0.110.0
    docker run quay.io/projectnessie/nessie-server-admin:0.110.0 --help
    ```

=== "Standalone Jar"

    Java version: minimum 17, 21 recommended, [supported operating systems](configuration.md#supported-operating-systems)

    ```bash
    curl -L -o nessie-server-admin-tool-0.110.0-runner.jar \
      https://github.com/projectnessie/nessie/releases/download/nessie-0.110.0/nessie-server-admin-tool-0.110.0-runner.jar
    java -jar nessie-server-admin-tool-0.110.0-runner.jar
    ```

## Nessie REST API

=== "Download"
    [OpenAPI Download](https://github.com/projectnessie/nessie/releases/download/nessie-0.110.0/nessie-openapi-0.110.0.yaml)

## Nessie artifacts on Maven Central

Artifacts are available in two groups: `org.projectnessie.nessie` and
`org.projectnessie.nessie-integrations`. Most users will only need the `org.projectnessie.nessie`
group, which contains the Nessie server and CLI. The `org.projectnessie.nessie-integrations` group
contains additional tools and integrations:

* Spark extensions
* Nessie GC tool

Useful links:

* [Nessie 0.110.0 BOM (Bill of Materials)](https://search.maven.org/artifact/org.projectnessie.nessie/nessie-bom/0.110.0/pom)
* [Nessie 0.110.0 `org.projectnessie.nessie` artifacts](https://search.maven.org/search?q=g:org.projectnessie.nessie%20v:0.110.0)
* [Nessie 0.110.0 `org.projectnessie.nessie-integrations` artifacts](https://search.maven.org/search?q=g:org.projectnessie.nessie-integrations%20v:0.110.0)

The following examples show how to add the Nessie BOM to your build configuration:

=== "Maven"
    In your Maven `pom.xml` add the Nessie BOM as a dependency:
    ```xml
    <dependencyManagement>
      <dependencies>
        <dependency>
          <groupId>org.projectnessie.nessie</groupId>
          <artifactId>nessie-bom</artifactId>
          <version>0.110.0</version>
          <type>pom</type>
          <scope>import</scope>
        </dependency>
      </dependencies>
    </dependencyManagement>
    ```
    Then you can use all Nessie artifacts like this:
    ```xml
    <dependencies>
      <dependency>
        <groupId>org.projectnessie.nessie</groupId>
        <artifactId>nessie-client</artifactId>
      </dependency>
    </dependencies>
    ```
=== "Gradle (Kotlin)"
    In your Gradle project's `build.gradle.kts` add the Nessie BOM as an enforced platform:
    ```kotlin
    dependencies {
      enforcedPlatform("org.projectnessie.nessie:nessie-bom:0.110.0")
    }
    ```
    A full example using the `nessie-client` artifact:
    ```kotlin
    dependencies {
      enforcedPlatform("org.projectnessie.nessie:nessie-bom:0.110.0")
      implementation("org.projectnessie.nessie:nessie-client")
    }
    ```

## Software bill of materials

Nessie publishes a software bill of materials (SBOM) for the server, admin tool,
CLI, garbage collector, content generator and Spark JARs. These CycloneDX 1.6
JSON files list the components used to build each JAR.

On the GitHub release page, the SBOM filename is the JAR filename followed by
`.cdx.json`. Its signature adds another `.asc`. SBOMs for the content generator
and Spark JARs are also available there; those JARs can be downloaded from Maven
Central.

On Maven Central, SBOMs use the `runner-sbom-cyclonedx` classifier for the server
and admin tool, and `sbom-cyclonedx` for the other JARs. The file extension is
`json`. Maven Central and GitHub provide the same SBOM content.

To check a downloaded JAR, first verify the SBOM's `.asc` signature with a trusted
Nessie release signing key. Then compare the JAR's SHA-256 with the value in the
SBOM's `metadata.component.hashes`.

The SBOM describes known build inputs. It may not list dependencies bundled
inside a third-party JAR or every relationship between components. Hashes of
inputs before repackaging are labelled separately from the final JAR's hash.
These SBOMs cover the JARs; container images and application directories are
separate. License information is also provided in the packaged LICENSE and
NOTICE files.

## Licenses and notices

Nessie distributable JARs include third-party license and notice information in
`META-INF/resources/LICENSE.txt` and `META-INF/resources/NOTICE.txt`.
Dependency license policy and packaged attribution are checked during release builds.
