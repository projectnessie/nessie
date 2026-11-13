---
title: "Nessie ::NESSIE_VERSION::"
---

# Nessie ::NESSIE_VERSION::

Older release downloads are available via [GitHub](https://github.com/projectnessie/nessie/releases).

Download options for this Nessie ::NESSIE_VERSION:: release:

* [Nessie Server](#nessie-server)
* [CLI & REPL](#nessie-cli--repl)
* [GC Tool](#nessie-gc-tool)
* [Server Admin Tool](#nessie-server-admin-tool)
* [REST OpenAPI](#nessie-rest-api)
* [Maven Central](#nessie-artifacts-on-maven-central)
* [Software bill of materials](#software-bill-of-materials)
* [Licenses and notices](#licenses-and-notices)

GitHub [release page](https://github.com/projectnessie/nessie/releases/tag/nessie-::NESSIE_VERSION::) for ::NESSIE_VERSION:: with release artifacts.

## Nessie Server

The main Nessie server serves the Nessie repository using the Iceberg REST API and Nessie's native REST API.

=== "Docker Image"

    Docker images are multiplatform images for amd64, arm64, ppc64le and s390x.
    They are available from the following repositories:

    * [GitHub Container Registry](https://ghcr.io/projectnessie/nessie):

    ```bash
    docker pull ghcr.io/projectnessie/nessie:::NESSIE_VERSION::
    docker run -p 19120:19120 -p 9000:9000 ghcr.io/projectnessie/nessie:::NESSIE_VERSION::
    ```

    * [Quay.io](https://quay.io/repository/projectnessie/nessie?tab=tags):

    ```bash
    docker pull quay.io/projectnessie/nessie:::NESSIE_VERSION::
    docker run -p 19120:19120 -p 9000:9000 quay.io/projectnessie/nessie:::NESSIE_VERSION::
    ```

=== "Helm Chart"

    Nessie ::NESSIE_VERSION:: Helm chart is available from the following locations:

    * [Nessie Helm Repo](https://charts.projectnessie.org/):

    ```bash
    helm repo add nessie https://charts.projectnessie.org/
    helm repo update
    helm install my-nessie nessie/nessie --version "::NESSIE_VERSION::"
    ```

    * [Artifact Hub](https://artifacthub.io/packages/helm/nessie/nessie).
    * [Nessie ::NESSIE_VERSION:: Helm Chart Tarball](https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-helm-::NESSIE_VERSION::.tgz).

=== "Standalone Jar"

    Java version: minimum 17, 21 recommended, [supported operating systems](configuration.md#supported-operating-systems)

    ```bash
    curl -L -o nessie-quarkus-::NESSIE_VERSION::-runner.jar \
      https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-quarkus-::NESSIE_VERSION::-runner.jar
    java -jar nessie-quarkus-::NESSIE_VERSION::-runner.jar
    ```

## Nessie CLI & REPL

[Nessie CLI](cli.md) is both a command-line interface but primarily a REPL.

=== "Docker Image"

    Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.

    They are available from the following repositories:

    * [GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-cli):

    ```bash
    docker pull ghcr.io/projectnessie/nessie-cli:::NESSIE_VERSION::
    docker run -it ghcr.io/projectnessie/nessie-cli:::NESSIE_VERSION:: 
    ```

    * [Quay.io](https://quay.io/repository/projectnessie/nessie-cli?tab=tags):

    ```bash
    docker pull quay.io/projectnessie/nessie-cli:::NESSIE_VERSION::
    docker run -it quay.io/projectnessie/nessie-cli:::NESSIE_VERSION::
    ```

=== "Standalone Jar"

    Requires Java 17, Java 21 recommended.

    ```bash
    curl -L -o nessie-cli-::NESSIE_VERSION::.jar \
      https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-cli-::NESSIE_VERSION::.jar
    java -jar nessie-cli-::NESSIE_VERSION::.jar
    ```

## Nessie GC Tool

[Nessie GC](gc.md) allows mark and sweep data files based on flexible expiration policies.

=== "Docker Image"

    Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.
    They are available from the following repositories:

    * [GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-gc):

    ```bash
    docker pull ghcr.io/projectnessie/nessie-gc:::NESSIE_VERSION::
    docker run ghcr.io/projectnessie/nessie-gc:::NESSIE_VERSION:: --help
    ```

    * [Quay.io](https://quay.io/repository/projectnessie/nessie-gc?tab=tags):

    ```bash
    docker pull quay.io/projectnessie/nessie-gc:::NESSIE_VERSION::
    docker run quay.io/projectnessie/nessie-gc:::NESSIE_VERSION:: --help
    ```

=== "Standalone Jar"

    Requires Java 17, Java 21 recommended, [supported operating systems](configuration.md#supported-operating-systems)

    ```bash
    curl -L -o nessie-gc-::NESSIE_VERSION::.jar \
      https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-gc-::NESSIE_VERSION::.jar
    java -jar nessie-gc-::NESSIE_VERSION::.jar
    ```

## Nessie Server Admin Tool

Nessie's [Server Admin Tool](export_import.md) allows migration (export/import) of a
Nessie repository.

=== "Docker Image"

    Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.
    They are available from the following repositories:

    * [GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-server-admin):

    ```bash
    docker pull ghcr.io/projectnessie/nessie-server-admin:::NESSIE_VERSION::
    docker run ghcr.io/projectnessie/nessie-server-admin:::NESSIE_VERSION:: --help
    ```

    * [Quay.io](https://quay.io/repository/projectnessie/nessie-server-admin?tab=tags):

    ```bash
    docker pull quay.io/projectnessie/nessie-server-admin:::NESSIE_VERSION::
    docker run quay.io/projectnessie/nessie-server-admin:::NESSIE_VERSION:: --help
    ```

=== "Standalone Jar"

    Java version: minimum 17, 21 recommended, [supported operating systems](configuration.md#supported-operating-systems)

    ```bash
    curl -L -o nessie-server-admin-tool-::NESSIE_VERSION::-runner.jar \
      https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-server-admin-tool-::NESSIE_VERSION::-runner.jar
    java -jar nessie-server-admin-tool-::NESSIE_VERSION::-runner.jar
    ```

## Nessie REST API

=== "Download"
    [OpenAPI Download](https://github.com/projectnessie/nessie/releases/download/nessie-::NESSIE_VERSION::/nessie-openapi-::NESSIE_VERSION::.yaml)

## Nessie artifacts on Maven Central

Artifacts are available in two groups: `org.projectnessie.nessie` and
`org.projectnessie.nessie-integrations`. Most users will only need the `org.projectnessie.nessie`
group, which contains the Nessie server and CLI. The `org.projectnessie.nessie-integrations` group
contains additional tools and integrations:

* Spark extensions
* Nessie GC tool

Useful links:

* [Nessie ::NESSIE_VERSION:: BOM (Bill of Materials)](https://search.maven.org/artifact/org.projectnessie.nessie/nessie-bom/::NESSIE_VERSION::/pom)
* [Nessie ::NESSIE_VERSION:: `org.projectnessie.nessie` artifacts](https://search.maven.org/search?q=g:org.projectnessie.nessie%20v:::NESSIE_VERSION::)
* [Nessie ::NESSIE_VERSION:: `org.projectnessie.nessie-integrations` artifacts](https://search.maven.org/search?q=g:org.projectnessie.nessie-integrations%20v:::NESSIE_VERSION::)

The following examples show how to add the Nessie BOM to your build configuration:

=== "Maven"
    In your Maven `pom.xml` add the Nessie BOM as a dependency:
    ```xml
    <dependencyManagement>
      <dependencies>
        <dependency>
          <groupId>org.projectnessie.nessie</groupId>
          <artifactId>nessie-bom</artifactId>
          <version>::NESSIE_VERSION::</version>
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
      enforcedPlatform("org.projectnessie.nessie:nessie-bom:::NESSIE_VERSION::")
    }
    ```
    A full example using the `nessie-client` artifact:
    ```kotlin
    dependencies {
      enforcedPlatform("org.projectnessie.nessie:nessie-bom:::NESSIE_VERSION::")
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
