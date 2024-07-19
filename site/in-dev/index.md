---
title: "Nessie (UNRELEASED)"
---

# Nessie (UNRELEASED/SNAPSHOT/unstable/nightly) Docs

**DISCLAIMER** You are viewing the docs for the **next** Nessie version.
Docs for the [latest release {{ versions.nessie }} are here](../nessie-latest/index.md).

This section contains information about an unreleased state, also known as _nightly_ or _snapshot_
builds. The state of the software as well as the documentation 

## Nessie Server unstable/nightly

The main Nessie server serves the Nessie repository using the Iceberg REST API and Nessie's native REST API.

=== "Docker Image"

    Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.

    **The image tags are updated twice per day during weekdays.**

    Images are available from the following repositories: 

    * [GitHub Container Registry](https://ghcr.io/projectnessie/nessie-unstable):

    ```bash
    docker pull ghcr.io/projectnessie/nessie-unstable
    docker run -p 19120:19120 -p 9000:9000 ghcr.io/projectnessie/nessie-unstable
    ```

    * [Quay.io](https://quay.io/repository/projectnessie/nessie-unstable?tab=tags):

    ```bash
    docker pull quay.io/projectnessie/nessie-unstable
    docker run -p 19120:19120 -p 9000:9000 quay.io/projectnessie/nessie-unstable
    ```

## Nessie CLI unstable/nightly

=== "Docker Image"

    Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.

    **The image tags are updated twice per day during weekdays.**

    Images are available from the following repositories: 

    * [GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-cli-unstable):

    ```bash
    docker pull ghcr.io/projectnessie/nessie-cli-unstable
    docker run -it ghcr.io/projectnessie/nessie-cli-unstable
    ```

    * [Quay.io](https://quay.io/repository/projectnessie/nessie-cli-unstable?tab=tags):

    ```bash
    docker pull quay.io/projectnessie/nessie-cli-unstable
    docker run -it quay.io/projectnessie/nessie-cli-unstable
    ```

## GC Tool unstable/nightly

[Nessie GC](/nessie-nightly/gc/) allows mark and sweep data files based on flexible
expiration policies.

=== "Docker Image"

    Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.

    **The image tags are updated twice per day during weekdays.**

    Images are available from the following repositories:

    * [GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-gc-unstable):

    ```bash
    docker pull ghcr.io/projectnessie/nessie-gc-unstable
    docker run ghcr.io/projectnessie/nessie-gc-unstable --help
    ```

    * [Quay.io](https://quay.io/repository/projectnessie/nessie-gc-unstable?tab=tags):

    ```bash
    docker pull quay.io/projectnessie/nessie-gc-unstable
    docker run quay.io/projectnessie/nessie-gc-unstable --help
    ```

## Server Admin Tool unstable/nightly

Nessie's [Server Admin Tool](/nessie-nightly/export-import/) allows migration (export/import) of a
Nessie repository.

=== "Docker Image"

    Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.

    **The image tags are updated twice per day during weekdays.**

    They are available from the following repositories:

    * [GitHub Container Registry](https://github.com/projectnessie/nessie/pkgs/container/nessie-server-admin-unstable):

    ```bash
    docker pull ghcr.io/projectnessie/nessie-server-admin-unstable
    docker run ghcr.io/projectnessie/nessie-server-admin-unstable --help
    ```

    * [Quay.io](https://quay.io/repository/projectnessie/nessie-server-admin-unstable?tab=tags):

    ```bash
    docker pull quay.io/projectnessie/nessie-server-admin-unstable
    docker run quay.io/projectnessie/nessie-server-admin-unstable --help
    ```

## Build Nessie from source

See [projectnessie/nessie GitHub](https://github.com/projectnessie/nessie) for build instructions.

## Nessie SNAPSHOT Maven artifacts

Snapshot artifacts are available from Sonatype. The version of the published _SNAPSHOT_ artifacts
changes with every Nessie release. 

!!! note
    Snapshot artifacts are meant for development and testing purposes only, especially when
    developing against Nessie. They are not meant for production use.

The currently published _SNAPSHOT_ version can be [inspected in a browser on
GitHub](https://github.com/projectnessie/nessie/blob/main/version.txt) or on the command line using
the following command:

```bash
curl https://github.com/projectnessie/nessie/blob/main/version.txt
```

Snapshot repositories must be added to your build configuration. The following examples show how to
add the repository to your build configuration:

=== "Maven"
    In your Maven `pom.xml` add the Sonatype repository:
    ```xml
      <repositories>
        <repository>
          <id>oss.sonatype.org-snapshot</id>
          <url>https://oss.sonatype.org/content/repositories/snapshots</url>
          <releases>
            <enabled>false</enabled>
          </releases>
          <snapshots>
            <enabled>true</enabled>
          </snapshots>
        </repository>
      </repositories>
    ```
=== "Gradle (Kotlin)"
    In your Gradle project's `settings.gradle.kts` add the repository:
    ```kotlin
    dependencyResolutionManagement {
      repositories {
        mavenCentral()
        maven {
          name = "Apache Snapshots"
          url = URI("https://oss.sonatype.org/content/repositories/snapshots")
          mavenContent { snapshotsOnly() }
          metadataSources {
            // Workaround for
            // https://youtrack.jetbrains.com/issue/IDEA-327421/IJ-fails-to-import-Gradle-project-with-dependency-with-classifier
            ignoreGradleMetadataRedirection()
            mavenPom()
          }
        }
      }
    }
    ```

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

