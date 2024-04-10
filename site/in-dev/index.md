# Nessie (UNRELEASED)

**DISCLAIMER** You are viewing the docs for the **next** Nessie version.
Docs for the [latest release {{ versions.nessie }} are here](../nessie-latest/index.md).

This section contains information about an unreleased state, also known as _nightly_ or _snapshot_
builds. The state of the software as well as the documentation 

## SNAPSHOT / Nightly artifacts

### Nessie Server UNSTABLE as a Docker image

Docker images are multiplatform images for amd64, arm64, ppc64le, s390x.

**The image tags are updated twice per day during weekdays.**

=== "GitHub Container Registry"
    ```bash
    docker pull ghcr.io/projectnessie/nessie-unstable
    docker run -p 19120:19120 ghcr.io/projectnessie/nessie-unstable
    ```
=== "Quay.io"
    ```bash
    docker pull quay.io/projectnessie/nessie-unstable
    docker run -p 19120:19120 quay.io/projectnessie/nessie-unstable
    ```

### Build from source

See [projectnessie/nessie GitHub](https://github.com/projectnessie/nessie) for build instructions.

### Nessie SNAPSHOT artifacts

Snapshot artifacts are available from Sonatype. The version of the published _SNAPSHOT_ artifacts
changes with every Nessie release. The currently published _SHAPSHOT_ version can be [inspected
in a browser on GitHub](https://github.com/projectnessie/nessie/blob/main/version.txt) or on the
command line using the following command:
```bash
curl https://github.com/projectnessie/nessie/blob/main/version.txt
```

=== "Maven"
    In your Maven `pom.xml` add the SonaType repository:
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
