# Distribution SBOMs

Nessie publishes CycloneDX 1.6 JSON SBOMs for its distributable JARs. Quarkus
produces the server and admin tool documents; the official CycloneDX Gradle
plugin produces the others. Each module has one `finalizeDistributionSbom` task
that adds the final artifact's identity and SHA-256, selects its build inputs,
and validates the document's schema and references.

Quarkus defaults to a fast-JAR application directory. Use `-Puber-jar` to build
the runner JARs published to Maven Central. A fast-JAR SBOM identifies the whole
application directory through a separate tree descriptor, not just its launcher.
The descriptor includes file paths, contents and POSIX permissions. Keep it
outside the directory it describes. Symlinks and special files are rejected;
tree finalization requires a POSIX filesystem.

Shadow SBOMs use the dependencies actually selected by Shadow, including
first-party JARs and their classifiers and types. They do not inspect fat JARs
to discover dependencies hidden inside them. Input hashes are labelled separately
from the final artifact's hash. Dependency relationships are retained only when
supported by the producer, and completeness is marked `unknown`.

## Portable core

Copy `SbomCore.kt`, `FinalizeDistributionSbom.kt`, `SbomSubject.kt` and
`PackageKind.kt` from
`gradle/license-check-plugin/src/main/kotlin/org/projectnessie/gradle/sbom/`,
along with the matching test package. The core uses Gradle's Kotlin DSL and
CycloneDX core Java. Tests use Gradle TestKit, JUnit Jupiter and AssertJ.
Dependency versions are in `gradle/libs.versions.toml`.

`SbomSubject` holds the group, name, version and packaging kind. These are tracked
task inputs; the package URL is derived from them. Supply the producer document,
selected files and their metadata, final artifact or directory, and output paths
through the task's other declared inputs and outputs. Shadow metadata uses
absolute file paths, so build-cache entries cannot be reused across different
checkout or dependency-cache locations.

`SbomCore.forDistribution` updates a CycloneDX `Bom` in place. It retains metadata
supported by that model and projects `dependsOn` relationships onto the selected
inputs. `provides` and unknown producer fields are not preserved. Tree descriptors
use Jackson with deterministic key and path ordering.

Nessie's project and publication wiring lives in `DistributionSbom.kt`. The
`SbomShadow` helper reads providers passed from build-logic through the
`sbomShadow` extra property. Only Gradle types and plain maps cross this boundary;
Shadow classes stay in build-logic. The private `licensee.gradle.kts` script loads
the included plugin and calls `configureDistributionSbom()`, keeping CycloneDX's
and Licensee's dependencies off the shared build-logic classpath.

Other projects supply their own producer and publication wiring. Different
Quarkus versions and packaging layouts need their own compatibility checks.
JAR, application-directory, archive and container-image hashes identify different
subjects; supporting a new packaging format requires an appropriate adapter.

## Publication

The finalized SBOM is attached to the existing Maven publication. Existing
signing and NMCP tasks publish it with the other artifacts. The release workflow
copies the same SBOM and its signature from each module's `reports/sbom`
directory to GitHub release assets. It retains the existing JAR names, including
the shorter GC download name.

Each SBOM contains its artifact's SHA-256 in `metadata.component.hashes`.
Consumers verify the SBOM signature with a trusted release key, then compare that
hash with the downloaded JAR. These documents cover JARs; container-image SBOMs
are separate.

Licensee remains the license-policy authority. Existing license and attribution
checks gate publication. Ordinary `check` and `codeChecks` do not run SBOM
finalization. Publication uses the existing workflows and inputs. If an upload
fails ambiguously, maintainers can make another release.
